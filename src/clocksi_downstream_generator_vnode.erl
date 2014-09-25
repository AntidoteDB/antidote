%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(clocksi_downstream_generator_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(dstate, {partition,
                 last_commit_time,
                 pending_operations,
                 stable_time}).

-export([start_vnode/1,
         trigger/2]).

-export([init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

%% @doc Notify downstream_generator_vnode that an update has been logged.
%%      Downstream_generator_vnode then reads the updates from log, generate
%%      downstream op and write to persistent log
%%      input: Key to identify the partition,
%%             Writeset -> set of updates
-spec trigger(key(),
              {TxId :: term(),
               Updates :: [{term(),
                            {Key :: term(), Type:: term(), Op :: term()}}],
               Vec_snapshot_time :: vectorclock:vectorclock(),
               Commit_time :: non_neg_integer()})
             -> ok | {error, timeout}.
trigger(Key, Writeset) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:command([IndexNode],
                                   {trigger, Writeset, self()},
                                   ?CLOCKSI_GENERATOR_MASTER).

start_vnode(I) ->
    {ok, Pid} = riak_core_vnode_master:get_vnode_pid(I, ?MODULE),
    riak_core_vnode:send_command(Pid, {trigger,Pid}),
    {ok, Pid}.

init([Partition]) ->
    {ok, #dstate{partition = Partition,
                 last_commit_time = 0,
                 pending_operations = [],
                stable_time = 0}}.

%%
handle_command({trigger,Pid}, _Sender,
               State= #dstate{partition = Partition,
                              last_commit_time = LastCommitTime
                              }) ->
    MyNode = node(),
    case MyNode of
        'dev2@127.0.0.1' ->
            {reply, ok, State};
        _ ->
            Node = {Partition, node()},
            Stable_time = get_stable_time(Node, LastCommitTime),
            riak_core_vnode_master:command([Node],
                                           {process},
                                           ?CLOCKSI_GENERATOR_MASTER),
            spawn(fun() ->
                          %% Trigger updating vectroclock periodically
                          timer:sleep(5000),
                          riak_core_vnode:send_command(Pid, {trigger,Pid})
                  end
                 ),
            {reply, {ok, trigger_received},
             State#dstate{stable_time=Stable_time}}
        end;

%% @doc Read client update operations,
%%      generate downstream operations and store it to persistent log.
handle_command({trigger, WriteSet, _From}, _Sender,
               State=#dstate{partition = Partition,
                             last_commit_time = LastCommitTime,
                             pending_operations = Pending}) ->
    PendingOperations = add_to_pending_operations(Pending, WriteSet),

    Node = {Partition, node()}, %% Send ack to caller and continue processing
    Stable_time = get_stable_time(Node, LastCommitTime),
    %% Send a message to itself to process operations
    riak_core_vnode_master:command([Node],
                                   {process},
                                   ?CLOCKSI_GENERATOR_MASTER),
    {reply, {ok, trigger_received}, State#dstate{
                                      pending_operations=PendingOperations,
                                      stable_time=Stable_time}};

handle_command({process}, _Sender,
               State=#dstate{partition = Partition,
                             last_commit_time = LastCommitTime,
                             pending_operations = PendingOperations,
                             stable_time = Stable_time}) ->
    Sorted_ops = filter_operations(PendingOperations,
                                   Stable_time, LastCommitTime),
    {Remaining_operations, Last_processed_time} =
        lists:foldl( fun(X, {Ops, LCTS}) ->
                             case process_update(X) of
                                 {ok, CommitTime} ->
                                     %% Remove this op from pending_operations
                                     New_pending = lists:delete(X, Ops),
                                     {New_pending, CommitTime};
                                 {error, _Reason} ->
                                     {Ops, LCTS}
                             end
                     end, {PendingOperations, LastCommitTime}, Sorted_ops),
    DcId = dc_utilities:get_my_dc_id(),
    {ok, Clock} = vectorclock:update_clock(Partition, DcId, Stable_time),
    _ = case PendingOperations of
            [] ->
                %%TODO
                MyNode = node(),
                case MyNode of
                    'dev2@127.0.0.1' ->
                        {reply, ok, State};
                    _ ->
                        inter_dc_repl_vnode:sync_clock(Partition, Clock)
                end;
            [H|_T] -> Key = H#clocksi_payload.key,
                      inter_dc_repl_vnode:trigger(Key)
        end,
    {reply, ok, State#dstate{last_commit_time = Last_processed_time,
                             pending_operations = Remaining_operations}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command( _Message , _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% @doc Generate downstream for one update and write to log
process_update(Update) ->
    case clocksi_downstream:generate_downstream_op(Update) of
        {ok, New_op} ->
            Key = New_op#clocksi_payload.key,
            case materializer_vnode:update(Key, New_op) of
                ok ->
                    {_Dcid, CommitTime} = New_op#clocksi_payload.commit_time,
                    {ok, CommitTime};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Return smallest snapshot time of active transactions.
%%      No new updates with smaller timestamp will occur in future.
get_stable_time(Node, Prev_stable_time) ->
    case riak_core_vnode_master:sync_command(
           Node, {get_active_txns}, ?CLOCKSI_MASTER) of
        {ok, Active_txns} ->
           lists:foldl(fun({_,{_TxId, Snapshot_time}}, Min_time) ->
                                case Min_time > Snapshot_time of
                                    true ->
                                        Snapshot_time;
                                    false ->
                                        Min_time
                                end
                        end,
                        now_milisec(erlang:now()),
                        Active_txns);
        _ -> Prev_stable_time
    end.

now_milisec({MegaSecs,Secs,MicroSecs}) ->
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

%% @doc Select updates committed before Time and sort them in timestamp order.
filter_operations(Ops, Before, After) ->
    %% Remove operations which are already processed
    Unprocessed_ops =
        lists:filtermap(
          fun(Payload) ->
                  {_Dcid, CommitTime} = Payload#clocksi_payload.commit_time,
                  CommitTime > After
          end,
          Ops),

    %% remove operations which are not safer to process now,
    %% because there could be other operations with lesser timestamps
    Filtered_ops =
        lists:filtermap(
          fun(Payload) ->
                  {_Dcid, CommitTime} = Payload#clocksi_payload.commit_time,
                  CommitTime < Before
          end,
          Unprocessed_ops),

    %% Sort operations in timestamp order
    Sorted_ops =
        lists:sort(
          fun( Payload1, Payload2) ->
                  {_Dcid1, Time1} = Payload1#clocksi_payload.commit_time,
                  {_Dcid1, Time2} = Payload2#clocksi_payload.commit_time,
                  Time1 < Time2
          end,
          Filtered_ops),
    Sorted_ops.

%%@doc Add updates in writeset ot Pending operations to process downstream
add_to_pending_operations(Pending, WriteSet) ->
    case WriteSet of
        {TxId, Updates, Vec_snapshot_time, CommitTime} ->
            lists:foldl( fun(Update, Operations) ->
                                 {_,{Key, Type, {Op,Actor}}} = Update,
                                 DcId = dc_utilities:get_my_dc_id(),
                                 New_op = #clocksi_payload{
                                             key = Key, type = Type,
                                             op_param = {Op, Actor},
                                             snapshot_time = Vec_snapshot_time,
                                             commit_time = {DcId, CommitTime},
                                             txid = TxId},
                                 lists:append(Operations, [New_op])
                         end,
                         Pending, Updates);
        _  -> Pending
    end.
