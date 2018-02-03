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
-module(materializer_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%% Number of snapshots to trigger GC
-define(SNAPSHOT_THRESHOLD, 10).
%% Number of snapshots to keep after GC
-define(SNAPSHOT_MIN, 2).
%% Expected time to wait until the logging vnode is up
-define(LOG_STARTUP_WAIT, 1000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
         check_tables_ready/0,
         read/5,
         update/2]).

%% Callbacks
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

-type op_and_id() :: {non_neg_integer(), #clocksi_payload{}}.
-record(state, {
    partition :: partition_id(),
    snapshot_cache :: cache_id(),
    is_ready :: boolean()
}).

%%---------------- API Functions -------------------%%

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time, this does not touch the vnode process
%%      directly, instead it just reads from the operations and snapshot tables that
%%      are in shared memory, allowing concurrent reads.
-spec read(key(), type(), snapshot_time(), clocksi_readitem_server:read_property_list(), partition_id()) -> {ok, snapshot()} | {error, reason()}.
read(Key, Type, SnapshotTime, PropertyList, Partition) ->
    SnapshotCache = get_cache_name(Partition, snapshot_cache),

    State = #state{snapshot_cache=SnapshotCache, partition=Partition, is_ready=false},
    internal_read(Key, Type, SnapshotTime, PropertyList, false, State).

%%@doc write operation to cache for future read, updates are stored
%%     one at a time into the ets tables
-spec update(key(), clocksi_payload()) -> ok | {error, reason()}.
update(Key, DownstreamOp) ->
    IndexNode = log_utilities:get_key_partition(Key),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp},
                                        materializer_vnode_master).

init([Partition]) ->
    OpsCache = open_table(Partition, ops_cache),
    SnapshotCache = open_table(Partition, snapshot_cache),
    IsReady = case application:get_env(antidote, recover_from_log) of
                {ok, true} ->
                    lager:debug("Trying to recover the materializer from log ~p", [Partition]),
                    riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
                    false;
                _ ->
                    true
    end,
    {ok, #state{is_ready = IsReady, partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache}}.


%% @doc The tables holding the updates and snapshots are shared with concurrent non-blocking
%%      readers.
%%      Returns true if all tables have been initialized, false otherwise.
-spec check_tables_ready() -> boolean().
check_tables_ready() ->
    PartitionList = dc_utilities:get_all_partitions_nodes(),
    check_table_ready(PartitionList).

-spec check_table_ready([{partition_id(), node()}]) -> boolean().
check_table_ready([]) ->
    true;
check_table_ready([{Partition, Node}|Rest]) ->
    Result =
        try
            riak_core_vnode_master:sync_command({Partition, Node},
                                                {check_ready},
                                                materializer_vnode_master,
                                                infinity)
        catch
            _:_Reason ->
                false
        end,
    case Result of
        true ->
            check_table_ready(Rest);
        false ->
            false
    end.

handle_command({hello}, _Sender, State) ->
  {reply, ok, State};

handle_command({check_ready}, _Sender, State = #state{partition=Partition, is_ready=IsReady}) ->
    Result = case ets:info(get_cache_name(Partition, ops_cache)) of
                 undefined ->
                     false;
                 _ ->
                     case ets:info(get_cache_name(Partition, snapshot_cache)) of
                         undefined ->
                             false;
                         _ ->
                             true
                     end
             end,
    Result2 = Result and IsReady,
    {reply, Result2, State};

handle_command({read, Key, Type, SnapshotTime}, _Sender, State) ->
    {reply, read(Key, Type, SnapshotTime, [], State), State};

handle_command({update, Key, DownstreamOp}, _Sender, State) ->
    true = op_insert_gc(Key, DownstreamOp, State),
    {reply, ok, State};

% handle_command(load_from_log, _Sender, State=#state{partition=Partition}) ->
%     IsReady = try
%                 case load_from_log_to_tables(Partition, State) of
%                     ok ->
%                         lager:debug("Finished loading from log to materializer on partition ~w", [Partition]),
%                         true;
%                     {error, not_ready} ->
%                         false;
%                     {error, Reason} ->
%                         lager:error("Unable to load logs from disk: ~w, continuing", [Reason]),
%                         true
%                 end
%             catch
%                 _:Reason1 ->
%                     lager:debug("Error loading from log ~w, will retry", [Reason1]),
%                     false
%             end,
%     ok = case IsReady of
%             false ->
%                 riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
%                 ok;
%             true ->
%                 ok
%     end,
%     {noreply, State#state{is_ready=IsReady}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0},
                       _Sender,
                       State = #state{ops_cache = OpsCache}) ->
    F = fun(Key, A) ->
            [Key1|_] = tuple_to_list(Key),
            Fun(Key1, Key, A)
        end,
    Acc = ets:foldl(F, Acc0, OpsCache),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {stop, not_implemented, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{snapshot_cache=SnapshotCache}) ->
    case ets:first(SnapshotCache) of
        '$end_of_table' ->
            {true, State};
        _ ->
            {false, State}
    end.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State=#state{snapshot_cache=SnapshotCache}) ->
    try
        ets:delete(SnapshotCache)
    catch
        _:_Reason->
            ok
    end,
    ok.



%%---------------- Internal Functions -------------------%%

-spec get_cache_name(non_neg_integer(), atom()) -> atom().
get_cache_name(Partition, Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).

% -spec load_from_log_to_tables(partition_id(), #state{}) -> ok | {error, reason()}.
% load_from_log_to_tables(Partition, State) ->
%     LogId = [Partition],
%     Node = {Partition, log_utilities:get_my_node(Partition)},
%     loop_until_loaded(Node, LogId, start, dict:new(), State).

% -spec loop_until_loaded({partition_id(), node()},
%             log_id(),
%             start | disk_log:continuation(),
%             dict:dict(txid(), [any_log_payload()]),
%             #state{}) ->
%                    ok | {error, reason()}.
% loop_until_loaded(Node, LogId, Continuation, Ops, State) ->
%     case logging_vnode:get_all(Node, LogId, Continuation, Ops) of
%         {error, Reason} ->
%             {error, Reason};
%         {NewContinuation, NewOps, OpsDict} ->
%             load_ops(OpsDict, State),
%             loop_until_loaded(Node, LogId, NewContinuation, NewOps, State);
%         {eof, OpsDict} ->
%             load_ops(OpsDict, State),
%             ok
%     end.

-spec open_table(partition_id(), 'snapshot_cache') -> atom() | ets:tid().
open_table(Partition, Name) ->
    case ets:info(get_cache_name(Partition, Name)) of
        undefined ->
            ets:new(get_cache_name(Partition, Name),
                [set, protected, named_table, ?TABLE_CONCURRENCY]);
        _ ->
            %% Other vnode hasn't finished closing tables
            lager:debug("Unable to open ets table in materializer vnode, retrying"),
            timer:sleep(100),
            try
                ets:delete(get_cache_name(Partition, Name))
            catch
                _:_Reason ->
                    ok
            end,
            open_table(Partition, Name)
    end.

-ifdef(TEST).

%% This tests to make sure when garbage collection happens, no updates are lost
gc_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    DC1 = 1,
    Type = antidote_crdt_counter,

    %% Make 10 snapshots
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    {ok, Res0} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2}]), ignore, [], false, State),
    ?assertEqual(0, Type:value(Res0)),

    op_insert_gc(Key, generate_payload(10, 11, Res0, a1), State),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 12}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(Res1)),

    op_insert_gc(Key, generate_payload(20, 21, Res1, a2), State),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 22}]), ignore, [], false, State),
    ?assertEqual(2, Type:value(Res2)),

    op_insert_gc(Key, generate_payload(30, 31, Res2, a3), State),
    {ok, Res3} = internal_read(Key, Type, vectorclock:from_list([{DC1, 32}]), ignore, [], false, State),
    ?assertEqual(3, Type:value(Res3)),

    op_insert_gc(Key, generate_payload(40, 41, Res3, a4), State),
    {ok, Res4} = internal_read(Key, Type, vectorclock:from_list([{DC1, 42}]), ignore, [], false, State),
    ?assertEqual(4, Type:value(Res4)),

    op_insert_gc(Key, generate_payload(50, 51, Res4, a5), State),
    {ok, Res5} = internal_read(Key, Type, vectorclock:from_list([{DC1, 52}]), ignore, [], false, State),
    ?assertEqual(5, Type:value(Res5)),

    op_insert_gc(Key, generate_payload(60, 61, Res5, a6), State),
    {ok, Res6} = internal_read(Key, Type, vectorclock:from_list([{DC1, 62}]), ignore, [], false, State),
    ?assertEqual(6, Type:value(Res6)),

    op_insert_gc(Key, generate_payload(70, 71, Res6, a7), State),
    {ok, Res7} = internal_read(Key, Type, vectorclock:from_list([{DC1, 72}]), ignore, [], false, State),
    ?assertEqual(7, Type:value(Res7)),

    op_insert_gc(Key, generate_payload(80, 81, Res7, a8), State),
    {ok, Res8} = internal_read(Key, Type, vectorclock:from_list([{DC1, 82}]), ignore, [], false, State),
    ?assertEqual(8, Type:value(Res8)),

    op_insert_gc(Key, generate_payload(90, 91, Res8, a9), State),
    {ok, Res9} = internal_read(Key, Type, vectorclock:from_list([{DC1, 92}]), ignore, [], false, State),

    ?assertEqual(9, Type:value(Res9)),

    op_insert_gc(Key, generate_payload(100, 101, Res9, a10), State),

    %% Insert some new values

    op_insert_gc(Key, generate_payload(15, 111, Res1, a11), State),
    op_insert_gc(Key, generate_payload(16, 121, Res1, a12), State),

    %% Trigger the clean
    {ok, Res10} = internal_read(Key, Type, vectorclock:from_list([{DC1, 102}]), ignore, [], true, State),
    ?assertEqual(10, Type:value(Res10)),

    op_insert_gc(Key, generate_payload(102, 131, Res9, a13), State),

    %% Be sure you didn't loose any updates
    {ok, Res13} = internal_read(Key, Type, vectorclock:from_list([{DC1, 142}]), ignore, [], true, State),
    ?assertEqual(13, Type:value(Res13)).

%% This tests to make sure operation lists can be large and resized
large_list_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    DC1 = 1,
    Type = antidote_crdt_counter,
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Make 1000 updates to grow the list, whithout generating a snapshot to perform the gc
    {ok, Res0} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2}]), ignore, [], false, State),
    ?assertEqual(0, Type:value(Res0)),

    lists:foreach(fun(Val) ->
                      op_insert_gc(Key, generate_payload(10, 11+Val, Res0, Val), State)
                  end, lists:seq(1, 1000)),

    {ok, Res1000} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2000}]), ignore, [], false, State),
    ?assertEqual(1000, Type:value(Res1000)),

    %% Now check everything is ok as the list shrinks from generating new snapshots
    lists:foreach(fun(Val) ->
                  op_insert_gc(Key, generate_payload(10+Val, 11+Val, Res0, Val), State),
                  {ok, Res} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2000}]), ignore, [], false, State),
                  ?assertEqual(Val, Type:value(Res))
              end, lists:seq(1001, 1100)).

generate_payload(SnapshotTime, CommitTime, Prev, _Name) ->
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = 1,

    {ok, Op1} = Type:downstream({increment, 1}, Prev),
    #clocksi_payload{key = Key,
                     type = Type,
                     op_param = Op1,
                     snapshot_time = vectorclock:from_list([{DC1, SnapshotTime}]),
                     commit_time = {DC1, CommitTime},
                     txid = 1
                    }.

seq_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = 1,
    S1 = Type:new(),
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Insert one increment
    {ok, Op1} = Type:downstream({increment, 1}, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = Op1,
                                     snapshot_time = vectorclock:from_list([{DC1, 10}]),
                                     commit_time = {DC1, 15},
                                     txid = 1
                                    },
    op_insert_gc(Key, DownstreamOp1, State),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(Res1)),
    %% Insert second increment
    {ok, Op2} = Type:downstream({increment, 1}, S1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = Op2,
                      snapshot_time = vectorclock:from_list([{DC1, 16}]),
                      commit_time = {DC1, 20},
                      txid = 2},

    op_insert_gc(Key, DownstreamOp2, State),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 21}]), ignore, [], false, State),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(ReadOld)).

multipledc_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = 1,
    DC2 = 2,
    S1 = Type:new(),
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},


    %% Insert one increment in DC1
    {ok, Op1} = Type:downstream({increment, 1}, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = Op1,
                                     snapshot_time = vectorclock:from_list([{DC2, 0}, {DC1, 10}]),
                                     commit_time = {DC1, 15},
                                     txid = 1
                                    },
    op_insert_gc(Key, DownstreamOp1, State),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}, {DC2, 0}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(Res1)),

    %% Insert second increment in other DC
    {ok, Op2} = Type:downstream({increment, 1}, S1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = Op2,
                      snapshot_time = vectorclock:from_list([{DC2, 16}, {DC1, 16}]),
                      commit_time = {DC2, 20},
                      txid = 2},
    op_insert_gc(Key, DownstreamOp2, State),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}, {DC2, 21}]), ignore, [], false, State),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1, 15}, {DC2, 15}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(ReadOld)).

concurrent_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = local,
    DC2 = remote,
    S1 = Type:new(),
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Insert one increment in DC1
    {ok, Op1} = Type:downstream({increment, 1}, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = Op1,
                                     snapshot_time = vectorclock:from_list([{DC1, 0}, {DC2, 0}]),
                                     commit_time = {DC2, 1},
                                     txid = 1},
    op_insert_gc(Key, DownstreamOp1, State),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC2, 1}, {DC1, 0}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(Res1)),

    %% Another concurrent increment in other DC
    {ok, Op2} = Type:downstream({increment, 1}, S1),
    DownstreamOp2 = #clocksi_payload{ key = Key,
                                      type = Type,
                                      op_param = Op2,
                                      snapshot_time = vectorclock:from_list([{DC1, 0}, {DC2, 0}]),
                                      commit_time = {DC1, 1},
                                      txid = 2},
    op_insert_gc(Key, DownstreamOp2, State),

    %% Read different snapshots
    {ok, ReadDC1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 1}, {DC2, 0}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(ReadDC1)),
    io:format("Result1 = ~p", [ReadDC1]),
    {ok, ReadDC2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 0}, {DC2, 1}]), ignore, [], false, State),
    io:format("Result2 = ~p", [ReadDC2]),
    ?assertEqual(1, Type:value(ReadDC2)),

    %% Read snapshot including both increments
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC2, 1}, {DC1, 1}]), ignore, [], false, State),
    ?assertEqual(2, Type:value(Res2)).

%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%% E.g., for a gcounter, return 0.
read_nonexisting_key_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},
    Type = antidote_crdt_counter,
    {ok, ReadResult} = internal_read(key, Type, vectorclock:from_list([{dc1, 1}, {dc2, 0}]), ignore, [], false, State),
    ?assertEqual(0, Type:value(ReadResult)).

-endif.
