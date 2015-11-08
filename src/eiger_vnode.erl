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
-module(eiger_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(EIGER_MASTER, eiger_vnode_master).

-export([start_vnode/1,
         read_key/4,
         read_key_time/5,
         prepare/4,
         prepare_replicated/3,
         commit/5,
         commit_replicated/3,
         coordinate_tx/3,
         check_deps/2,
         get_clock/1,
         update_clock/2,
         init/1,
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

-ignore_xref([start_vnode/1]).

-record(state, {partition,
                min_pendings=dict:new() :: dict(),
                buffered_reads=dict:new() :: dict(),
                pending=dict:new() :: dict(),
                prop_txs=dict:new() :: dict(),
                clock=0 :: integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


check_deps(Node, Deps) ->
    riak_core_vnode_master:command(Node,
                                   {check_deps, Deps},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

propagated_group_txs(Transactions) ->
    lists:foreach(fun(Txn) ->
                    {Txid,_Commitime,_ST, _Deps, _Ops} = Txn,
                    Preflist = log_utilities:get_preflist_from_key(Txid),
                    Indexnode = hd(Preflist),
                    notify_tx(Indexnode, Txn)
                  end, Transactions),
    ok.

notity_tx(Node, Transaction)->
    riak_core_vnode_master:command(Node,
                                   {notify_tx, Transaction},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER). 
clean_propagated_tx_fsm(Node, TxId)->
    riak_core_vnode_master:command(Node,
                                   {clean_propagated_tx_fsm, TxId},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER). 

read_key(Node, Key, Type, TxId) ->
    riak_core_vnode_master:command(Node,
                                   {read_key, Key, Type, TxId},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

read_key_time(Node, Key, Type, TxId, Clock) ->
    riak_core_vnode_master:command(Node,
                                   {read_key_time, Key, Type, TxId, Clock},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

prepare(Node, Transaction, Clock, Keys) ->
    riak_core_vnode_master:command(Node,
                                   {prepare, Transaction, Clock, Keys},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

prepare_replicated(Node, TxId, Keys) ->
    riak_core_vnode_master:command(Node,
                                   {prepare_replicated, TxId, Keys},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).
commit(Node, Transaction, Updates, Clock, TotalOps) ->
    riak_core_vnode_master:command(Node,
                                   {commit, Transaction, Updates, Clock, TotalOps},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

commit_replicated(Node, Transaction, Updates) ->
    riak_core_vnode_master:command(Node,
                                   {commit_replicated, Transaction, Updates},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

coordinate_tx(Node, Updates, Debug) ->
    riak_core_vnode_master:sync_command(Node,
                                        {coordinate_tx, Updates, Debug},
                                        ?EIGER_MASTER,
                                        infinity).

get_clock(Node) ->
    riak_core_vnode_master:sync_command(Node,
                                        get_clock,
                                        ?EIGER_MASTER,
                                        infinity).

update_clock(Node, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update_clock, Clock},
                                        ?EIGER_MASTER,
                                        infinity).
%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    {ok, #state{partition=Partition}}.

handle_command({check_deps, Deps}, Sender, State0) ->
    %RestDeps = lists:foldl(fun(Dep, Acc) ->
    %                        {reply, {Key, _Value, EVT, _Clock}, S1} = do_read(Key, Type, TxId, latest, State#state{clock=Clock}),
    %                        {C1, DC1} = EVT,
    %                        case (C1 < C2) of
    %                            true -> Acc + [Key];
    %                            false ->
    %                                case (C1 == C2) of
    %                                    true ->
    %                                        case (DC1<DC2) of
    %                                            true -> Acc + [Key];
    %                                            false -> Acc
    %                                        end;
    %                                    false -> Acc
    %                                end
    %                        end
 
    {reply, deps_checked, State0};

handle_command({clean_propagated_tx_fsm, TxId}, _Sender, S0=#state{prop_txs=PropTxs0}) ->
    PropTxs1 = dict:erase(TxId, PropTxs0),
    {noreply, S0#state{prop_txs=PropTxs1}};

handle_command({notify_tx, Transaction}, _Sender, State0=#state{prop_txs=PropTxs0, partition=Partition}) ->
    {Txid, _Commitime, _ST, Deps, Ops} = Transaction,
    case dict:find(Txid, PropTxs0) of
        {ok, FsmRef} ->
            gen_fsm:send_event(FsmRef, {notify, Ops, {Partition, node()}}),
            {noreply, State0};
        error ->
            {ok, FsmRef} = eiger_propagatedtx_coord_fsm:start_link({Partition, node()}, TxId, Committime, Deps, Ops),
            PropTxs1 = dict:store(Txid, FsmRef),
            {noreply, State0#state{prop_txs=PropTxs1}}
    end;

%% @doc starts a read_fsm to handle a read operation.
handle_command({read_key, Key, Type, TxId}, _Sender,
               #state{clock=Clock, min_pendings=MinPendings}=State) ->
    case dict:find(Key, MinPendings) of
        {ok, _Min} ->
            {reply, {Key, empty, empty, Clock}, State};
        error ->
            do_read(Key, Type, TxId, latest, State)
    end;

handle_command({read_key_time, Key, Type, TxId, Time}, Sender,
               #state{clock=Clock0, buffered_reads=BufferedReads0, min_pendings=MinPendings}=State) ->
    Clock = max(Clock0, Time),
    case dict:find(Key, MinPendings) of
        {ok, Min} ->
            case Min =< Time of
                true ->
                    case dict:find(Key, BufferedReads0) of
                        {ok, Orddict0} ->
                            Orddict = orddict:store(Time, {Sender, Type, TxId}, Orddict0);
                        error ->
                            Orddict0 = orddict:new(),
                            Orddict = orddict:store(Time, {Sender, Type, TxId}, Orddict0)
                    end,
                    BufferedReads = dict:store(Key, Orddict, BufferedReads0),
                    {noreply, State#state{clock=Clock, buffered_reads=BufferedReads}};
                false ->
                    do_read(Key, Type, TxId, Time, State#state{clock=Clock})
            end;
        error ->
            do_read(Key, Type, TxId, Time, State#state{clock=Clock})
    end;

handle_command({coordinate_tx, Updates, Debug}, Sender, #state{partition=Partition}=State) ->
    Vnode = {Partition, node()},
    {ok, _Pid} = eiger_updatetx_coord_fsm:start_link(Vnode, Sender, Updates, Debug),
    {noreply, State};

handle_command({remote_prepare, TxId, TimeStamp, Keys0}, _Sender, #state{clock=Clock0, pending=Pending0, min_pendings=MinPendings0, partition=Partition}=State) ->
    {DC2, C2} = Commit,
    Clock = max(Clock0, C2) + 1,
    Keys1 = lists:foldl(fun(Key, Acc) ->
                            {reply, {Key, _Value, EVT, _Clock}, S1} = do_read(Key, Type, TxId, latest, State#state{clock=Clock}),
                            {C1, DC1} = EVT,
                            case (C1 < C2) of
                                true -> Acc + [Key];
                                false ->
                                    case (C1 == C2) of
                                        true ->
                                            case (DC1<DC2) of
                                                true -> Acc + [Key];
                                                false -> Acc
                                            end;
                                        false -> Acc
                                    end
                            end
                        end, [], Keys0),
    S1 = do_prepare(TxId, Clock, Keys1, S0),
    {reply, {prepared, Clock, Keys1, {Partition, node()}}, S1#state{clock=Clock}};

handle_command({prepare, TxId, CoordClock, Keys}, _Sender, S0=#state{clock=Clock0}) ->
    Clock = max(Clock0, CoordClock) + 1;
    S1 = do_prepare(TxId, Clock, Keys, S0),, 
    {reply, {prepared, Clock}, S1#state{clock=Clock}}
    end;

handle_command({commit, TxId, Updates, CommitTime, TotalOps}, _Sender, State0=#state{clock=Clock0}) ->
    {_Dc, CommitClock} = CommitTime,
    Clock = max(Clock0, CommitClock),
    case update_keys(Updates, [], TxId, CommitTime, TotalOps, [], State0) of
        {ok, State} ->
            {reply, {committed, Clock}, State#state{clock=Clock}};
        {error, Reason} ->
            {reply, {error, Reason}, State0#state{clock=Clock}}
    end;

handle_command(get_clock, _Sender, S0=#state{clock=Clock}) ->
    {reply, {ok, Clock}, S0};

handle_command({update_clock, NewClock}, _Sender, S0=#state{clock=Clock0}) ->
    Clock =  max(Clock0, NewClock),
    {reply, ok, S0#state{clock=Clock}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

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

update_keys([], Deps, TxId, CommitTime, TotalOps, DownstreamOps, State0) ->
    {_DCFrom, CommitClock} = CommitTime,
    FirstDSOp = hd(DownstreamOps),
    Key = FirstDSOp#clocksi_payload.key,
    LogId = log_utilities:get_logid_from_key(Key),
    [Node] = log_utilities:get_preflist_from_key(Key),
    PreparedLogRecord = #log_record{tx_id=TxId,
                                    op_type=prepare,
                                    op_payload=CommitTime},
    logging_vnode:append(Node,LogId,PreparedLogRecord),
    CommitLogRecord=#log_record{tx_id=TxId,
                                op_type=commit,
                                op_payload={CommitTime, null, Deps, TotalOps}},
    case logging_vnode:append(Node,LogId,CommitLogRecord) of
        {ok, _} ->
            State = lists:foldl(fun(Op, S0) ->
                                    Key = Op#clocksi_payload.key,
                                    %% This can only return ok, it is therefore pointless to check the return value.
                                    eiger_materializer_vnode:update(Key, Op),
                                    post_commit_update(Key, TxId, CommitClock, S0)
                                end, State0, DownstreamOps),
            {ok, State};
        {error, timeout} ->
            error
    end;

update_keys([Update|Rest], Deps, TxId, CommitTime, TotalOps, DownstreamOps, State0) ->
    {Key, Type, _} = Update,
    DSOp = eiger_downstream:generate_downstream_op(Update),
    LogRecord = #log_record{tx_id=TxId, op_type=update, op_payload={Key, Type, DSOp}},
    LogId = log_utilities:get_logid_from_key(Key),
    [Node] = log_utilities:get_preflist_from_key(Key),
    Result = logging_vnode:append(Node,LogId,LogRecord),
    case Result of
        {ok, _} ->
            CommittedDownstreamOp = #clocksi_payload{
                                    key = Key,
                                    type = Type,
                                    op_param = DSOp,
                                    snapshot_time = null,
                                    commit_time = CommitTime,
                                    txid = TxId},
            update_keys(Rest, Deps, TxId, CommitTime, TotalOps, DownstreamOps ++ [CommittedDownstreamOp], State0);
        {error, _Reason} ->
            error
    end.
    
post_commit_update(Key, TxId, CommitTime, State0=#state{pending=Pending0, min_pendings=MinPendings0, buffered_reads=BufferedReads0, clock=Clock}) ->
    List0 = dict:fetch(Key, Pending0),
    {List, PrepareTime} = delete_pending_entry(List0, TxId, []),
    case List of
        [] ->
            Pending = dict:erase(Key, Pending0),
            MinPendings = dict:erase(Key, MinPendings0),
            case dict:find(Key, BufferedReads0) of
                {ok, Orddict0} ->
                    lists:foreach(fun({Time, {Client, TypeB, TxIdB}}) ->
                                    {reply, Reply, _S} = do_read(Key, TypeB, TxIdB, Time, State0),
                                    riak_core_vnode:reply(Client, Reply)
                                  end, Orddict0),
                    BufferedReads=dict:erase(Key, BufferedReads0),
                    State=State0#state{pending=Pending, min_pendings=MinPendings, buffered_reads=BufferedReads};
                error ->
                    State=State0#state{pending=Pending, min_pendings=MinPendings}
            end;
        _ ->
            Pending = dict:store(Key, List, Pending0),
            case dict:fetch(Key, MinPendings0) < PrepareTime of
                true ->
                    State=State0#state{pending=Pending};
                false ->
                    Times = [PT || {_TxId, PT} <- List],
                    Min = lists:min(Times),
                    MinPendings =  dict:store(Key, Min, MinPendings0),
                    case dict:find(Key, BufferedReads0) of
                        {ok, Orddict0} ->
                            case handle_pending_reads(Orddict0, CommitTime, Key, Clock) of
                                [] ->
                                    BufferedReads = dict:erase(Key, BufferedReads0);
                                Orddict ->
                                    BufferedReads = dict:store(Key, Orddict, BufferedReads0)
                            end,
                            State=State0#state{pending=Pending, min_pendings=MinPendings, buffered_reads=BufferedReads};
                        error ->
                            State=State0#state{pending=Pending, min_pendings=MinPendings}
                    end
            end
    end,
    State.

delete_pending_entry([], _TxId, List) ->
    {List, not_found};

delete_pending_entry([Element|Rest], TxId, List) ->
    case Element of
        {PrepareTime, TxId} ->
            {List ++ Rest, PrepareTime};
        _ ->
            delete_pending_entry(Rest, TxId, List ++ [Element])
    end.

handle_pending_reads([], _CommitClock, _Key, _Clock) ->
    [];

handle_pending_reads([Element|Rest], CommitClock, Key, Clock) ->
    {Time, Type, TxId, Client} = Element,
    case Time < CommitClock of
        true ->
            {reply, Reply, _State} = do_read(Key, Type, TxId, Time, #state{clock=Clock}),
            riak_core_vnode:reply(Client, Reply),
            handle_pending_reads(Rest, CommitClock, Key, Clock);
        false ->
            [Element|Rest]
    end.

do_read(Key, Type, TxId, Time, State=#state{clock=Clock}) -> 
    case eiger_materializer_vnode:read(Key, Type, Time, TxId) of
    %case eiger_materializer_vnode:read(Key, Time) of
        {ok, Snapshot, {_CoordId, EVT}} ->
            Value = Type:value(Snapshot),
            case Time of
                latest -> 
                    Reply = {Key, Value, EVT, Clock};
                _ -> 
                    Reply = {Key, Value}
            end,
            {reply, Reply, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

do_prepare(TxId, Clock, Keys, S0=#state{pending=Pending0, min_pendings=MinPendings0}) ->
    {Pending, MinPendings} = lists:foldl(fun(Key, {P0, MP0}) ->
                                            P = dict:append(Key, {Clock, TxId}, P0),
                                            MP = case dict:find(Key, MP0) of
                                                    {ok, _Min} ->
                                                        MP0;
                                                    _ ->
                                                        dict:store(Key, Clock, MP0)
                                                 end,
                                            {P, MP}
                                         end, {Pending0, MinPendings0}, Keys),
    S0#state{pending=Pending, min_pendings=MinPendings}.
