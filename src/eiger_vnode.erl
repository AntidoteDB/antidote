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
         remote_prepare/4,
         commit/7,
         coordinate_tx/4,
         check_deps/2,
         notify_tx/2,
         propagated_group_txs/1,
         clean_propagated_tx_fsm/3,
         get_clock/1,
         update_clock/2,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         eiger_ts_lt/2,
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
                prop_txs,
                idle_fsms=queue:new() :: queue(),
                fsm_deps=dict:new() :: dict(),
                deps_keys=dict:new() :: dict(),
                queue_fsm=queue:new() :: queue(),
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
                    {Txid,_Commitime,_ST, _Deps, _Ops, _TOps} = Txn,
                    Preflist = log_utilities:get_preflist_from_key(Txid),
                    Indexnode = hd(Preflist),
                    notify_tx(Indexnode, Txn)
                  end, Transactions),
    ok.

notify_tx(Node, Transaction)->
    riak_core_vnode_master:command(Node,
                                   {notify_tx, Transaction},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER). 
clean_propagated_tx_fsm(Node, TxId, FsmRef)->
    riak_core_vnode_master:command(Node,
                                   {clean_propagated_tx_fsm, TxId, FsmRef},
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

remote_prepare(Node, TxId, TimeStamp, Keys) ->
    riak_core_vnode_master:command(Node,
                                   {remote_prepare, TxId, TimeStamp, Keys},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).
commit(Node, Transaction, Updates, Deps, TimeStamp, Clock, TotalOps) ->
    riak_core_vnode_master:command(Node,
                                   {commit, Transaction, Updates, Deps, TimeStamp, Clock, TotalOps},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

coordinate_tx(Node, Updates, Deps, Debug) ->
    riak_core_vnode_master:sync_command(Node,
                                        {coordinate_tx, Updates, Deps, Debug},
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
    Name = list_to_atom(integer_to_list(Partition) ++ atom_to_list(prop_txs)),
    PropTxs0 = ets:new(Name, [set, named_table]),
    {ok, Queue} = init_prop_fsms({Partition, node()}, queue:new(), ?NPROP_FSMS), 
    {ok, #state{partition=Partition, prop_txs=PropTxs0, idle_fsms=Queue}}.

init_prop_fsms(_Vnode, Queue0, 0) ->
    {ok, Queue0};

init_prop_fsms(Vnode, Queue0, Rest) ->
    {ok, FsmRef} = eiger_propagatedtx_coord_fsm:start_link(Vnode),
    Queue1 = queue:in(FsmRef, Queue0),
    init_prop_fsms(Vnode, Queue1, Rest-1).
    

handle_command({check_deps, Deps}, Sender, S0=#state{fsm_deps=FsmDeps0, deps_keys=DepsKeys0, partition=_Partition}) ->
    RestDeps = lists:foldl(fun({Key, TimeStamp}=Dep, Acc) ->
                            {Key, _Value, _EVT, _Clock, TS2} = do_read(Key, ?EIGER_DATATYPE, latest, latest, S0),
                            case eiger_ts_lt(TS2, TimeStamp) of
                                true ->
                                    Acc ++ [Dep];
                                false ->
                                    Acc
                            end
                        end, [], Deps),
    case length(RestDeps) of
        0 ->
            {reply, deps_checked, S0};
        Other ->
            FsmDeps1 = dict:store(Sender, Other, FsmDeps0),
            DepsKeys1 = lists:foldl(fun({Key, TimeStamp}=_Dep, Acc) ->
                                        dict:append(Key, {TimeStamp, Sender}, Acc)
                                    end, DepsKeys0, RestDeps),
            {noreply, S0#state{fsm_deps=FsmDeps1, deps_keys=DepsKeys1}}
    end;

handle_command({clean_propagated_tx_fsm, TxId, FsmRef}, _Sender, S0=#state{prop_txs=PropTxs, queue_fsm=Queue0, idle_fsms=IdleFsms0}) ->
    true = ets:delete(PropTxs, TxId),
    case queue:out(Queue0) of
        {{value, {TxIdPending, CommitTime, Deps}}, Queue1} ->
            [{TxIdPending, {queue, ListOps}}] = ets:lookup(PropTxs, TxIdPending),
            gen_fsm:send_event(FsmRef, {new_tx, TxId, CommitTime, Deps, ListOps}),
            true = ets:insert(PropTxs, {TxId, {running, FsmRef}}),
            {noreply, S0#state{queue_fsm=Queue1}};
        {empty, _Queue1} ->
            IdleFsms1 = queue:in(FsmRef, IdleFsms0),
            {noreply, S0#state{idle_fsms=IdleFsms1}}
    end;

handle_command({notify_tx, Transaction}, _Sender, State0=#state{prop_txs=PropTxs, partition=Partition, queue_fsm=Queue0, idle_fsms=IdleFsms0}) ->
    {TxId, CommitTime, _ST, Deps, Ops, _TOps} = Transaction,
    case ets:lookup(PropTxs, TxId) of
        [{TxId, {running, FsmRef}}] ->
            gen_fsm:send_event(FsmRef, {notify, Ops, {Partition, node()}}),
            {noreply, State0};
        [{TxId, {queue, OpsList}}] ->
            true = ets:insert(PropTxs, {TxId, {queue, [Ops|OpsList]}}),
            {noreply, State0};
        [] ->
            case queue:out(IdleFsms0) of
                {{value, FsmRef}, IdleFsms1} ->
                    gen_fsm:send_event(FsmRef, {new_tx, TxId, CommitTime, Deps, [Ops]}),
                    true = ets:insert(PropTxs, {TxId, {running, FsmRef}}),
                    {noreply, State0#state{idle_fsms=IdleFsms1}};
                true ->
                    Queue1 = queue:in({TxId, CommitTime, Deps}, Queue0),
                    true = ets:insert(PropTxs, {TxId, {queue, [Ops]}}),
                    {noreply, State0#state{queue_fsm=Queue1}}
            end
    end;

%% @doc starts a read_fsm to handle a read operation.
handle_command({read_key, Key, Type, TxId}, _Sender,
               #state{clock=Clock, min_pendings=MinPendings}=State) ->
    case dict:find(Key, MinPendings) of
        {ok, _Min} ->
            {reply, {Key, empty, empty, Clock, empty}, State};
        error ->
            Reply = do_read(Key, Type, TxId, latest, State),
            {reply, Reply, State}
    end;

handle_command({read_key_time, Key, Type, TxId, Time}, Sender,
               #state{clock=Clock0, buffered_reads=BufferedReads0, min_pendings=MinPendings}=State) ->
    Clock = max(Clock0, Time),
    case dict:find(Key, MinPendings) of
        {ok, Min} ->
            case Min =< Time of
                true ->
                    Orddict = case dict:find(Key, BufferedReads0) of
                                {ok, Orddict0} ->
                                    orddict:store(Time, {Sender, Type, TxId}, Orddict0);
                                error ->
                                    Orddict0 = orddict:new(),
                                    orddict:store(Time, {Sender, Type, TxId}, Orddict0)
                              end,
                    BufferedReads = dict:store(Key, Orddict, BufferedReads0),
                    {noreply, State#state{clock=Clock, buffered_reads=BufferedReads}};
                false ->
                    Reply = do_read(Key, Type, TxId, Time, State#state{clock=Clock}),
                    {reply, Reply, State#state{clock=Clock}} 
            end;
        error ->
            Reply = do_read(Key, Type, TxId, Time, State#state{clock=Clock}),
            {reply, Reply, State#state{clock=Clock}} 
    end;

handle_command({coordinate_tx, Updates, Deps, Debug}, Sender, #state{partition=Partition}=State) ->
    Vnode = {Partition, node()},
    {ok, _Pid} = eiger_updatetx_coord_fsm:start_link(Vnode, Sender, Updates, Deps, Debug),
    {noreply, State};

handle_command({remote_prepare, TxId, TimeStamp, Keys0}, _Sender, #state{clock=Clock0, partition=Partition}=S0) ->
    {_DC1, C1} = TimeStamp,
    Clock = max(Clock0, C1) + 1,
    Keys1 = lists:foldl(fun(Key, Acc) ->
                            {Key, _Value, _EVT, _Clock, TS2} = do_read(Key, ?EIGER_DATATYPE, TxId, latest, S0#state{clock=Clock}),
                            case eiger_ts_lt(TS2, TimeStamp) of
                                true ->
                                    Acc ++ [Key];
                                false ->
                                    Acc
                            end
                        end, [], Keys0),
    %lager:info("Keys ~p to be prepared. TimeStamp: ~p", [Keys1, TimeStamp]),
    S1 = do_prepare(TxId, Clock, Keys1, S0),
    {reply, {prepared, Clock, Keys1, {Partition, node()}}, S1#state{clock=Clock}};

handle_command({prepare, Transaction, CoordClock, Keys}, _Sender, S0=#state{clock=Clock0}) ->
    Clock = max(Clock0, CoordClock) + 1,
    S1 = do_prepare(Transaction#transaction.txn_id, Clock, Keys, S0),
    {reply, {prepared, Clock}, S1#state{clock=Clock}};

handle_command({commit, Transaction, Updates, Deps, TimeStamp, CommitClock, TotalOps}, _Sender, State0=#state{clock=Clock0}) ->
    Clock = max(Clock0, CommitClock),
    case update_keys(Updates, Deps, Transaction, TimeStamp, CommitClock, TotalOps, State0) of
        {ok, State} ->
            {reply, {committed, CommitClock}, State#state{clock=Clock}};
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

update_keys(Ups, Deps, Transaction, {_DcId, _TimeStampClock}=TimeStamp, CommitTime, TotalOps, State0) ->
    Payloads = lists:foldl(fun(Update, Acc) ->
                    {Key, Type, _} = Update,
                    TxId = Transaction#transaction.txn_id,
                    LogRecord = #log_record{tx_id=TxId, op_type=update, op_payload={Key, Type, Update}},
                    LogId = log_utilities:get_logid_from_key(Key),
                    [Node] = log_utilities:get_preflist_from_key(Key),
                    case Deps of
                        nodeps ->
                            {ok, _} = logging_vnode:nonlocal_append(Node,LogId,LogRecord);
                        _ ->    
                            {ok, _} = logging_vnode:append(Node,LogId,LogRecord)
                    end,
                    CommittedOp = #clocksi_payload{
                                            key = Key,
                                            type = Type,
                                            op_param = Update,
                                            snapshot_time = Transaction#transaction.vec_snapshot_time,
                                            commit_time = TimeStamp,
                                            evt = CommitTime,
                                            txid = Transaction#transaction.txn_id},
                    [CommittedOp|Acc] end, [], Ups),
    FirstOp = hd(Ups),
    {Key, _, _} = FirstOp,
    LogId = log_utilities:get_logid_from_key(Key),
    [Node] = log_utilities:get_preflist_from_key(Key),
    TxId = Transaction#transaction.txn_id,
    PreparedLogRecord = #log_record{tx_id=TxId,
                                    op_type=prepare,
                                    op_payload=CommitTime},
    case Deps of
        nodeps ->
            {ok, _} = logging_vnode:nonlocal_append(Node,LogId,PreparedLogRecord);
        _ ->    
            {ok, _} = logging_vnode:append(Node,LogId,PreparedLogRecord)
    end,
    CommitLogRecord=#log_record{tx_id=TxId,
                                op_type=commit,
                                op_payload={TimeStamp, Transaction#transaction.vec_snapshot_time, Deps, TotalOps}},
    case Deps of
        nodeps ->
            ResultCommit = logging_vnode:nonlocal_append(Node,LogId,CommitLogRecord);
        _ ->    
            ResultCommit = logging_vnode:append(Node,LogId,CommitLogRecord)
    end,
    case ResultCommit of
        {ok, _} ->
            State = lists:foldl(fun(Op, S0) ->
                                    LKey = Op#clocksi_payload.key,
                                    %% This can only return ok, it is therefore pointless to check the return value.
                                    eiger_materializer_vnode:update(LKey, Op),
                                    S1 = post_commit_dependencies(LKey, TimeStamp, S0),
                                    post_commit_update(LKey, TxId, CommitTime, S1)
                                end, State0, Payloads),
            {ok, State};
        {error, timeout} ->
            error
    end.
    
post_commit_update(Key, TxId, CommitTime, State0=#state{pending=Pending0, min_pendings=MinPendings0, buffered_reads=BufferedReads0, clock=Clock}) ->
    %lager:info("Key to post commit : ~p", [Key]),
    List0 = dict:fetch(Key, Pending0),
    {List, PrepareTime} = delete_pending_entry(List0, TxId, []),
    case List of
        [] ->
            Pending = dict:erase(Key, Pending0),
            MinPendings = dict:erase(Key, MinPendings0),
            case dict:find(Key, BufferedReads0) of
                {ok, Orddict0} ->
                    lists:foreach(fun({Time, {Client, TypeB, TxIdB}}) ->
                                    Reply = do_read(Key, TypeB, TxIdB, Time, State0),
                                    riak_core_vnode:reply(Client, Reply)
                                  end, Orddict0),
                    BufferedReads=dict:erase(Key, BufferedReads0),
                    State0#state{pending=Pending, min_pendings=MinPendings, buffered_reads=BufferedReads};
                error ->
                    State0#state{pending=Pending, min_pendings=MinPendings}
            end;
        _ ->
            Pending = dict:store(Key, List, Pending0),
            case dict:fetch(Key, MinPendings0) < PrepareTime of
                true ->
                    State0#state{pending=Pending};
                false ->
                    Times = [PT || {_TxId, PT} <- List],
                    Min = lists:min(Times),
                    MinPendings =  dict:store(Key, Min, MinPendings0),
                    case dict:find(Key, BufferedReads0) of
                        {ok, Orddict0} ->
                            BufferedReads = case handle_pending_reads(Orddict0, CommitTime, Key, Clock) of
                                                [] ->
                                                    dict:erase(Key, BufferedReads0);
                                                Orddict ->
                                                    dict:store(Key, Orddict, BufferedReads0)
                                            end,
                            State0#state{pending=Pending, min_pendings=MinPendings, buffered_reads=BufferedReads};
                        error ->
                            State0#state{pending=Pending, min_pendings=MinPendings}
                    end
            end
    end.

post_commit_dependencies(Key, TimeStamp, S0=#state{deps_keys=DepsKeys0, fsm_deps=FsmDeps, partition=_Partition}) ->
    case dict:find(Key, DepsKeys0) of
        {ok, List0} ->
            {List1, FsmDeps1} = lists:foldl(fun({TS2, Fsm}, {Acc, Dict0}) ->
                                                case eiger_ts_lt(TimeStamp, TS2) of
                                                    true ->
                                                        {Acc ++ [{TS2, Fsm}], Dict0};
                                                    false ->
                                                        case dict:fetch(Fsm, Dict0) of
                                                            1 ->
                                                                riak_core_vnode:reply(Fsm, deps_checked),
                                                                {Acc, dict:erase(Fsm, Dict0)};
                                                            Rest ->
                                                                {Acc, dict:store(Fsm, Rest - 1, Dict0)} 
                                                        end
                                                end
                                            end, {[], FsmDeps}, List0),
            S0#state{deps_keys=dict:store(Key, List1, DepsKeys0), fsm_deps=FsmDeps1};
        error ->
            S0
    end.

delete_pending_entry([], _TxId, List) ->
    {List, not_found};

delete_pending_entry([Element|Rest], TxId, List) ->
    case Element of
        {PrepareTime, TxId} ->
            {List ++ Rest, PrepareTime};
        _ ->
            delete_pending_entry(Rest, TxId, List ++ [Element])
    end.

handle_pending_reads([], _CommitTime, _Key, _Clock) ->
    [];
handle_pending_reads([Element|Rest], CommitTime, Key, Clock) ->
    {Time, {Client, Type, TxId}} = Element,
    case Time < CommitTime of
        true ->
            Reply = do_read(Key, Type, TxId, Time, #state{clock=Clock}),
            riak_core_vnode:reply(Client, Reply),
            handle_pending_reads(Rest, CommitTime, Key, Clock);
        false ->
            [Element|Rest]
    end.

do_read(Key, Type, TxId, Time, #state{clock=Clock}) -> 
    %lager:info("Do read ~w, ~w, ~w", [Key, Type, Time]),
    case eiger_materializer_vnode:read(Key, Type, Time, TxId) of
    %case eiger_materializer_vnode:read(Key, Time) of
        {ok, Snapshot, EVT, Timestamp} ->
            Value = Type:value(Snapshot),
            %lager:info("Snapshot is ~w, EVT is ~w", [Snapshot, EVT]),
            case Time of
                latest -> 
                    {Key, Value, EVT, Clock, Timestamp};
                _ -> 
                    {Key, Value, Timestamp}
            end;
        {error, Reason} ->
            {error, Reason}
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

eiger_ts_lt(TS1, TS2) ->
   %lager:info("ts1: ~p, ts2: ~p",[TS1, TS2]),
    {DC1, C1} = TS1,
    {DC2, C2} = TS2,
    case (C1 < C2) of
        true -> true;
        false ->
            case (C1 == C2) of
                true ->
                    DC1<DC2;
                false -> false
            end
    end.
