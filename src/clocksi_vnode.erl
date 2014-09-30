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
-module(clocksi_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
         read_data_item/4,
         update_data_item/6,
         prepare/2,
         commit/3,
         abort/2,
         now_milisec/1,
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

%%---------------------------------------------------------------------
%% @doc Data Type: state
%%      where:
%%          partition: the partition that the vnode is responsible for.
%%          prepared_tx: a list of prepared transactions.
%%          committed_tx: a list of committed transactions.
%%          active_txs_per_key: a list of the active transactions that
%%              have updated a key (but not yet finished).
%%          downstream_set: a list of the downstream operations that the
%%              transactions generate.
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition,
                prepared_tx,
                committed_tx,
                active_txs_per_key,
                downstream_set,
                write_set}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, TxId, Key, Type) ->
    try
        riak_core_vnode_master:sync_command(Node,
                                            {read_data_item, TxId, Key, Type},
                                            ?CLOCKSI_MASTER,
                                            infinity)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Sends an update request to the Node that is responsible for the Key
update_data_item(Node, TxId, Key, Type, Op, DownstreamOp) ->
    lager:info("Update issued for key: ~p txid: ~p", [Key, TxId]),
    try
        riak_core_vnode_master:sync_command(Node,
                                            {update_data_item, TxId, Key, Type, Op, DownstreamOp},
                                            ?CLOCKSI_MASTER,
                                            infinity)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId) ->
    riak_core_vnode_master:command(ListofNodes,
                                   {prepare, TxId},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId, CommitTime) ->
    riak_core_vnode_master:command(ListofNodes,
                                   {commit, TxId, CommitTime},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    riak_core_vnode_master:command(ListofNodes,
                                   {abort, TxId},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    PreparedTx = ets:new(list_to_atom(atom_to_list(prepared_tx) ++
                                      integer_to_list(Partition)),
                         [set, {write_concurrency, true}]),
    CommittedTx = ets:new(list_to_atom(atom_to_list(committed_tx) ++
                                       integer_to_list(Partition)),
                          [set, {write_concurrency, true}]),
    ActiveTxsPerKey = ets:new(list_to_atom(atom_to_list(active_txs_per_key)
                                           ++ integer_to_list(Partition)),
                              [bag, {write_concurrency, true}]),
    WriteSet = ets:new(list_to_atom(atom_to_list(write_set) ++
                                    integer_to_list(Partition)),
                       [duplicate_bag, {write_concurrency, true}]),
    DownstreamSet = ets:new(list_to_atom(atom_to_list(downstream_set) ++
                                    integer_to_list(Partition)),
                       [duplicate_bag, {write_concurrency, true}]),
    {ok, #state{partition=Partition,
                prepared_tx=PreparedTx,
                committed_tx=CommittedTx,
                write_set=WriteSet,
                downstream_set=DownstreamSet,
                active_txs_per_key=ActiveTxsPerKey}}.

%% @doc starts a read_fsm to handle a read operation.
handle_command({read_data_item, Txn, Key, Type}, Sender,
               #state{write_set=WriteSet, partition=Partition}=State) ->
    Vnode = {Partition, node()},
    Updates = ets:lookup(WriteSet, Txn#transaction.txn_id),
    {ok, _Pid} = clocksi_readitem_fsm:start_link(Vnode, Sender, Txn,
                                                 Key, Type, Updates),
    {noreply, State};

%% @doc handles an update operation at a Leader's partition
handle_command({update_data_item, Txn, Key, Type, Op, DownstreamOp}, Sender,
               #state{partition=Partition,
                      write_set=WriteSet,
                      downstream_set=DownstreamSet,
                      active_txs_per_key=ActiveTxsPerKey}=State) ->
    TxId = Txn#transaction.txn_id,
    LogId = log_utilities:get_logid_from_key(Key),
    [Node] = log_utilities:get_preflist_from_key(Key),
    {ok, _} = logging_vnode:append(Node, LogId, DownstreamOp),
    true = ets:insert(ActiveTxsPerKey, {Key, Type, TxId}),
    true = ets:insert(WriteSet, {TxId, {Key, Type, Op}}),
    true = ets:insert(DownstreamSet, {TxId, DownstreamOp}),
    {ok, _Pid} = clocksi_updateitem_fsm:start_link(
                    Sender,
                    Txn#transaction.vec_snapshot_time,
                    Partition),
    {noreply, State};

handle_command({prepare, Transaction}, _Sender,
               State = #state{partition=_Partition,
                              committed_tx=CommittedTx,
                              active_txs_per_key=ActiveTxPerKey,
                              prepared_tx=PreparedTx,
                              write_set=WriteSet}) ->
    TxId = Transaction#transaction.txn_id,
    TxWriteSet = ets:lookup(WriteSet, TxId),
    case certification_check(TxId, TxWriteSet, CommittedTx, ActiveTxPerKey) of
        true ->
            PrepareTime = now_milisec(erlang:now()),
            LogRecord = #log_record{tx_id=TxId,
                                    op_type=prepare,
                                    op_payload=PrepareTime},
            true = ets:insert(PreparedTx, {active, {TxId, PrepareTime}}),
            Updates = ets:lookup(WriteSet, TxId),
            [{_, {Key, _Type, {_Op, _Actor}}} | _Rest] = Updates,
            LogId = log_utilities:get_logid_from_key(Key),
            [Node] = log_utilities:get_preflist_from_key(Key),
            Result = logging_vnode:append(Node,LogId,LogRecord),
            case Result of
                {ok, _} ->
                    {reply, {prepared, PrepareTime}, State};
                {error, timeout} ->
                    {reply, {error, timeout}, State}
            end;
        false ->
            {reply, abort, State}
    end;

%% TODO: sending empty writeset to clocksi_downstream_generatro
%% Just a workaround, need to delete downstream_generator_vnode
%% eventually.
handle_command({commit, Transaction, TxCommitTime}, _Sender,
               #state{partition=_Partition,
                      committed_tx=CommittedTx,
                      downstream_set=DownstreamSet} = State) ->
    TxId = Transaction#transaction.txn_id,
    LogRecord=#log_record{tx_id=TxId,
                          op_type=commit,
                          op_payload={TxCommitTime,
                                      Transaction#transaction.vec_snapshot_time}},
    DownstreamOps = ets:lookup(DownstreamSet, TxId),
    [{_, DownstreamOp} | _Rest] = DownstreamOps,
    Key = DownstreamOp#clocksi_payload.key,
    LogId = log_utilities:get_logid_from_key(Key),
    [Node] = log_utilities:get_preflist_from_key(Key),
    case logging_vnode:append(Node,LogId,LogRecord) of
        {ok, _} ->
            true = ets:insert(CommittedTx, {TxId, TxCommitTime}),
            case update_materializer(DownstreamOps, TxCommitTime) of 
                ok ->
                    clocksi_downstream_generator_vnode:trigger(
                            Key, {TxId,
                                [],
                                Transaction#transaction.vec_snapshot_time,
                                TxCommitTime}),
                    clean_and_notify(TxId, Key, State),
                    {reply, committed, State};
                error ->
                    {reply, {error, materializer_failure}, State}
            end;
        {error, timeout} ->
            {reply, {error, timeout}, State}
    end;

handle_command({abort, Transaction}, _Sender,
               #state{partition=_Partition, write_set=WriteSet} = State) ->
    TxId = Transaction#transaction.txn_id,
    Updates = ets:lookup(WriteSet, TxId),
    [{_, {Key, _Type, {_Op, _Actor}}} | _Rest] = Updates,
    LogId = log_utilities:get_logid_from_key(Key),
    [Node] = log_utilities:get_preflist_from_key(Key),
    Result = logging_vnode:append(Node,LogId,{TxId, aborted}),
    case Result of
        {ok, _} ->
            clean_and_notify(TxId, Key, State);
        {error, timeout} ->
            clean_and_notify(TxId, Key, State)
    end,
    {reply, ack_abort, State};

%% @doc Return active transactions in prepare state with their preparetime
handle_command({get_active_txns}, _Sender,
               #state{prepared_tx=Prepared, partition=_Partition} = State) ->
    ActiveTxs = ets:lookup(Prepared, active),
    {reply, {ok, ActiveTxs}, State};

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

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc clean_and_notify:
%%      This function is used for cleanning the state a transaction
%%      stores in the vnode while it is being procesed. Once a
%%      transaction commits or aborts, it is necessary to:
%%      1. notify all read_fsms that are waiting for this transaction to finish
%%      2. clean the state of the transaction. Namely:
%%      a. ActiteTxsPerKey,
%%      b. PreparedTx
%%
clean_and_notify(TxId, _Key, #state{active_txs_per_key=_ActiveTxsPerKey,
                                   prepared_tx=PreparedTx,
                                   write_set=WriteSet}) ->
    true = ets:match_delete(PreparedTx, {active, {TxId, '_'}}),
    true = ets:delete(WriteSet, TxId).

%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_milisec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
certification_check(_, [], _, _) ->
    true;
certification_check(TxId, [H|T], CommittedTx, ActiveTxPerKey) ->
    {_, {Key, _Type, _}} = H,
    TxsPerKey = ets:lookup(ActiveTxPerKey, Key),
    case check_keylog(TxId, TxsPerKey, CommittedTx) of
        true ->
            false;
        false ->
            certification_check(TxId, T, CommittedTx, ActiveTxPerKey)
    end.

check_keylog(_, [], _) ->
    false;
check_keylog(TxId, [H|T], CommittedTx)->
    {_Key, _Type, ThisTxId}=H,
    case ThisTxId > TxId of
        true ->
            CommitInfo = ets:lookup(CommittedTx, ThisTxId),
            case CommitInfo of
                [{_, _CommitTime}] ->
                    true;
                [] ->
                    check_keylog(TxId, T, CommittedTx)
            end;
        false ->
            check_keylog(TxId, T, CommittedTx)
    end.

update_materializer(DownstreamOps, TxCommitTime) ->
    DcId = dc_utilities:get_my_dc_id(),
    lager:info("DS:~w",[DownstreamOps]),
    UpdateFunction = fun ({_, DownstreamOp}, AccIn) -> 
                            CommittedDownstreamOp = DownstreamOp#clocksi_payload{ 
                                                commit_time = {DcId, TxCommitTime}},
                            Key = DownstreamOp#clocksi_payload.key,
                            AccIn++[materializer_vnode:update_cache(Key, CommittedDownstreamOp)]
                    end,
    Results = lists:foldl(UpdateFunction, [], DownstreamOps),
    Failures = lists:filter(fun(Elem) -> Elem /= ok end, Results),
    case length(Failures) of
        0 ->
            ok;
        _ ->
            error
    end. 
