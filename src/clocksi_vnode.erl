-module(clocksi_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
         read_data_item/4,
         update_data_item/5,
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
%%          waiting_fsms: a list of the read_fsms that are currently
%%              waiting for each tx to finish.
%%          active_txs_per_key: a list of the active transactions that
%%              have updated a key (but not yet finished).
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition,
                prepared_tx,
                committed_tx,
                waiting_fsms,
                active_txs_per_key,
                write_set}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, TxId, Key, Type) ->
    lager:info("ClockSI-Vnode: read key ~w for TxId ~w ~n",[Key, TxId]),
    try
        riak_core_vnode_master:sync_command(Node,
                                            {read_data_item, TxId, Key, Type},
                                            ?CLOCKSI_MASTER)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Sends an update request to the Node that is responsible for the Key
update_data_item(Node, TxId, Key, Type, Op) ->
    lager:info("ClockSI-Vnode: update key ~w for TxId ~w ~n",[Key, TxId]),
    try
        riak_core_vnode_master:sync_command(Node,
                                            {update_data_item, TxId, Key, Type, Op},
                                            ?CLOCKSI_MASTER)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, Txn) ->
    lager:info("ClockSI-Vnode: prepare TxId ~w ~n",[Txn]),
    riak_core_vnode_master:command(ListofNodes,
                                   {prepare, Txn},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId, CommitTime) ->
    lager:info("ClockSI-Vnode: commit TxId ~w ~n",[TxId]),
    riak_core_vnode_master:command(ListofNodes,
                                   {commit, TxId, CommitTime},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    lager:info("ClockSI-Vnode: abort TxId ~w ~n",[TxId]),
    riak_core_vnode_master:command(ListofNodes,
                                   {abort, TxId},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER),
    lager:info("ClockSI-Vnode: sent command to abort TxId ~w ~n",[TxId]).

%% @doc Initializes all data structures that vnode needs to track information
%% the transactions it participates on.
init([Partition]) ->
    PreparedTx=ets:new(prepared_tx, [set]),
    CommittedTx=ets:new(committed_tx, [set]),
    ActiveTxsPerKey=ets:new(active_txs_per_key, [bag]),
    WriteSet=ets:new(write_set, [duplicate_bag]),
    {ok, #state{partition=Partition,
                prepared_tx=PreparedTx,
                committed_tx=CommittedTx,
                write_set=WriteSet,
                active_txs_per_key=ActiveTxsPerKey}}.

%% @doc starts a read_fsm to handle a read operation.
handle_command({read_data_item, Txn, Key, Type}, Sender,
               #state{write_set=WriteSet, partition=Partition}=State) ->
    Vnode = {Partition, node()},
    Updates = ets:lookup(WriteSet, Txn#transaction.txn_id),
    lager:info
      ("ClockSI-Vnode: start a read fsm for key ~w. Previous updates:~p",
       [Key, Updates]),
    {ok, _Pid} = clocksi_readitem_fsm:start_link
                   (Vnode, Sender, Txn, Key, Type, Updates),
    lager:info("ClockSI-Vnode: done. Reply to the coordinator."),
    {noreply, State};

%% @doc handles an update operation at a Leader's partition
handle_command({update_data_item, Txn, Key, Type, Op}, Sender,
               #state{partition = Partition, write_set=WriteSet,
                      active_txs_per_key=ActiveTxsPerKey}=State) ->
    TxId = Txn#transaction.txn_id,
    LogRecord=#log_record{tx_id=TxId, op_type=update, op_payload={Key, Type, Op}},
    TxId = Txn#transaction.txn_id,
    lager:info("ClockSI_Vnode: logging the following operation: ~p.",
               [LogRecord]),
                                                %LogId=log_utilities:get_logid_from_key(Key),
    Result = floppy_rep_vnode:append(Key, Type, LogRecord),
    case Result of
        {ok,_} ->
            ets:insert(ActiveTxsPerKey, {Key, Type, TxId}),
            Check2=ets:lookup(ActiveTxsPerKey, Key),
            lager:info("ClockSI-Vnode: Inserted to ActiveTxsPerKey ~p",
                       [Check2]),
            ets:insert(WriteSet, {TxId, {Key, Type, Op}}),
            Check3=ets:lookup(WriteSet, TxId),
            lager:info("ClockSI-Vnode: Inserted to WriteSet ~p",[Check3]),
            {ok, _Pid} = clocksi_updateitem_fsm:start_link(
                           Sender, Txn#transaction.vec_snapshot_time,
                           Partition),
            {noreply, State};
        {error, timeout} ->
            {reply, {error, timeout}, State}
    end;

handle_command({prepare, Transaction}, _Sender,
               State = #state{ partition = _Partition,
                               committed_tx=CommittedTx,
                               active_txs_per_key=ActiveTxPerKey,
                               prepared_tx=PreparedTx, write_set=WriteSet}) ->
    TxId = Transaction#transaction.txn_id,
    lager:info("ClockSI_Vnode: got prepare message."),
    TxWriteSet=ets:lookup(WriteSet, TxId),
    lager:info("ClockSI_Vnode: starting certification check."),
    case certification_check(TxId, TxWriteSet, CommittedTx, ActiveTxPerKey) of
        true ->
            lager:info("ClockSI_Vnode: certification check passed."),
            PrepareTime=now_milisec(erlang:now()),
            LogRecord=#log_record{
                         tx_id=TxId, op_type=prepare, op_payload=PrepareTime},
            lager:info(
              "ClockSI_Vnode: logging the following operation: ~p.",
              [LogRecord]),
            ets:insert(PreparedTx, {active,{TxId, PrepareTime}}),
            Updates = ets:lookup(WriteSet, TxId),
            [{_,{Key,Type,{_Op,_Actor}}} | _Rest] = Updates,
            Result = floppy_rep_vnode:append(Key,Type,LogRecord),
            case Result of
                {ok,_} ->
                    {reply, {prepared, PrepareTime}, State};
                {error, timeout} ->
                    {reply, {error, timeout}, State}
            end;
        false ->
            lager:info(
              "ClockSI_Vnode: certification_check failed,Aborting."),
            {reply, abort, State}
    end;

handle_command({commit, Transaction, TxCommitTime}, _Sender,
               #state{partition = _Partition, committed_tx=CommittedTx,
                      write_set=WriteSet}=State) ->
    lager:info("ClockSI_Vnode: got commit message."),
    TxId = Transaction#transaction.txn_id,
    LogRecord=#log_record{
                 tx_id=TxId, op_type=commit,
                 op_payload={TxCommitTime,
                             Transaction#transaction.vec_snapshot_time}},
    lager:info(
      "ClockSI_Vnode: logging the following operation: ~p.",
      [LogRecord]),
    Updates = ets:lookup(WriteSet, TxId),
    [{_,{Key,Type,{_Op,_Actor}}} | _Rest] = Updates,
    Result = floppy_rep_vnode:append(Key,Type,LogRecord),
    case Result of
        {ok,_} ->
            %% gen_fsm:send_event(Sender, committed)
            %%TODO: Return results to caller here,
            ets:insert(CommittedTx, {TxId, TxCommitTime}),
            lager:info(
              "ClockSI_Vnode: sending updates to downstream vnode layer: ~p",
              [Updates]),
            _Return = clocksi_downstream_generator_vnode:trigger(
                        Key, {TxId, Updates,
                              Transaction#transaction.vec_snapshot_time,
                              TxCommitTime}),
            lager:info(
              "ClockSI_Vnode: clean state and Notifying pending read_FSMs"),
            clean_and_notify(TxId, State),
            lager:info("ClockSI_Vnode: done"),
            {reply, committed, State};
        {error, timeout} ->
            {reply, {error, timeout}, State}
    end;

handle_command({abort, Transaction}, _Sender, State =
                   #state{partition=_Partition, write_set = WriteSet}) ->
    %LogId=log_utilities:get_logid_from_partition(Partition),
    %Result = floppy_rep_vnode:append(LogId, {TxId, aborted}),
    TxId = Transaction#transaction.txn_id,
    Updates = ets:lookup(WriteSet, TxId),
    [{_,{Key,Type,{_Op,_Actor}}} | _Rest] = Updates,
    Result = floppy_rep_vnode:append(Key,Type,{TxId, aborted}),
    case Result of
        {ok,_} ->
            clean_and_notify(TxId, State),
            lager:info("Vnode: Recieved abort. State cleaned.");
        {error, timeout} ->
            clean_and_notify(TxId, State),
            lager:info("Abort not written to all replica of log")
    end,
    {reply, ack_abort, State};

%% @doc Return active transactions in prepare state with their preparetime
handle_command({get_active_txns}, _Sender,
               #state{prepared_tx=Prepared, partition=Partition}=State) ->
    ActiveTxs = ets:lookup(Prepared, active),
    lager:info("ActiveTxs: ~p -> ~p", [Partition, ActiveTxs]),
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
%%      b. Waiting_Fsms,
%%      c. PreparedTx
%%
clean_and_notify(TxId, Key, #state{active_txs_per_key=ActiveTxsPerKey,
                                   prepared_tx=PreparedTx,
                                   write_set=WriteSet}) ->
    true = ets:match_delete(PreparedTx, {active, {TxId, '_'}}),
    true = ets:delete(WriteSet, TxId),
    true = ets:delete(ActiveTxsPerKey, Key).

%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_milisec({MegaSecs,Secs,MicroSecs}) ->
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

%% @doc performs a certification check when a transaction wants to move
%% to the prepared state.
certification_check(_, [], _, _) -> true;
certification_check(TxId, [H|T], CommittedTx, ActiveTxPerKey) ->
    lager:info("ClockSI_Vnode: getting key of operation: ~p.", [H]),
    {_,{Key,_Type,_}}=H,
    lager:info("ClockSI_Vnode: checking key: ~p.", [Key]),
    TxsPerKey=ets:lookup(ActiveTxPerKey, Key),
    lager:info("ClockSI_Vnode: active txs for Key: ~p: ~p.", [Key, TxsPerKey]),
    case check_keylog(TxId, TxsPerKey, CommittedTx) of
        true ->
            false;
        false ->
            certification_check(TxId, T, CommittedTx, ActiveTxPerKey)
    end.

check_keylog(_, [], _) -> false;
check_keylog(TxId, [H|T], CommittedTx)->
    {_Key, _Type, ThisTxId}=H,
    lager:info("Checking if Tx ~p started after Tx to check ~w~n",
               [ThisTxId, TxId]),
    case ThisTxId > TxId of
        true ->
            lager:info("It did! Checking if it has committed..."),
            CommitInfo=ets:lookup(CommittedTx, ThisTxId),
            lager:info("got this from the lookup ~p", [CommitInfo]),
            timer:sleep(1000),
            case CommitInfo of
                [{_,CommitTime}] ->
                    lager:info(
                      "Transaction has already committed with commit time: ~p",
                      [CommitTime]),
                    true;
                []->
                    lager:info("It did not... Continuing..."),
                    check_keylog(TxId, T, CommittedTx)
            end;
        false ->
            lager:info("It did not... Continuing..."),
            check_keylog(TxId, T, CommittedTx)
    end.
