-module(clockSI_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
         %%API begin
         read_data_item/4,
         update_data_item/4,
         %%clean_and_notify/3,
         get_pending_txs/2,
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

-ignore_xref([
              start_vnode/1
             ]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%	partition: the partition that the vnode is responsible for.
%%	log: the transaction log (stored on disc by using dets)
%%	active_tx: a list of active transactions.
%%	prepared_tx: a list of prepared transactions.		
%%	committed_tx: a list of committed transactions.	
%%	waiting_fsms: a list of the read_fsms that are currently waiting for each tx to finish.
%%	active_txs_per_key: a list of the active transactions that have updated a key (but not yet finished).
%%	write_set: a list of the write sets that the transactions generate.	
%%----------------------------------------------------------------------
-record(state, {partition,log, active_tx, prepared_tx, committed_tx, waiting_fsms, active_txs_per_key, write_set}).


%%%===================================================================
%%% API
%%%=================================================================== 

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, TxId, Key, Type) ->
    lager:info("ClockSI-Vnode: read key ~w for TxId ~w ~n",[Key, TxId]),
    riak_core_vnode_master:sync_command(Node, {read_data_item, TxId, Key, Type}, ?CLOCKSIMASTER).

%% @doc Sends an update request to the Node that is responsible for the Key
update_data_item(Node, TxId, Key, Op) ->
    lager:info("ClockSI-Vnode: update key ~w for TxId ~w ~n",[Key, TxId]),
    riak_core_vnode_master:sync_command(Node, {update_data_item, TxId, Key, Op}, ?CLOCKSIMASTER).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, Txn) ->
    lager:info("ClockSI-Vnode: prepare TxId ~w ~n",[Txn]),
    riak_core_vnode_master:command(ListofNodes, {prepare, Txn}, {fsm, undefined, self()},?CLOCKSIMASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId, CommitTime) ->
    lager:info("ClockSI-Vnode: commit TxId ~w ~n",[TxId]),
    riak_core_vnode_master:command(ListofNodes, {commit, TxId, CommitTime}, {fsm, undefined, self()},?CLOCKSIMASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    lager:info("ClockSI-Vnode: abort TxId ~w ~n",[TxId]),
    riak_core_vnode_master:command(ListofNodes, {abort, TxId}, {fsm, undefined, self()},?CLOCKSIMASTER).

%% @doc sends a request to Node to get all the transactions that are pending for a given Key
%% This function is called by the ClockSI read FSM
get_pending_txs(Node, {Key, TxId}) ->
    lager:info("ClockSI-Vnode: get pending txs for Key ~w, TxId ~w ~n",[Key,TxId]),
    riak_core_vnode_master:sync_command (Node, {get_pending_txs, {Key, TxId}}, ?CLOCKSIMASTER).


%% @doc Initializes all the data structures that the vnode needs to track information of
%% the transactions it participates on.
init([Partition]) ->
                                                %lager:info("ClockSI-Vnode: initialize partition ~w ~n",[Partition]),
                                                % It generates a dets file to store active transactions.
    TxLogFile = string:concat(integer_to_list(Partition), "_clockSI"),
    TxLogPath = filename:join(
                  app_helper:get_env(riak_core, platform_data_dir), TxLogFile),
    case dets:open_file(TxLogFile, [{file, TxLogPath}, {type, bag}]) of
        {ok, TxLog} ->
            ActiveTx=ets:new(active_tx, [set]),
            PreparedTx=ets:new(prepared_tx, [set]),
            CommittedTx=ets:new(committed_tx, [set]),
            WaitingFsms=ets:new(waiting_fsms, [bag]),
            ActiveTxsPerKey=ets:new(active_txs_per_key, [bag]),
            WriteSet=ets:new(write_set, [duplicate_bag]),
                                                %lager:info("ClockSI-Vnode: Initialized state, data structures and Log ~n"),
            {ok, #state{partition=Partition, log=TxLog, active_tx=ActiveTx, prepared_tx=PreparedTx, committed_tx=CommittedTx, 
                        write_set=WriteSet, waiting_fsms=WaitingFsms, active_txs_per_key=ActiveTxsPerKey}};
        {error, Reason} ->
            lager:error("ClockSI-Vnode: could not initialize state, reason: ~w ~n",[Reason]),
            {error, Reason}
    end.


%% @doc starts a read_fsm to handle a read operation.
handle_command({read_data_item, Txn, Key, Type}, Sender, #state{write_set=WriteSet, partition= Partition, log=_Log}=State) ->
    Vnode={Partition, node()},
    Updates = ets:lookup(WriteSet, Txn#transaction.txn_id),
    lager:info("ClockSI-Vnode: start a read fsm for key ~w. Previous updates:~p",[Key, Updates]),
    clockSI_readitem_fsm:start_link(Vnode, Sender, Txn, Key, Type, Updates),
    lager:info("ClockSI-Vnode: done."),
    {noreply, State};

%% @doc handles an update operation at a Leader's partition
handle_command({update_data_item, Txn, Key, Op}, Sender, #state{active_tx=ActiveTx, write_set=WriteSet, active_txs_per_key=ActiveTxsPerKey, log=Log}=State) ->
    %%do we need the Sender here?
    TxId = Txn#transaction.txn_id,
    Result = dets:insert(Log, {TxId, {Key, Op}}),
    case Result of
        ok ->
            ets:insert(ActiveTx, {TxId, Txn#transaction.snapshot_time}),
            Check1=ets:lookup(ActiveTx, TxId),
            lager:info("ClockSI-Vnode: Inserted to ActiveTx ~p",[Check1]),
            ets:insert(ActiveTxsPerKey, {Key, TxId}),
            Check2=ets:lookup(ActiveTxsPerKey, Key),
            lager:info("ClockSI-Vnode: Inserted to ActiveTxsPerKey ~p",[Check2]),
            ets:insert(WriteSet, {TxId, {Key, Op}}),
            Check3=ets:lookup(WriteSet, TxId),
            lager:info("ClockSI-Vnode: Inserted to WriteSet ~p",[Check3]),
            clockSI_updateitem_fsm:start_link(Sender, TxId),
            {noreply, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;


handle_command({prepare, Transaction}, _Sender, #state{committed_tx=CommittedTx, log=Log, active_txs_per_key=ActiveTxPerKey, prepared_tx=PreparedTx, write_set=WriteSet}=State) ->
    TxId = Transaction#transaction.txn_id,
    TxWriteSet=ets:lookup(WriteSet, TxId), 
    case certification_check(TxId, TxWriteSet, CommittedTx, ActiveTxPerKey) of
        true ->
            PrepareTime=now_milisec(erlang:now()),
            Result = dets:insert(Log, {TxId, {prepare, PrepareTime}}),
            case Result of
                ok ->
                    ets:insert(PreparedTx, {TxId, PrepareTime}),
                    {reply, {prepared, PrepareTime}, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        false ->
            lager:info("ClockSI_Vnode: certification_check failed"),
                                                %This does not match Ale's coordinator. We have to discuss it. It also expect Node.
            {reply, abort, State}
    end;

handle_command({commit, Transaction, TxCommitTime}, _Sender, #state{log=Log, committed_tx=CommittedTx, write_set=WriteSet}=State) ->
    lager:info("ClockSI_Vnode: got commit message."),
    TxId = Transaction#transaction.txn_id,
    Result = dets:insert(Log, {TxId, {committed, TxCommitTime}}),
    case Result of
        ok ->
            Updates = ets:lookup(WriteSet, TxId),
            lager:info("ClockSI_Vnode: issuing all updates to the logging layer: ~p", [Updates]),
            issue_updates(Updates, Transaction, TxCommitTime),
            ets:insert(CommittedTx, {TxId, TxCommitTime}),
            lager:info("ClockSI_Vnode: starting to clean state and Notifying pending read_FSMs (if any)."),
            clean_and_notify(TxId, State),
            lager:info("ClockSI_Vnode: done"),
            {reply, committed, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_command({abort, _TxId}, _Sender, #state{log=_Log}=State) ->
                                                %clean_and_notify(timeout, TxId, State),
    lager:info("Vnode: Recieved abort~n"),
    {reply, ack_abort, State};

handle_command ({get_pending_txs, {Key, TxId}}, Sender, #state{
                                                           active_txs_per_key=ActiveTxsPerKey, waiting_fsms=WaitingFsms, prepared_tx=PreparedTx}=State) ->
    lager:info("ClockSI-Vnode: retrieving active Txs for key ~w ~n",[Key]),
    ActiveTxs=ets:lookup(ActiveTxsPerKey, Key),
    lager:info("ClockSI-Vnode:  Active Txs for key ~w: ~w ",[Key, ActiveTxs]),
    Pending=pending_txs(ActiveTxs, TxId, PreparedTx),
    lager:info("ClockSI-Vnode:  Pending Txs for key ~w: ~w ",[Key, Pending]),
    case Pending of
        []->
            lager:info("ClockSI-Vnode: no pending txs. ~n"),
            {reply, {ok, empty}, State};
        [H|T]->
            lager:info("ClockSI-Vnode: There are pending Txs for key ~w, notifying the read_FSM. ~w ~n",[Key, Sender]),
            add_to_waiting_fsms(WaitingFsms, [H|T], Sender),    
            {reply, {ok, [H|T]}, State}
    end;

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command_logging, Message}),
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
%% This function is used for cleanning the state a transaction stores in the vnode
%% while it is being procesed. Once a transaction commits or aborts, it is necessary to:
%% 1. notify all read_fsms that are waiting for this transaction to finish to perform a read.
%% 2. clean the state of the transaction. Namely:
%% 	a. ActiteTxsPerKey,
%% 	b. Waiting_Fsms,
%% 	c. PreparedTx
clean_and_notify(TxId, #state{active_tx=ActiveTx, prepared_tx=PreparedTx, 
                              write_set=WriteSet, waiting_fsms=WaitingFsms, active_txs_per_key=ActiveTxsPerKey}) ->
    Pending=ets:lookup(WaitingFsms, TxId),
    lager:info("ClockSI_Vnode: fsms that are waiting for tx ~p to finish: ~p", [TxId, Pending]),
    notify_all(Pending, TxId),
    case ets:lookup(WriteSet, TxId) of
        [Op|Ops] ->
            lager:info("ClockSI_Vnode: cleanning tx state on the vnode."),
            clean_active_txs_per_key(TxId, [Op|Ops], ActiveTxsPerKey)
    end,
    ets:delete(PreparedTx, TxId),
    ets:delete(ActiveTx, TxId),
    ets:delete(WriteSet, TxId),
    ets:delete(WaitingFsms, TxId).

%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into a number expressed in microseconds
now_milisec({MegaSecs,Secs,MicroSecs}) ->
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

notify_all([], _) -> ok; 
notify_all([Next|Rest], TxId) -> 
    {_, {_,_,{FsmId,_}}} = Next,
    lager:info("ClockSI_Vnode: Notifying fsm: ~p", [FsmId]),
    gen_fsm:send_event(FsmId, {committed, TxId}),
    notify_all(Rest, TxId).

%% @doc returns a list of transactions with
%% prepare timestamp bigger than TxId (snapshot time)
%% input:
%% 	ListOfPendingTxs: a List of all transactions in prepare state that update Key
%% 	Key: the key of the object for which there are prepared transactions that updated it.
pending_txs(ListOfPendingTxs, TxId, PreparedTx) -> 
    SnapshotTime= TxId#tx_id.snapshot_time,
    internal_pending_txs(ListOfPendingTxs, SnapshotTime, PreparedTx, []).
internal_pending_txs([], _ST, _PreparedTx, Result) -> Result;
internal_pending_txs([H|T], ST, PreparedTx, Result) ->
    {_, TxId}=H,
    Lookup=ets:lookup(PreparedTx, TxId),
    case Lookup of 
        [{_, PrepareTime}] ->
            lager:info("Got prepare time for tx ~w of ~w, SnapshotTime of ~w", [TxId, PrepareTime, ST]),
            case PrepareTime < ST of
                true -> 
                    internal_pending_txs(T, ST,PreparedTx, lists:append(Result,[H]));
                false-> 
                    internal_pending_txs(T, ST, PreparedTx, Result)
            end;
        [] ->
            internal_pending_txs(T, ST, PreparedTx, Result)
    end.


clean_active_txs_per_key(_, [], _) -> ok;
clean_active_txs_per_key(TxId, [Op|Ops], ActiveTxsPerKey) ->
    {_, {Key, _}}=Op,
    lager:info("ClockSI-Vnode: trying to remove tx ~w for Key ~w.", [TxId, Key]),
    ets:delete_object(ActiveTxsPerKey, {Key, TxId}),
    clean_active_txs_per_key(TxId, Ops, ActiveTxsPerKey).

add_to_waiting_fsms(_, [], _) -> ok;    
add_to_waiting_fsms(WaitingFsms, [H|T], Sender) ->
    {_, ToWaitForId}= H,
    lager:info("ClockSI-Vnode: adding fsm ~w to wait for tx ~w.", [ToWaitForId]),  
    ets:insert(WaitingFsms, {ToWaitForId, Sender}),
    add_to_waiting_fsms(WaitingFsms, T, Sender).

%% @doc performs a certification check when a transaction wants to move
%% to the prepared state.	
certification_check(_, [], _, _) -> true;
certification_check(TxId, TxWriteSet, CommittedTx, ActiveTxPerKey) ->
    [{Key, _}|T]=TxWriteSet,
    TxsPerKey=ets:lookup(ActiveTxPerKey, Key),
    case check_keylog(TxId, TxsPerKey, CommittedTx) of
        true -> 
            false;	
        false ->
            certification_check(TxId, T, CommittedTx, ActiveTxPerKey)
    end.

check_keylog(_, [], _) -> false;
check_keylog(TxId, [H|T], CommittedTx)->
    lager:info("Active Tx to check ~w~n",[H]),
    case H > TxId of 
        true ->
            case ets:lookup(CommittedTx, H) of 
                true ->
                    true;
                false->
                    check_keylog(TxId, T, CommittedTx)
            end
    end.

issue_updates([], _Transaction,  _CommitTS)-> ok;

issue_updates([Next|Rest], Transaction, Commit_time)->
    {_,{Key,{Op,Actor}}}=Next,
    lager:info("About to append this: ~w~n",[{Key, {Op, Actor, Commit_time}}]),
    Vec_snapshot_time = Transaction#transaction.vec_snapshot_time,  
    Dc_id = 1, %TODO: Find local dc id
    OpPayload = #clocksi_payload{key = Key, type = riak_dt_pncounter, op_param = {Op, Actor}, actor = Actor, snapshot_time = Vec_snapshot_time, commit_time = {Dc_id, Commit_time}, txid = Transaction#transaction.txn_id},
    Update=#operation{payload = OpPayload},
    Downstream_op = clockSI_downstream:generate_downstream_op(Update),
    floppy_rep_vnode:append(Key, Downstream_op#operation.payload),
    issue_updates(Rest, Transaction, Commit_time).
