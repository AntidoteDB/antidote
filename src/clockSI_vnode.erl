-module(clockSI_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
	 %API begin
	 read_data_item/4,
	 update_data_item/4,
	 %clean_and_notify/3,
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
    io:format("ClockSI-Vnode: read key ~w for TxId ~w ~n",[Key, TxId]),
    riak_core_vnode_master:sync_command(Node, {read_data_item, TxId, Key, Type}, ?CLOCKSIMASTER).

%% @doc Sends an update request to the Node that is responsible for the Key
update_data_item(Node, TxId, Key, Op) ->
    io:format("ClockSI-Vnode: update key ~w for TxId ~w ~n",[Key, TxId]),
    riak_core_vnode_master:sync_command(Node, {update_data_item, TxId, Key, Op}, ?CLOCKSIMASTER).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId) ->
    io:format("ClockSI-Vnode: prepare TxId ~w ~n",[TxId]),
    riak_core_vnode_master:command(ListofNodes, {prepare, TxId}, {fsm, undefined, self()},?CLOCKSIMASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId, CommitTime) ->
    io:format("ClockSI-Vnode: commit TxId ~w ~n",[TxId]),
    riak_core_vnode_master:command(ListofNodes, {commit, TxId, CommitTime}, {fsm, undefined, self()},?CLOCKSIMASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    io:format("ClockSI-Vnode: abort TxId ~w ~n",[TxId]),
    riak_core_vnode_master:command(ListofNodes, {abort, TxId}, {fsm, undefined, self()},?CLOCKSIMASTER).

%% @doc sends a request to Node to get all the transactions that are pending for a given Key
%% This function is called by the ClockSI read FSM
get_pending_txs(Node, {Key, TxId}) ->
	io:format("ClockSI-Vnode: get pending txs for Key ~w, TxId ~w ~n",[Key,TxId]),
	riak_core_vnode_master:sync_command (Node, {get_pending_txs, {Key, TxId}}, ?CLOCKSIMASTER).


%% @doc Initializes all the data structures that the vnode needs to track information of
%% the transactions it participates on.
init([Partition]) ->
	io:format("ClockSI-Vnode: initialize partition ~w ~n",[Partition]),
    % It generates a dets file to store active transactions.
    TxLogFile = string:concat(integer_to_list(Partition), "clockSI_tx"),
    TxLogPath = filename:join(
            app_helper:get_env(riak_core, platform_data_dir), TxLogFile),
    case dets:open_file(TxLogFile, [{file, TxLogPath}, {type, bag}]) of
        {ok, TxLog} ->
	    	ActiveTx=ets:new(active_tx, [set]),
	    	PreparedTx=ets:new(prepared_tx, [set]),
	    	CommittedTx=ets:new(committed_tx, [set]),
	    	WaitingFsms=ets:new(waiting_fsms, [bag]),
	    	ActiveTxsPerKey=ets:new(active_txs_per_key, [bag]),
	    	WriteSet=ets:new(write_set, [bag]),
	    	io:format("ClockSI-Vnode: Initialized state, data structures and Log ~n"),
            {ok, #state{partition=Partition, log=TxLog, active_tx=ActiveTx, prepared_tx=PreparedTx, committed_tx=CommittedTx, 
						write_set=WriteSet, waiting_fsms=WaitingFsms, active_txs_per_key=ActiveTxsPerKey}};
        {error, Reason} ->
        	io:format("ClockSI-Vnode: could not initialize state, reason: ~w ~n",[Reason]),
            {error, Reason}
    end.


%% @doc starts a read_fsm to handle a read operation.
handle_command({read_data_item, TxId, Key, Type}, Sender, #state{partition= Partition, log=_Log}=State) ->
    Vnode={Partition, node()},
    io:format("ClockSI-Vnode: start a read fsm for key ~w ~n",[Key]),
    clockSI_readitem_fsm:start_link(Vnode, Sender, TxId, Key, Type),
    io:format("ClockSI-Vnode: done. Reply to the coordinator."),
    {noreply, State};

%% @doc handles an update operation at a Leader's partition
handle_command({update_data_item, TxId, Key, Op}, _Sender, #state{active_tx=ActiveTx, write_set=WriteSet, active_txs_per_key=ActiveTxsPerKey, log=Log}=State) ->
	%%do we need the Sender here?
    
	%%clockSI_updateitem_fsm:start_link(Sender, Tx, Key, Op),
	Result = dets:insert(Log, {TxId, {Key, Op}}),
    case Result of
    	ok ->
    		ets:insert(ActiveTx, {TxId, TxId#tx_id.snapshot_time}),
			ets:insert(ActiveTxsPerKey, {Key, TxId}),
			ets:insert(WriteSet, {TxId, {Key, Op}}),
        	{reply, {ok, {Key, Op}}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;


handle_command({prepare, TxId}, _Sender, #state{committed_tx=CommittedTx, log=Log, active_txs_per_key=ActiveTxPerKey, prepared_tx=PreparedTx, write_set=WriteSet}=State) ->
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
	io:format("ClockSI_Vnode: certification_check failed~n"),
	%This does not match Ale's coordinator. We have to discuss it. It also expect Node.
	{reply, abort, State}
    end;

handle_command({commit, TxId, TxCommitTime}, _Sender, #state{log=Log, committed_tx=CommittedTx}=State) ->
    Result = dets:insert(Log, {TxId, {committed, TxCommitTime}}),
    case Result of
    ok ->
        ets:insert(CommittedTx, {TxId, TxCommitTime}),
        {reply, committed, State};
    {error, Reason} ->
        {reply, {error, Reason}, State}
    end;

handle_command({abort, _TxId}, _Sender, #state{log=_Log}=State) ->
    %clean_and_notify(timeout, TxId, State),
    io:format("Vnode: Recieved abort~n"),
    {reply, ack_abort, State};

handle_command ({get_pending_txs, {Key, TxId}}, Sender, #state{
			active_txs_per_key=ActiveTxsPerKey, waiting_fsms=WaitingFsms, prepared_tx=PreparedTx}=State) ->
	io:format("ClockSI-Vnode: retrieving pending Txs for key ~w ~n",[Key]),
	Pending=pending_txs(ets:lookup(ActiveTxsPerKey, Key), TxId, PreparedTx),
	case Pending of
		[]->
			io:format("ClockSI-Vnode: no pending txs. ~n"),
			{reply, {ok, empty}, State};
		[H|T]->
			io:format("ClockSI-Vnode: There are pending Txs for key ~w, notifying the read_FSM. ~w ~n",[Key, Sender]),
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
%clean_and_notify(timeout, TxId, #state{active_tx=ActiveTx, prepared_tx=PreparedTx, 
%					write_set=WriteSet, waiting_fsms=WaitingFsms, active_txs_per_key=_ActiveTxsPerKey}) ->
%	notify_all(ets:lookup(WaitingFsms, TxId), TxId),
	%case ets:lookup(WriteSet, TxId) of
        %[Op|Ops] ->
		%clean_active_txs_per_key(TxId, [Op|Ops], ActiveTxsPerKey)
    	%end,
%	ets:delete(PreparedTx, TxId),
%	ets:delete(ActiveTx, TxId),
%	ets:delete(WriteSet, TxId),
%	ets:delete(WaitingFsms, TxId).

%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into a number expressed in microseconds
now_milisec({MegaSecs,Secs,MicroSecs}) ->
        (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

%% add_list_to_pendings([], _Sender, _Pendings) -> done;
%% 
%% add_list_to_pendings([Next|Rest], Sender, Pendings) ->
%%     ets:insert(Pendings, {Next, Sender}),
%%     add_list_to_pendings(Rest, Sender, Pendings).

%notify_all([], _) -> done; 
%notify_all([Next|Rest], TxId) -> 
%    riak_core_vnode:reply(Next, {committed, TxId}),
%    notify_all(Rest, TxId).

%% @doc returns a list of transactions with
%% prepare timestamp bigger than TxId (snapshot time)
%% input:
%% 	ListOfPendingTxs: a List of all transactions in prepare state that update Key
%% 	Key: the key of the object for which there are prepared transactions that updated it.
pending_txs(ListOfPendingTxs, TxId, PreparedTx) -> 
	internal_pending_txs(ListOfPendingTxs, TxId, PreparedTx, []).
internal_pending_txs([], _ST, _PreparedTx, Result) -> Result;
internal_pending_txs([H|T], ST, PreparedTx, Result) ->
	case ets:lookup(PreparedTx, H) of 
	[PrepareTime] ->
		case PrepareTime < ST of
			true -> 
				internal_pending_txs(T, ST,PreparedTx, lists:append(Result,[H]));
			false-> 
				internal_pending_txs(T, ST, PreparedTx, Result)
		end;
	[] ->
		internal_pending_txs(T, ST, PreparedTx, Result)
	end.


%clean_active_txs_per_key(_, [], _) -> ok;
%clean_active_txs_per_key(TxId, [Op|Ops], ActiveTxsPerKey) ->
%	{Key, _}=Op,
%	dets:delete_object(ActiveTxsPerKey, {Key, TxId}),
%	clean_active_txs_per_key(TxId, Ops, ActiveTxsPerKey).

add_to_waiting_fsms(_, [], _) -> ok;    
add_to_waiting_fsms(WaitingFsms, [H|T], Sender) ->   
	ets:insert(WaitingFsms, {H, Sender}),
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
    io:format("Active Tx to check ~w~n",[H]),
    case H > TxId of 
    true ->
        case ets:lookup(CommittedTx, H) of 
	true ->
	    true;
	false->
	    check_keylog(TxId, T, CommittedTx)
	end
    end.
