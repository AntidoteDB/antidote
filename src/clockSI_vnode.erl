-module(clockSI_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
	 %API begin
	 read_data_item/4,
	 update_data_item/4,
	 notify_commit/2,
	 clean_and_notify/3,
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

-record(state, {partition,log, active_tx, prepared_tx, committed_tx, waiting_fsms, active_txs_per_key, write_set}).


%%%===================================================================
%%% API
%%%=================================================================== 

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).
    
read_data_item(Node, Tx, Key, Type) ->
    riak_core_vnode_master:sync_command(Node, {read_data_item, Tx, Key, Type}, ?CLOCKSIMASTER).

update_data_item(Node, Tx, Key, Op) ->
    riak_core_vnode_master:sync_command(Node, {update_data_item, Tx, Key, Op}, ?CLOCKSIMASTER).

prepare(Node, TxId) ->
    riak_core_vnode_master:sync_command(Node, {prepare, TxId}, ?CLOCKSIMASTER).

commit(Node, TxId, CommitTime) ->
    riak_core_vnode_master:sync_command(Node, {commit, TxId, CommitTime}, ?CLOCKSIMASTER).

abort(Node, TxId) ->
    riak_core_vnode_master:sync_command(Node, {abort, TxId}, ?CLOCKSIMASTER).

notify_commit(Node, TxId) ->
   riak_core_vnode_master:sync_command(Node, {notify_commit, TxId}, ?CLOCKSIMASTER).

get_pending_txs(Node, {Key, TxId}) ->
	riak_core_vnode_master:sync_command (Node, {get_pending_txs, {Key, TxId}}, ?CLOCKSIMASTER).

init([Partition]) ->
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
            {ok, #state{partition=Partition, log=TxLog, active_tx=ActiveTx, prepared_tx=PreparedTx, committed_tx=CommittedTx, 
						write_set=WriteSet, waiting_fsms=WaitingFsms, active_txs_per_key=ActiveTxsPerKey}};
        {error, Reason} ->
            {error, Reason}
    end.

handle_command({read_data_item, TxId, Key, Type}, Sender, #state{partition= Partition, log=_Log}=State) ->
    Vnode={Partition, node()},
    clockSI_readitem_fsm:start_link(Vnode, Sender, TxId, Key, Type),
    {no_reply, State};

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
%%     {no_reply, State};

handle_command({prepare, TxId}, _Sender, #state{committed_tx=CommittedTx, log=Log,
	active_txs_per_key=ActiveTxPerKey, prepared_tx=PreparedTx, write_set=WriteSet}=State) ->
	
	TxWriteSet=ets:lookup(WriteSet, TxId), 
    case certification_check(TxId, TxWriteSet, CommittedTx, ActiveTxPerKey) of
    	true ->
			PrepareTime=now_milisec(erlang:now()),
			Result = dets:insert(Log, {TxId, {prepare, PrepareTime}}),
    		case Result of
        		ok ->
					ets:insert(PreparedTx, {TxId, PrepareTime}),
            		{reply, PrepareTime, State};
        		{error, Reason} ->
            		{reply, {error, Reason}, State}
    		end;
    	false ->
		%This does not match Ale's coordinator. We have to discuss it. It also expect Node.
			{reply, abort, State}
			
   		end;

handle_command({commit, TxId, TxCommitTime}, _Sender, #state{log=Log, committed_tx=CommittedTx}=State) ->
			%ale: Log T.writeset
			Result = dets:insert(Log, {TxId, {committed, TxCommitTime}}),
    		case Result of
        		ok ->
        			ets:insert(CommittedTx, {TxId, TxCommitTime}),
            		{reply, ack, State};
        		{error, Reason} ->
            		{reply, {error, Reason}, State}
    		end;
%% 			{next_state, clean_and_notify, TxId, State, 0};


handle_command({abort, TxId}, _Sender, #state{log=_Log}=State) ->
    clean_and_notify(timeout, TxId, State),
    {reply, ack_abort, State};

handle_command ({get_pending_txs, {Key, TxId}}, Sender, #state{
			active_txs_per_key=ActiveTxsPerKey, waiting_fsms=WaitingFsms, prepared_tx=PreparedTx}=State) ->
	Pending=pending_txs(ets:lookup(ActiveTxsPerKey, Key), TxId, PreparedTx),
	case Pending of
		[]->
			{reply, {ok, empty}, State};
		[H|T]->
			add_to_waiting_fsms(WaitingFsms, [H|T], Sender),    
			{reply, {ok, [H|T]}, State}
	end;
	
		
%% handle_command({notify_commit, TXs}, Sender, #state{pending=Pendings}=State) ->
%%     %TODO: Check if anyone is already commited 
%%     add_list_to_pendings(TXs, Sender, Pendings),
%%     {no_reply, State};

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
%%% States
%%%===================================================================


%% clean_and_notify: 
%% This function is used for cleanning the state a transaction stores in the vnode
%% while it is being procesed. Once a transaction commits or aborts, it is necessary to:
%% 1. notify all read_fsms that are waiting for this transaction to finish to perform a read.
%% 2. clean the state of the transaction. Namely:
%% 	a. ActiteTxsPerKey,
%% 	b. Waiting_Fsms,
%% 	c. PreparedTx
clean_and_notify(timeout, TxId, #state{active_tx=ActiveTx, prepared_tx=PreparedTx, 
					write_set=WriteSet, waiting_fsms=WaitingFsms, active_txs_per_key=ActiveTxsPerKey}) ->
	notify_all(ets:lookup(WaitingFsms, TxId), TxId),
	case ets:lookup(WriteSet, TxId) of
        [Op|Ops] ->
			clean_active_txs_per_key(TxId, [Op|Ops], ActiveTxsPerKey)
    end,
	ets:delete(PreparedTx, TxId),
	ets:delete(ActiveTx, TxId),
	ets:delete(WriteSet, TxId),
	ets:delete(WaitingFsms, TxId).




%%%===================================================================
%%% Internal Functions
%%%===================================================================

now_milisec({MegaSecs,Secs,MicroSecs}) ->
        (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

%% add_list_to_pendings([], _Sender, _Pendings) -> done;
%% 
%% add_list_to_pendings([Next|Rest], Sender, Pendings) ->
%%     ets:insert(Pendings, {Next, Sender}),
%%     add_list_to_pendings(Rest, Sender, Pendings).

notify_all([], _) -> done; 
notify_all([Next|Rest], TxId) -> 
    riak_core_vnode:reply(Next, {committed, TxId}),
    notify_all(Rest, TxId).

%% returns a list of transactions with
%% prepare timestamp bigger than TxId (snapshot time)
%% input:
%% 	ListOfPendingTxs: a List of all transactions in prepare state that update Key
%% 	Key: the key of the object for which there are prepared transactions that updated it.
pending_txs(ListOfPendingTxs, TxId, PreparedTx) -> 
	internal_pending_txs(ListOfPendingTxs, TxId, PreparedTx, []).
internal_pending_txs([], _TxId, _, Result) -> Result;
internal_pending_txs([H|T], TxId, PreparedTx, Result) ->
	case ets:lookup(PreparedTx, H) of {prepared, {PrepTime, ProcId}} ->
		case {PrepTime, ProcId} > TxId of
			true -> 
				internal_pending_txs(T, TxId,PreparedTx, lists:append(Result, H));
			false-> 
				internal_pending_txs(T, TxId, PreparedTx, Result)
		end
	end.


clean_active_txs_per_key(_, [], _) -> ok;
clean_active_txs_per_key(TxId, [Op|Ops], ActiveTxsPerKey) ->
	{Key, _}=Op,
	dets:delete_object(ActiveTxsPerKey, {Key, TxId}),
	clean_active_txs_per_key(TxId, Ops, ActiveTxsPerKey).

add_to_waiting_fsms(_, [], _) -> ok;    
add_to_waiting_fsms(WaitingFsms, [H|T], Sender) ->   
	ets:insert(WaitingFsms, {H, Sender}),
	add_to_waiting_fsms(WaitingFsms, T, Sender).
		
	
certification_check(_, [], _, _) -> true;
certification_check(TxId, TxWriteSet, CommittedTx, ActiveTxPerKey) ->
	[{Key, _}|T]=TxWriteSet,
	TxsPerKey=ets:lookup(ActiveTxPerKey, Key),
	case check_keylog(TxId, TxsPerKey, CommittedTx) of
		true -> false;	
		false ->
			certification_check(TxId, T, CommittedTx, ActiveTxPerKey)
	end.

check_keylog(_, [], _) -> true;
check_keylog(TxId, [H|T], CommittedTx)->
	case H > TxId of 
		true ->
			case ets:lookup(CommittedTx, H) of 
				true -> true;
				false->
					check_keylog(TxId, T, CommittedTx)
			end
	end.






	%%case (ets:lookup())
