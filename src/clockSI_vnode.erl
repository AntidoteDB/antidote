-module(clockSI_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
	 %API begin
	 read_data_item/4,
	 update_data_item/4,
	 prepare/2,
	 commit/2,
	 abort/2,
  	 notify_commit/2,
	 %API end
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

-record(state, {partition,log, prepared_tx, waiting_fsms, active_txs_per_key, write_set}).


%%%===================================================================
%%% API
%%%=================================================================== 

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).
    
read_data_item(Node, Tx, Key, Type) ->
    riak_core_vnode_master:sync_command(Node, {read_data_item, Tx, Key, Type}, ?CLOCKSIMASTER).

update_data_item(Node, Tx, Key, Op) ->
    riak_core_vnode_master:sync_command(Node, {update_data_item, Tx, Key, Op}, ?CLOCKSIMASTER).

prepare(Node, Tx) ->
    riak_core_vnode_master:sync_command(Node, {prepare, Tx}, ?CLOCKSIMASTER).

commit(Node, Tx) ->
    riak_core_vnode_master:sync_command(Node, {commit, Tx}, ?CLOCKSIMASTER).

abort(Node, Tx) ->
    riak_core_vnode_master:sync_command(Node, {abort, Tx}, ?CLOCKSIMASTER).

notify_commit(Node, Tx) ->
    riak_core_vnode_master:sync_command(Node, {notify_commit, Tx}, ?CLOCKSIMASTER).

init([Partition]) ->
    % It generates a dets file to store active transactions.
    TxLogFile = string:concat(integer_to_list(Partition), "clockSI_tx"),
    TxLogPath = filename:join(
            app_helper:get_env(riak_core, platform_data_dir), TxLogFile),
    case dets:open_file(TxLogFile, [{file, TxLogPath}, {type, bag}]) of
        {ok, TxLog} ->
	    	PreparedTx=prepared_tx,
			WaitingFsms=waiting_fsms,
			ActiveTxsPerKey=active_txs_per_key,
			WriteSet=write_set,
	    	ets:new(PreparedTx, [set, named_table, public]),
	    	ets:new(WaitingFsms, [bag, named_table, public]),
	    	ets:new(ActiveTxsPerKey, [bag, named_table, public]),
	    	ets:new(WriteSet, [bag, named_table, public]),
            {ok, #state{partition=Partition, log=TxLog, prepared_tx=PreparedTx, 
						write_set=WriteSet, waiting_fsms=WaitingFsms, active_txs_per_key=ActiveTxsPerKey}};
        {error, Reason} ->
            {error, Reason}
    end.

handle_command({read_data_item, TxId, Key, Type}, Sender, #state{partition= Partition, log=_Log}=State) ->
    Vnode={Partition, node()},
    clockSI_readitem_fsm:start_link(Vnode, Sender, TxId, Key, Type),
    {no_reply, State};

handle_command({update_data_item, TxId, Key, Op}, Sender, #state{write_set=WriteSet, active_txs_per_key=ActiveTxsPerKey, log=Log}=State) ->
	%%do we need the Sender here?
    
	%%clockSI_updateitem_fsm:start_link(Sender, Tx, Key, Op),
	Result = dets:insert(Log, {TxId, {Key, Op}}),
    case Result of
    	ok ->
			ets:insert(ActiveTxsPerKey, {Key, TxId}),
			ets:insert(WriteSet, {TxId, {Key, Op}}),
        	{reply, {ok, {Key, Op}}, State=#state{active_txs_per_key=ActiveTxsPerKey, write_set=WriteSet}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end,
    {no_reply, State};

handle_command({prepare, TxId}, _Sender, #state{log=Log, prepared_tx=PreparedTx}=State) ->
    case certification_check(TxId,Log) of
    	true ->
			PrepareTime=now_milisec(erlang:now()),
			Result = dets:insert(Log, {TxId, {prepare, PrepareTime}}),
    		case Result of
        		ok ->
					ets:insert(PreparedTx, TxId),
            		{reply, PrepareTime, State=#state{prepared_tx=PreparedTx}};
        		{error, Reason} ->
            		{reply, {error, Reason}, State}
    		end;
    	false ->
		%This does not match Ale's coordinator. We have to discuss it. It also expect Node.
			{reply, abort, State}
   		end;

handle_command({commit, TxId, TxCommitTime}, _Sender, #state{log=Log}=State) ->
			%ale: Log T.writeset
			Result = dets:insert(Log, {TxId, {commit, TxCommitTime}}),
    		case Result of
        		ok ->
					#state{log=Log}=State,
            		{reply, ack, State},
					{next_state, clean_and_notify, TxId, State, 0};
        		{error, Reason} ->
            		{reply, {error, Reason}, State}
    		end;


handle_command({abort, _Tx}, _Sender, #state{log=_Log}=State) ->
    %TODO: abort tx
    {no_reply, State};

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

clean_and_notify(timeout, TxId, State=#state{prepared_tx=PreparedTx, 
					write_set=WriteSet, waiting_fsms=WaitingFsms, active_txs_per_key=ActiveTxsPerKey}) ->
	case ets:lookup(WriteSet, TxId) of
        [Op|Ops] ->
			notify(TxId, [Op|Ops], WaitingFsms)
    end,
	ets:delete(PreparedTx, TxId),
	ets:delete(WriteSet, TxId).





%%%===================================================================
%%% Internal Functions
%%%===================================================================

now_milisec({MegaSecs,Secs,MicroSecs}) ->
        (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

add_list_to_pendings([], _Sender, _Pendings) -> done;

add_list_to_pendings([Next|Rest], Sender, Pendings) ->
    ets:insert(Pendings, {Next, Sender}),
    add_list_to_pendings(Rest, Sender, Pendings).
notify_all([],_Reply) -> done;
notify_all([Next|Rest],Reply) -> 
    riak_core_vnode:reply(Next, Reply),
    notify_all(Rest, Reply).
certification_check(_Tx,_Log) -> true.

state_cleanup(TxId, PreparedTx, ActiveTxsPerKey) ->
	{PreparedTx, ActiveTxsPerKey}.


notify([], TxId) -> {ok};
notify(TxId, [Op|Ops], ActiveTxsPerKey) -> 
	case (ets:lookup())
