%%@doc	The coordinator for a given Clock SI interactive transaction.  
%%		It handles the state of the tx and executes the operations sequentially by
%%		sending each operation
%%		to the responsible clockSI_vnode of the involved key.
%%		when a tx is finalized (committed or aborted, the fsm
%%		also finishes.

-module(clockSI_interactive_tx_coord_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/2]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export	([executeOp/3, finishOp/3, prepare/2, 
		receive_prepared/2, committing/3, receive_committed/2, abort/2, receive_aborted/2,
		reply_to_client/2]).


%-record(operationCSI, {opType, key, params}).

%%---------------------------------------------------------------------
%% Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    txid: the transaction id that this fsm handles, as defined in src/floppy.hrl.
%%    updated_partitions: the partitions where update operations take place. 
%%	  num_to_ack: when sending prepare_commit, the number of partitions that have acked.
%% 	  prepare_time: transaction prepare time.
%% 	  commit_time: transaction commit time.
%% 	  state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------
-record(state, {
                from,
                txid :: #tx_id{},
                updated_partitions :: list, 
                num_to_ack :: int, 
				prepare_time :: int,	
				commit_time ::int,
				state:: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, ClientClock) ->
    gen_fsm:start_link(?MODULE, [From, ClientClock], []).

finishOp(From, Key,Result) ->
   gen_fsm:send_event(From, {Key, Result}).
   
%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.

init([From, ClientClock]) ->	
	{ok, SnapshotTime}= get_snapshot_time(ClientClock),
	TxId=#tx_id{snapshot_time=SnapshotTime, server_pid=self()},            
	SD = #state{
				txid=TxId,
				updated_partitions=[],
		        prepare_time=0
		},
	From ! {ok, TxId},
	{ok, executeOp, SD}.
	
%% @doc Contact the leader computed in the prepare state for it to execute the operation,
%%		wait for it to finish (synchronous) and go to the prepareOP to execute the next
%%		operation.
executeOp({OpType, Args}, Sender, SD0=#state{
                            txid=TransactionId, from=From,
                            updated_partitions=UpdatedPartitions}) ->
	case OpType of
	prepare ->
        lager:info("ClockSI-Interactive-Coord: Sender ~w ~n ", [Sender]),
		{next_state, prepare, SD0#state{from=Sender}, 0};
	read ->
		{Key, Param}=Args,                       
		DocIdx = riak_core_util:chash_key({?BUCKET,
									   term_to_binary(Key)}),
		lager:info("ClockSI-Interactive-Coord: PID ~w ~n ", [self()]),
		lager:info("ClockSI-Interactive-Coord: Op ~w ~n ", [Args]),
		lager:info("ClockSI-Interactive-Coord: Sender ~w ~n ", [Sender]),
		lager:info("ClockSI-Interactive-Coord: getting leader for Key ~w ~n", [Key]),
		[{IndexNode,_}] = riak_core_apl:get_primary_apl(DocIdx, 1, ?CLOCKSI),	
		case clockSI_vnode:read_data_item(IndexNode, TransactionId, Key, Param) of
		error ->
			{reply, error, abort, SD0};
		ReadResult -> 
			lager:info("ClockSI-Interactive-Coord: Read Result:  ~w ~n",[ReadResult]),
			{reply, {ok, ReadResult}, executeOp, SD0}
		end;
	update ->
		{Key, Param}=Args,                       
		DocIdx = riak_core_util:chash_key({?BUCKET,
									   term_to_binary(Key)}),
		lager:info("ClockSI-Interactive-Coord: PID ~w ~n ", [self()]),
		lager:info("ClockSI-Interactive-Coord: Op ~w ~n ", [Args]),
		lager:info("ClockSI-Interactive-Coord: Sender ~w ~n ", [Sender]),
		lager:info("ClockSI-Interactive-Coord: From ~w ~n ", [From]),
		lager:info("ClockSI-Interactive-Coord: getting leader for Key ~w ~n", [Key]),
		[{IndexNode,_}] = riak_core_apl:get_primary_apl(DocIdx, 1, ?CLOCKSI),	

		case clockSI_vnode:update_data_item(IndexNode, TransactionId, Key, Param) of
		ok ->
		    case lists:member(IndexNode, UpdatedPartitions) of
			false ->
				lager:info("ClockSI-Interactive-Coord: Adding Leader node ~w, updt: ~w ~n",
				[IndexNode, UpdatedPartitions]),
				NewUpdatedPartitions= lists:append(UpdatedPartitions, [IndexNode]),
				{reply, ok, executeOp, SD0#state{updated_partitions= NewUpdatedPartitions}};
			true->
				{reply, ok, executeOp, SD0}
		    end;
		error ->
			{reply, error, abort, SD0}
		end
	end.     
       
 
%%	a message from a client wanting to start committing the tx.
%%	this state sends a prepare message to all updated partitions and goes
%%	to the "receive_prepared"state. 
prepare(timeout, SD0=#state{txid=TransactionId, updated_partitions=UpdatedPartitions, from=From}) ->
    case length(UpdatedPartitions) of
	0->
		SnapshotTime=TransactionId#tx_id.snapshot_time,
		gen_fsm:reply(From, {ok, SnapshotTime}),
	    {next_state, committing, SD0#state{state=committing, commit_time=SnapshotTime}};
	_->
		clockSI_vnode:prepare(UpdatedPartitions, TransactionId),
		NumToAck=length(UpdatedPartitions),
		{next_state, receive_prepared, SD0#state{txid=TransactionId, num_to_ack=NumToAck, state=prepared}}
	end.
	
	
%%	in this state, the fsm waits for prepare_time from each updated partitions in order
%%	to compute the final tx timestamp (the maximum of the received prepare_time).  
receive_prepared({prepared, ReceivedPrepareTime}, S0=#state{num_to_ack= NumToAck, from= From, prepare_time=PrepareTime}) ->		
	MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 1 -> 
    	lager:info("ClockSI: Finished collecting prepare replies, start committing... Commit time: ~w~n",[MaxPrepareTime]),
		gen_fsm:reply(From, {ok, MaxPrepareTime}),
		{next_state, committing, S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, state=committing}};
    _ ->
        lager:info("ClockSI: Keep collecting prepare replies~n"),
	{next_state, receive_prepared, S0#state{num_to_ack= NumToAck-1, prepare_time=MaxPrepareTime}}
    end;

receive_prepared(abort, S0) ->
	{next_state, abort, S0, 0};   

receive_prepared(timeout, S0) ->
	{next_state, abort, S0 ,0}.

%%	after receiving all prepare_times, send the commit message to all updated partitions,
%% 	and go to the "receive_committed" state.
committing(commit, Sender, SD0=#state{txid=TransactionId, 
				updated_partitions=UpdatedPartitions, commit_time=CommitTime}) -> 
	NumToAck=length(UpdatedPartitions),
	case NumToAck of
		0 ->
			{next_state, reply_to_client, SD0#state{state=committed, from=Sender},0};
		_ ->
			clockSI_vnode:commit(UpdatedPartitions, TransactionId, CommitTime),
			{next_state, receive_committed, SD0#state{num_to_ack=NumToAck, from=Sender, state=committing}}
	end.
	
	
%%	the fsm waits for acks indicating that each partition has successfully committed the tx
%%	and finishes operation.
%% 	Should we retry sending the committed message if we don't receive a reply from
%% 	every partition?
%% 	What delivery guarantees does sending messages provide?
receive_committed(committed, S0=#state{num_to_ack= NumToAck}) ->
    case NumToAck of
    1 -> 
    	lager:info("ClockSI: Finished collecting commit acks. Tx committed succesfully.~n"),
    	{next_state, reply_to_client, S0#state{state=committed}, 0};
    _ ->
        lager:info("ClockSI: Keep collecting commit replies~n"),
	{next_state, receive_committed, S0#state{num_to_ack= NumToAck-1}}
    end.
    
%% when an updated partition does not pass the certification check, the transaction
%% aborts.
abort(timeout, SD0=#state{txid=TxId, updated_partitions=UpdatedPartitions}) -> 
	clockSI_vnode:abort(UpdatedPartitions, TxId),
	NumToAck=lists:lenght(UpdatedPartitions),
	{next_state, receive_aborted, SD0#state{state=abort, num_to_ack=NumToAck}, 0};

abort(abort, SD0=#state{txid=TxId, updated_partitions=UpdatedPartitions}) -> 
	%TODO: Do not send to who issue the abort
	clockSI_vnode:abort(UpdatedPartitions, TxId),
	NumToAck=length(UpdatedPartitions),
	{next_state, receive_aborted, SD0#state{state=abort, num_to_ack=NumToAck}}.
	
%%	the fsm waits for acks indicating that each partition has aborted the tx
%%	and finishes operation.	
receive_aborted(ack_abort, S0=#state{num_to_ack= NumToAck}) ->
    case NumToAck of 1 -> 
   	lager:info("ClockSI-coord-fsm: Finished collecting abort acks. Tx aborted."),
    	{next_state, reply_to_client, S0, 0};
    _ ->
        lager:info("ClockSI-coord-fsm: Keep collecting abort replies~n"),
	{next_state, receive_aborted, S0#state{num_to_ack= NumToAck-1}}
    end.
    
%% when the transaction has committed or aborted, a reply is sent to the client
%% that started the transaction.
reply_to_client(timeout, SD=#state{from=From, txid=TxId, state=TxState, commit_time=CommitTime}) ->
	case TxState of
	committed->
		Reply={ok, {TxId, CommitTime}}, 
		lager:info("ClockSI-coord-fsm: Replying ~w to ~w ~n", [Reply, From]),
		gen_fsm:reply(From,Reply);
	aborted->
		gen_fsm:reply(From,{abort, TxId});
	Reason->
		gen_fsm:reply(From,{ok, TxId, Reason})
	end,
	{stop, normal, SD}.
	

    
%% ====================================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================


%%	Set the transaction Snapshot Time to the maximum value of:
%%	1.	ClientClock, which is the last clock of the system the client
%%		starting this transaction has seen, and
%%	2. 	machine's local time, as returned by erlang:now(). 	
get_snapshot_time(ClientClock) ->
	Now=clockSI_vnode: now_milisec(erlang:now()), 
	case (ClientClock > Now) of 
		true->
			SnapshotTime = ClientClock + ?MIN;
		false ->
			SnapshotTime = Now - ?DELTA
	end,
	{ok, SnapshotTime}.
