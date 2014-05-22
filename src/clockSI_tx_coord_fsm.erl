%%@doc	The coordinator for a given Clock SI transaction.  
%%		It handles the state of the tx and executes the operations sequentially by
%%		sending each operation
%%		to the responsible clockSI_vnode of the involved key.
%%		when a tx is finalized (committed or aborted, the fsm
%%		also finishes.

-module(clockSI_tx_coord_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/3]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export	([handle_read/2, prepareOp/2, executeOp/2, finishOp/3, prepare_2PC/2, 
		receive_prepared/2, committing/2, receive_committed/2, abort/2, receive_aborted/2,
		reply_to_client/2]).


%-record(operationCSI, {opType, key, params}).

%%---------------------------------------------------------------------
%% Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    transaction: the transaction that this fsm handles, as defined in src/floppy.hrl.
%%    operations: a list of all the operation the tx involves.
%%    updated_partitions: the partitions where update operations take place. 
%%	  currentOp: a currently executing operation of the form {Key, Params}.
%%	  currentOpLeader: the partition responsible for the key involved in 'currentOP'.
%%	  num_to_ack: when sending prepare_commit, the number of partitions that have acked.
%% 	  prepare_time: transaction prepare time.
%% 	  commit_time: transaction commit time.
%%	  read_set: a list of the objects read by read operations that have already returned.
%% 	  state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------
-record(state, {
                from :: pid(),
                txid :: #tx_id{},
                operations :: list,
                updated_partitions :: list, 
                currentOp = undefined :: term() | undefined,
                num_to_ack :: int, 
                currentOpLeader :: riak_core_apl:preflist2(),
				prepare_time,	
				commit_time,
				read_set,
				state}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, ClientClock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, ClientClock, Operations], []).

finishOp(From, Key,Result) ->
   gen_fsm:send_event(From, {Key, Result}).
   
%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.

init([From, ClientClock, Operations]) ->	

	{ok, SnapshotTime}= get_snapshot_time(ClientClock),
	TransactionId=#tx_id{snapshot_time=clockSI_vnode:now_milisec(SnapshotTime), server_pid=From},            
    SD = #state{
                from=From,
                txid=TransactionId,
                operations=Operations,
                updated_partitions=[],
		prepare_time=0,
                read_set=[]
		},
    {ok, prepareOp, SD, 0}.


%% @doc Prepare the execution of the next operation. 
%%		It calculates the responsible vnode and sends the operation to it.
%%		when there are no more operations to be executed there are three posibilities:
%%		1.  it finishes (read tx),
%%		2. 	it starts a local_commit (update tx that only updates a single partition) or
%%		3.	it goes to the prepare_2PC to start a two phase commit (when multiple partitions
%%		are updated. 
prepareOp(timeout, SD0=#state{operations=Operations, txid=_TransactionId, updated_partitions=UpdatedPartitions}) ->
	case Operations of 
	[] ->
		case length(UpdatedPartitions) of
		0 ->
			io:format("Executed all operations of a read-only transaction.~n"),
			{stop, normal, SD0};
		1 ->
			[IndexNode]=UpdatedPartitions,
			io:format("Starting local commit at node ~w.~n", [IndexNode]),
			%clockSI_vnode:local_commit(IndexNode, TransactionId),
			%{stop, normal, SD0};
			{next_state, prepare_2PC, SD0, 0};
		_ ->
			{next_state, prepare_2PC, SD0, 0}		
		end;
		
	[Op|TailOps] ->
		[Op|TailOps] = Operations,
		{_, Key,_} = Op,
		DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
        io:format("ClockSI-Coord: PID ~w ~n ", [self()]),
        io:format("ClockSI-Coord: Op ~w ~n ", [Op]),
        io:format("ClockSI-Coord: TailOps ~w ~n ", [TailOps]),
        io:format("ClockSI-Coord: getting leader for Key ~w ~n", [Key]),
    	[Leader] = riak_core_apl:get_primary_apl(DocIdx, 1, ?CLOCKSI),	
        SD1 = SD0#state{operations=TailOps, currentOp=Op, currentOpLeader=Leader},
        {next_state, executeOp, SD1, 0}
    end.


%% @doc Contact the leader computed in the prepare state for it to execute the operation,
%%		wait for it to finish (synchronous) and go to the prepareOP to execute the next
%%		operation.
executeOp(timeout, SD0=#state{
                            currentOp=CurrentOp,
                            txid=TransactionId,
                            updated_partitions=UpdatedPartitions,
                            currentOpLeader=CurrentOpLeader}) ->    
    {OpType, Key, Param}=CurrentOp,                       
    io:format("ClockSI-Coord: Execute operation ~w ~n",[CurrentOp]),
	{IndexNode, _} = CurrentOpLeader,
	case OpType of read ->
		clockSI_vnode:read_data_item(IndexNode, TransactionId, Key, Param),
		io:format("ClockSI-Coord: Leader node ~w ~n",[CurrentOpLeader]),
		SD1=SD0;
	update ->
		clockSI_vnode:update_data_item(IndexNode, TransactionId, Key, Param),
		case lists:member(IndexNode, UpdatedPartitions) of
			false ->
				io:format("ClockSI-Coord: Adding Leader node ~w, updt: ~w ~n",[IndexNode, UpdatedPartitions]),
				NewUpdatedPartitions= lists:append(UpdatedPartitions, [IndexNode]),
				SD1 = SD0#state{updated_partitions= NewUpdatedPartitions};
			true->
				SD1 = SD0
		end
	end, 
    {next_state, prepareOp, SD1, 0}.
    
       
%%	Handles the reply of a clockSI_read_fsm when it has finished reading.
%%	It stores the replied ReadResult in the read_set list of the vnode's state.
handle_read({ReadResult}, #state{read_set=ReadSet}=State) ->
	case ReadResult of 
	error ->
		{no_reply, State};
    _ ->
        NewReadSet=lists:append(ReadSet, ReadResult),
        {no_reply, #state{read_set=NewReadSet}}
    end.   
%%	when the tx updates multiple partitions, a two phase commit protocol is started.
%%	the prepare_2PC state sends a prepare message to all updated partitions and goes
%%	to the "receive_prepared"state. 
prepare_2PC(timeout, SD0=#state{txid=TransactionId, updated_partitions=UpdatedPartitions}) ->
	clockSI_vnode:prepare(UpdatedPartitions, TransactionId),
	NumToAck=length(UpdatedPartitions),
	{next_state, receive_prepared, SD0#state{txid=TransactionId, num_to_ack=NumToAck, state=prepared}}.
	
%%	in this state, the fsm waits for prepare_time from each updated partitions in order
%%	to compute the final tx timestamp (the maximum of the received prepare_time).  
receive_prepared({prepared, ReceivedPrepareTime}, S0=#state{num_to_ack= NumToAck, prepare_time=PrepareTime}) ->		
	MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 1 -> 
    	io:format("ClockSI: Finished collecting prepare replies, start committing... Commit time: ~w~n",[MaxPrepareTime]),
	{next_state, committing, S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, state=committing},0};
    _ ->
        io:format("ClockSI: Keep collecting prepare replies~n"),
	{next_state, receive_prepared, S0#state{num_to_ack= NumToAck-1, prepare_time=MaxPrepareTime}}
    end;

receive_prepared(abort, S0) ->
	{next_state, abort, S0, 0};   

receive_prepared(timeout, S0) ->
	{next_state, abort, S0}.

%%	after receiving all prepare_times, send the commit message to all updated partitions,
%% 	and go to the "receive_committed" state.
committing(timeout, SD0=#state{txid=TransactionId, updated_partitions=UpdatedPartitions, prepare_time=PrepareTime}) -> 
	clockSI_vnode:commit(UpdatedPartitions, TransactionId, PrepareTime),
	NumToAck=length(UpdatedPartitions),
	{next_state, receive_committed, SD0#state{num_to_ack=NumToAck, state=committing}}.
	
%%	the fsm waits for acks indicating that each partition has successfully committed the tx
%%	and finishes operation.
%% 	Should we retry sending the committed message if we don't receive a reply from
%% 	every partition?
%% 	What delivery guarantees does sending messages provide?
receive_committed(committed, S0=#state{num_to_ack= NumToAck}) ->
    case NumToAck of
    1 -> 
    	io:format("ClockSI: Finished collecting commit acks. Tx committed succesfully.~n"),
    	{next_state, reply_to_client, S0#state{state=committed}, 0};
    _ ->
        io:format("ClockSI: Keep collecting commit replies~n"),
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
   	io:format("ClockSI-coord-fsm: Finished collecting abort acks. Tx aborted."),
    	{next_state, reply_to_client, S0, 0};
    _ ->
        io:format("ClockSI-coord-fsm: Keep collecting abort replies~n"),
	{next_state, receive_aborted, S0#state{num_to_ack= NumToAck-1}}
    end.
    
%% when the transaction has committed or aborted, a reply is sent to the client
%% that started the transaction.
reply_to_client(timeout, SD=#state{from=From, txid=TxId, read_set=ReadSet, state=TxState, commit_time=CommitTime}) ->
	case TxState of
	committed->
		From ! {ok, {TxId, ReadSet, CommitTime}};
	aborted->
		From ! {abort, TxId};
	Reason->
		From ! {ok, TxId, Reason}
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
	{Megasecs, Secs, Microsecs}=erlang:now(), 
	case (ClientClock > {Megasecs, Secs, Microsecs - ?DELTA}) of 
		true->
			%% should we wait until the clock of this machine catches up with this value?
			{ClientMegasecs, ClientSecs, ClientMicrosecs}=ClientClock,
			SnapshotTime = {ClientMegasecs, ClientSecs, ClientMicrosecs + ?MIN};

		false ->
			SnapshotTime = {Megasecs, Secs, Microsecs - ?DELTA}
	end,
	{ok, SnapshotTime}.
