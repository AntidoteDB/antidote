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
-export([prepareOp/2, executeOp/2, finishOp/3, prepare_2PC/2, receive_prepared/2, committing/2, receive_committed/2]).


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
%%----------------------------------------------------------------------
-record(state, {
                from :: pid(),
                transaction :: #tx{},
                operations :: list,
                updated_partitions :: list, 
                currentOp = undefined :: term() | undefined,
                num_to_ack :: int, 
                currentOpLeader :: riak_core_apl:preflist2()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Transaction=#tx{}, Operations) ->
    gen_fsm:start_link(?MODULE, [From, Transaction, Operations], []).

finishOp(From, Key,Result) ->
   gen_fsm:send_event(From, {Key, Result}).
   
%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state, set the transaction's state to active.
init([From, ClientClock, Operations]) ->	


	{Megasecs, Secs, Microsecs}=erlang:now(),
	 
	case (ClientClock > {Megasecs, Secs, Microsecs - ?DELTA}) of 
		true->
			%% should we wait?
			{ClientMegasecs, ClientSecs, ClientMicrosecs}=ClientClock,
			SnapshotTime = {ClientMegasecs, ClientSecs, ClientMicrosecs + ?MIN};

		false ->
			SnapshotTime = {Megasecs, Secs, Microsecs - ?DELTA}
	end,
		Transaction=#tx{snapshot_time=SnapshotTime, state=active, write_set=[]},
                
    SD = #state{
                from=From,
                transaction=Transaction,
                operations=Operations,
                updated_partitions=[]
		},
    {ok, prepareOp, SD, 0}.


%% @doc Prepare the execution of the next operation. 
%%		It calculates the responsible vnode and sends the operation to it.
%%		when there are no more operations to be executed there are three posibilities:
%%		1.  it finishes (read tx),
%%		2. 	it starts a local_commit (update tx that only updates a single partition) or
%%		3.	it goes to the prepare_2PC to start a two phase commit (when multiple partitions
$$		are updated. 
prepareOp(timeout, SD0=#state{operations=Operations, transaction=Transaction, updated_partitions=UpdatedPartitions}) ->
	case Operations of 
	[] ->
		case lists:lenght(UpdatedPartitions) of
		0 ->
			io:format("Executed all operations of a read-only transaction.~n"),
			{stop, normal, SD0};
		1 ->
			[IndexNode]=UpdatedPartitions,
			clockSI_vnode:local_commit(IndexNode, Transaction);
			{stop, normal, SD0};
		_ ->
			{next_state, prepare_2PC, SD0, 0}		
		end;
		
	[Op|TailOps] ->
		[Op|TailOps] = Operations,
		{_, Key,_} = Op, 
		DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    	[Leader] = riak_core_apl:get_primary_apl(DocIdx, 1, ?CLOCKSIMASTER),	
        SD1 = SD0#state{operations=TailOps, currentOp=Op, currentOpLeader=Leader},
        {next_state, executeOp, SD1, 0}
    end.


%% @doc Contact the leader computed in the prepare state for it to execute the operation,
%%		wait for it to finish (synchronous) and go to the prepareOP to execute the next
%%		operation.
executeOp(timeout, SD0=#state{
                            currentOp=CurrentOp,
                            transaction=Transaction,
                            updated_partitions=UpdatedPartitions,
                            currentOpLeader=CurrentOpLeader}) ->
    [OpType, DataType, Key, Param]=CurrentOp,                        
    io:format("Coord: Execute operation ~w ~w ~w ~w~n",[OpType, DataType, Key, Param]),
	io:format("Coord: Forward to node~w~n",[CurrentOpLeader]),
	{IndexNode, _} = CurrentOpLeader,
	case OpType of read ->
		clockSI_vnode:read_data_item(IndexNode, Transaction, Key, DataType),
		SD1=SD0;
	update ->
		clockSI_vnode:update_data_item(IndexNode, Transaction#tx{write_set=[]}, Key, Param),
		WriteSet= lists:append([Transaction#tx.write_set, [{CurrentOp}]]),
		NewUpdatedPartitions= lists:append(UpdatedPartitions, [{IndexNode}]),
		SD1 = SD0#state{transaction=Transaction#tx{write_set=WriteSet}, updated_partitions= NewUpdatedPartitions}
	end,
    {next_state, prepareOp, SD1, 0}.
    
%%	when the tx updates multiple partitions, a two phase commit protocol is started.
%%	the prepare_2PC state sends a prepare message to all updated partitions and goes
%%	to the "receive_prepared"state. 
prepare_2PC(timeout, SD0=#state{transaction=Transaction, updated_partitions=UpdatedPartitions}) ->
	TransactionLight=Transaction#tx{write_set=[]},
	clock_SI_vnode:prepare(TransactionLight, UpdatedPartitions),
	NumToAck=lists:lenght(UpdatedPartitions),
	{next_state, receive_prepared, SD0#state{transaction=TransactionLight, num_to_ack=NumToAck}, ?CLOCKSI_TIMEOUT}.
	
%%	in this state, the fsm waits for prepare_time from each updated partitions in order
%%	to compute the final tx timestamp (the maximum of the received prepare_time).  
receive_prepared({_Node, ReceivedPrepareTime}, S0=#state{num_to_ack= NumToAck, transaction=Transaction}) ->		
	PrepareTime = max(Transaction#tx.prepare_time, ReceivedPrepareTime),
    case NumToAck of 1 -> 
    	io:format("ClockSI: Finished collecting prepare replies, start committing..."),
		{next_state, committing, S0=#state{transaction=#tx{prepare_time=coomit_time=PrepareTime, state=committing}},0};
	_ ->
         io:format("ClockSI: Keep collecting prepare replies~n"),
	{next_state, receive_prepared, S0#state{num_to_ack= NumToAck-1}, Transaction#tx{prepare_time=PrepareTime}}
    end;
receive_prepared({_Node, abort}, S0) ->
	{next_state, abort, S0};   
receive_prepared(timeout, S0) ->
	{next_state, abort, S0}.

%%	after receiving all prepare_times, send the commit message to all updated partitions,
%% 	and go to the "receive_committed" state.
committing(timeout, SD0=#state{transaction=Transaction, updated_partitions=UpdatedPartitions}) -> 
	clock_SI_vnode:commit(UpdatedPartitions, Transaction),
	NumToAck=lists:lenght(UpdatedPartitions),
	{next_state, receive_committed, SD0#state{num_to_ack=NumToAck, transaction=#tx{state=committed}}, 0}.
	
%%	the fsm waits for acks indicating that each partition has successfully committed the tx
%%	and finishes operation.
%% Should we retry sending the committed message if we don't receive a reply from
%% every partition?
%% What delivery guarantees does sending messages provide?
receive_committed({_Node}, S0=#state{num_to_ack= NumToAck}) ->
    case NumToAck of 1 -> 
    	io:format("ClockSI: Finished collecting commit acks. Tx committed succesfully."),
    	{stop, normal, S0};
	_ ->
         io:format("ClockSI: Keep collecting prepare replies~n"),
		{next_state, receive_committed, S0#state{num_to_ack= NumToAck-1}}
    end.
    
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


