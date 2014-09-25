%% @doc The coordinator for a given Clock SI transaction.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clocksi_vnode of the
%%      involved key. when a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(clocksi_tx_coord_fsm).

-behavior(gen_fsm).

-include("floppy.hrl").

%% API
-export([start_link/3,
         start_link/2]).

%% Callbacks
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export ([prepare_op/2,
          execute_op/2,
          finish_op/3,
          prepare_2pc/2,
          receive_prepared/2,
          committing/2,
          receive_committed/2,
          abort/2,
          receive_aborted/2,
          reply_to_client/2]).

%%---------------------------------------------------------------------
%% Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    txid: transaction id that this fsm handles, as defined in src/floppy.hrl.
%%    operations: a list of all the operation the tx involves.
%%    updated_partitions: the partitions where update operations take place.
%%    currentOp: a currently executing operation of the form {Key, Params}.
%%    currentOpLeader: the partition responsible for the key involved in
%%                    'currentOP'.
%%    num_to_ack: when sending prepare_commit, number of partitions acked.
%%    prepare_time: transaction prepare time.
%%    commit_time: transaction commit time.
%%    read_set: a list of the objects read by read operations
%%              that have already returned.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------
-record(state, {
          from :: pid(),
          txid :: #tx_id{},
          operations :: list(),
          transaction :: #transaction{},
          updated_partitions :: list(),
          current_op = undefined :: term() | undefined,
          num_to_ack :: integer(),
          current_op_leader :: term(),
          prepare_time :: integer(),
          commit_time ::integer(),
          read_set :: list(),
          state :: prepared | committed | aborted | committing }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock, Operations], []).

start_link(From, Operations) ->
    gen_fsm:start_link(?MODULE, [From, ignore, Operations], []).

finish_op(From, Key,Result) ->
    gen_fsm:send_event(From, {Key, Result}).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, Operations]) ->
    {ok, LocalClock} = case ClientClock of
        ignore ->
            get_snapshot_time();
        _ ->
            get_snapshot_time(ClientClock)
    end,
    TransactionId = #tx_id{snapshot_time=LocalClock, server_pid=self()},
    %% Op = hd(Operations),
    %% case Op of
    %%     {update, K, _,_} -> Key = K;
    %%     {read,K,_} -> Key = K
    %%                       end,
    %{ok, VecSnapshotTime} = vectorclock:get_clock_by_key(Key),
    {ok, VecSnapshotTime} = vectorclock:get_clock_node(node()),
    DcId = dc_utilities:get_my_dc_id(),
    SnapshotTime = dict:update(DcId,
                                fun (_Old) -> LocalClock end,
                                LocalClock, VecSnapshotTime),
    Transaction = #transaction{snapshot_time=LocalClock,
                               vec_snapshot_time=SnapshotTime,
                               txn_id=TransactionId},
    lager:error("Transaction at vec_snapshot_time: ~p, snapshot_time: ~p",
                [SnapshotTime, LocalClock]),
    SD = #state{from=From,
                transaction=Transaction,
                operations=Operations,
                updated_partitions=[],
                prepare_time=0,
                read_set=[]},
    {ok, prepare_op, SD, 0}.

%% @doc Prepare the execution of the next operation. It calculates the
%%      responsible vnode and sends the operation to it. When there are no more
%%      operations to be executed there are three posibilities:
%%      1. it finishes (read tx),
%%      2. it starts a local_commit (tx that only updates a single partition)
%%      3. it starts a two phase commit (when multiple partitions are updated.
%%
prepare_op(timeout, SD0=#state{operations=Operations}) ->
    case Operations of
        [] ->
            {next_state, prepare_2pc, SD0, 0};
        [Op|TailOps] ->
            lager:error("Received operation: ~p", [Op]),
            [Op|TailOps] = Operations,
            {Key, FormattedOp} = case Op of
                {update, Key0, _Type, _} ->
                    {Key0, Op};
                {read, Key0, Type} ->
                    {Key0, {read, Key0, Type, ignore}}
            end,
            lager:info("ClockSI-Coord: PID ~w ~n ", [self()]),
            lager:info("ClockSI-Coord: Op ~w ~n ", [Op]),
            lager:info("ClockSI-Coord: TailOps ~w ~n ", [TailOps]),
            lager:info("ClockSI-Coord: getting leader for Key ~w ~n", [Key]),
            Logid = log_utilities:get_logid_from_key(Key),
            Preflist = log_utilities:get_preflist_from_logid(Logid),
            Leader = hd(Preflist),
            SD1 = SD0#state{operations=TailOps,
                            current_op=FormattedOp, current_op_leader=Leader},
            {next_state, execute_op, SD1, 0}
    end.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%       operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
%%
execute_op(timeout, SD0=#state{current_op=CurrentOp,
                               transaction=Transaction,
                               updated_partitions=UpdatedPartitions,
                               read_set=ReadSet,
                               current_op_leader=CurrentOpLeader}) ->
    {OpType, Key, Type, Param} = CurrentOp,
    lager:info("ClockSI-Coord: Execute operation ~w", [CurrentOp]),
    IndexNode = CurrentOpLeader,
    case OpType of
        read ->
            case clocksi_vnode:read_data_item(IndexNode,
                                              Transaction,
                                              Key,
                                              Type) of
                error ->
                    {next_state, abort, SD0};
                {error, _Reason} ->
                    {next_state, abort, SD0};
                ReadResult ->
                    NewReadSet = lists:append(ReadSet, [ReadResult]),
                    lager:info("Read value added to read set: ~p",
                               [CurrentOpLeader]),
                    SD1 = SD0#state{read_set=NewReadSet},
                    {next_state, prepare_op, SD1, 0}
            end;
        update ->
            case clocksi_vnode:update_data_item(IndexNode,
                                                Transaction,
                                                Key,
                                                Type,
                                                Param) of
                ok ->
                    case lists:member(IndexNode, UpdatedPartitions) of
                        false ->
                            lager:info("Adding leader: ~p, update: ~p",
                                       [IndexNode, UpdatedPartitions]),
                            NewUpdatedPartitions = lists:append(UpdatedPartitions,
                                                                [IndexNode]),
                            SD1 = SD0#state{updated_partitions=NewUpdatedPartitions},
                            {next_state, prepare_op, SD1, 0};
                        true->
                            {next_state, prepare_op, SD0, 0}
                    end;
                error ->
                    {next_state, abort, SD0};
                {error, _Reason} ->
                    {next_state, abort, SD0}
            end
    end.

%% @doc When the tx updates multiple partitions, a two phase commit
%%      protocol is started the prepare_2PC state sends a prepare message to
%%      all updated partitions and goes to the "receive_prepared" state.
%%
prepare_2pc(timeout, SD0=#state{transaction=Transaction,
                                updated_partitions=UpdatedPartitions}) ->
    case length(UpdatedPartitions) of
        0 ->
            SnapshotTime=Transaction#transaction.snapshot_time,
            {next_state, committing, SD0#state{state=prepared,
                                               commit_time=SnapshotTime}, 0};
        _ ->
            clocksi_vnode:prepare(UpdatedPartitions, Transaction),
            NumToAck = length(UpdatedPartitions),
            {next_state, receive_prepared,
             SD0#state{num_to_ack=NumToAck, state=prepared}}
    end.

%% @doc In this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the commit time.
%%
receive_prepared({prepared, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck, prepare_time=PrepareTime}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 1 ->
            lager:info("ClockSI: start committing... Commit time: ~w.",
                       [MaxPrepareTime]),
            {next_state, committing, S0#state{prepare_time=MaxPrepareTime,
                                              commit_time=MaxPrepareTime,
                                              state=committing}, 0};
        _ ->
            lager:info("ClockSI: Keep collecting prepare replies."),
            {next_state, receive_prepared, S0#state
             {num_to_ack=NumToAck-1, prepare_time=MaxPrepareTime}}
    end;

receive_prepared(abort, S0) ->
    {next_state, abort, S0, 0};

receive_prepared(timeout, S0) ->
    {next_state, abort, S0, 0}.

%% @doc After receiving all prepare_times, send the commit message to
%%      all updated partitions, and go to the "receive_committed" state.
%%
committing(timeout, SD0=#state{transaction=Transaction,
                               updated_partitions=UpdatedPartitions,
                               commit_time=CommitTime}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            lager:info("CommitTime: ~p", [CommitTime]),
            {next_state, reply_to_client, SD0#state{state=committed}, 0};
        _ ->
            clocksi_vnode:commit(UpdatedPartitions, Transaction, CommitTime),
            {next_state, receive_committed, SD0#state{num_to_ack=NumToAck}}
    end.

%% @doc The fsm waits for acks indicating that each partition has successfully
%%      committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't
%%      receive a reply from every partition?
%%      What delivery guarantees does sending messages provide?
%%
receive_committed(committed, S0=#state{num_to_ack=NumToAck}) ->
    case NumToAck of
        1 ->
            lager:info("ClockSI: Tx committed succesfully."),
            {next_state, reply_to_client, S0#state{state=committed}, 0};
        _ ->
            lager:info("ClockSI: Keep collecting commit replies."),
            {next_state, receive_committed, S0#state{num_to_ack=NumToAck-1}}
    end.

%% @doc When an updated partition does not pass the certification check,
%%      the transaction aborts.
%%
abort(timeout, SD0=#state{transaction=Transaction,
                          updated_partitions=UpdatedPartitions}) ->
    clocksi_vnode:abort(UpdatedPartitions, Transaction#transaction.txn_id),
    NumToAck = length(UpdatedPartitions),
    {next_state, receive_aborted,
     SD0#state{state=aborted, num_to_ack=NumToAck}};

abort(abort, SD0=#state{transaction=Transaction,
                        updated_partitions=UpdatedPartitions}) ->
    clocksi_vnode:abort(UpdatedPartitions, Transaction#transaction.txn_id),
    NumToAck = length(UpdatedPartitions),
    {next_state, receive_aborted,
     SD0#state{state=aborted, num_to_ack=NumToAck}}.

%% @doc The fsm waits for acks indicating that each partition has
%%      aborted the tx and finishes operation.
%%
receive_aborted(ack_abort, S0=#state{num_to_ack= NumToAck}) ->
    case NumToAck of
        1 ->
            lager:info("ClockSI-coord-fsm: Tx aborted."),
            {next_state, reply_to_client, S0, 0};
        _ ->
            lager:info("ClockSI-coord-fsm: Keep collecting abort replies."),
            {next_state, receive_aborted, S0#state{num_to_ack=NumToAck-1}}
    end.

%% @doc When the transaction has committed or aborted, a reply is sent
%%      to the client that started the transaction.
%%
reply_to_client(timeout, SD=#state{from=From,
                                   transaction=Transaction,
                                   read_set=ReadSet,
                                   state=TxState,
                                   commit_time=CommitTime}) ->
    TxId = Transaction#transaction.txn_id,
    _ = case TxState of
        committed ->
            From ! {ok, {TxId, ReadSet, CommitTime}};
        aborted ->
            From ! {abort, TxId};
        Reason ->
            From ! {ok, TxId, Reason}
    end,
    {stop, normal, SD}.

%% ====================================================================

handle_info(Info, _StateName, StateData) ->
    lager:info("Received ignored info: ~p", [Info]),
    {stop, badmsg, StateData}.

handle_event(Event, _StateName, StateData) ->
    lager:info("Received ignored event: ~p", [Event]),
    {stop, badmsg, StateData}.

handle_sync_event(Event, _From, _StateName, StateData) ->
    lager:info("Received ignored sync event: ~p", [Event]),
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(Reason, _SN, _SD) ->
    lager:info("Terminate triggered with reason: ~p", [Reason]),
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Set the transaction Snapshot Time to the maximum value of:
%%      1. ClientClock, which is the last clock of the system the client
%%         starting this transaction has seen, and
%%      2. machine's local time, as returned by erlang:now().
%%
-spec get_snapshot_time(non_neg_integer()) -> {ok, non_neg_integer()}.
get_snapshot_time(ClientClock) ->
    Now = clocksi_vnode:now_milisec(erlang:now()),
    SnapshotTime = case (ClientClock > Now) of
        true->
            ClientClock;
        false ->
            Now
    end,
    {ok, SnapshotTime}.

-spec get_snapshot_time() -> {ok, non_neg_integer()}.
get_snapshot_time() ->
    Now = clocksi_vnode:now_milisec(erlang:now()),
    SnapshotTime  = Now,
    {ok, SnapshotTime}.
