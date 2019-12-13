%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% @doc The coordinator for a given Clock SI interactive transaction.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. When a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(clocksi_interactive_coord).

-behavior(gen_statem).

-include("antidote.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DC_META_UTIL, mock_partition).
-define(DC_UTIL, mock_partition).
-define(VECTORCLOCK, mock_partition).
-define(LOG_UTIL, mock_partition).
-define(CLOCKSI_VNODE, mock_partition).
-define(CLOCKSI_DOWNSTREAM, mock_partition).
-define(LOGGING_VNODE, mock_partition).

-else.
-define(DC_META_UTIL, dc_utilities).
-define(DC_UTIL, dc_utilities).
-define(VECTORCLOCK, vectorclock).
-define(LOG_UTIL, log_utilities).
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(CLOCKSI_DOWNSTREAM, clocksi_downstream).
-define(LOGGING_VNODE, logging_vnode).
-endif.


%% API
-export([
    start_link/0,
    perform_singleitem_operation/4,
    perform_singleitem_update/5,
    finish_op/3
]).

%% gen_statem callbacks
-export([
    init/1,
    code_change/4,
    callback_mode/0,
    terminate/3,
    stop/1,
    wait_for_start_transaction/3
]).

%% states
-export([
    receive_committed/3,
    receive_logging_responses/3,
    receive_read_objects_result/3,
    receive_aborted/3,
    single_committing/3,
    receive_prepared/3,
    execute_op/3,

    committing/3,
    committing_single/3
]).

%%%===================================================================
%%% API
%%%===================================================================


% called by clocksi_interactive_coord_sup:start_fm
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_statem:start_link(?MODULE, [], []).


%% TODO spec
stop(Pid) -> gen_statem:stop(Pid).

%% @doc This is a standalone function for directly contacting the read
%%      server located at the vnode of the key being read.  This read
%%      is supposed to be light weight because it is done outside of a
%%      transaction fsm and directly in the calling thread.
%%      It either returns the object value or the object state.
-spec perform_singleitem_operation(snapshot_time() | ignore, key(), type(), clocksi_readitem:read_property_list()) ->
    {ok, val() | term(), snapshot_time()} | {error, reason()}.
perform_singleitem_operation(Clock, Key, Type, Properties) ->
    Transaction = create_transaction_record(Clock, true, Properties),
    %%OLD: {Transaction, _TransactionId} = create_transaction_record(ignore, update_clock, false, undefined, true),
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case clocksi_readitem:read_data_item(IndexNode, Key, Type, Transaction, []) of
        {error, Reason} ->
            {error, Reason};
        {ok, Snapshot} ->
            %% Read only transaction has no commit, hence return the snapshot time
            CommitTime = Transaction#transaction.vec_snapshot_time,
            {ok, Snapshot, CommitTime}
    end.

%% @doc This is a standalone function for directly contacting the update
%%      server vnode.  This is lighter than creating a transaction
%%      because the update/prepare/commit are all done at one time
-spec perform_singleitem_update(snapshot_time() | ignore, key(), type(), {atom(), term()}, list()) -> {ok, {txid(), [], snapshot_time()}} | {error, term()}.
perform_singleitem_update(Clock, Key, Type, Params, Properties) ->
    Transaction = create_transaction_record(Clock, true, Properties),
    Partition = ?LOG_UTIL:get_key_partition(Key),
    %% Execute pre_commit_hook if any
    case antidote_hooks:execute_pre_commit_hook(Key, Type, Params) of
        {Key, Type, Params1} ->
            case ?CLOCKSI_DOWNSTREAM:generate_downstream_op(Transaction, Partition, Key, Type, Params1, []) of
                {ok, DownstreamRecord} ->
                    UpdatedPartitions = [{Partition, [{Key, Type, DownstreamRecord}]}],
                    TxId = Transaction#transaction.txn_id,
                    LogRecord = #log_operation{
                        tx_id=TxId,
                        op_type=update,
                        log_payload=#update_log_payload{key=Key, type=Type, op=DownstreamRecord}
                    },
                    LogId = ?LOG_UTIL:get_logid_from_key(Key),
                    case ?LOGGING_VNODE:append(Partition, LogId, LogRecord) of
                        {ok, _} ->
                            case ?CLOCKSI_VNODE:single_commit_sync(UpdatedPartitions, Transaction) of
                                {committed, CommitTime} ->

                                    %% Execute post commit hook
                                    case antidote_hooks:execute_post_commit_hook(Key, Type, Params1) of
                                        {error, Reason} ->
                                            ?LOG_INFO("Post commit hook failed. Reason ~p", [Reason]);
                                        _ ->
                                            ok
                                    end,

                                    TxId = Transaction#transaction.txn_id,
                                    DcId = ?DC_META_UTIL:get_my_dc_id(),

                                    CausalClock = ?VECTORCLOCK:set(
                                        DcId,
                                        CommitTime,
                                        Transaction#transaction.vec_snapshot_time
                                    ),

                                    {ok, {TxId, [], CausalClock}};

                                abort ->
                                    % TODO increment aborted transaction metrics?
                                    {error, aborted};
                                {error, Reason} ->
                                    {error, Reason}
                            end;

                        Error ->
                            {error, Error}
                    end;

                {error, Reason} ->
                    {error, Reason}
            end;

        {error, Reason} ->
            {error, Reason}
    end.

%% TODO spec
finish_op(From, Key, Result) ->
    gen_statem:cast(From, {Key, Result}).

%%%===================================================================
%%% Internal State
%%%===================================================================

%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    txid: transaction id handled by this fsm, as defined in src/antidote.hrl.
%%    updated_partitions: the partitions where update operations take place.
%%    num_to_ack: when sending prepare_commit,
%%                number of partitions that have acknowledged.
%%    num_to_read: when sending read requests
%%                 number of partitions that are asked.
%%    prepare_time: transaction prepare time.
%%    commit_time: transaction commit time.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------

-record(state, {
    from :: undefined | gen_statem:from(),
    transaction :: undefined | tx(),
    updated_partitions :: list(),
    client_ops :: list(), % list of upstream updates, used for post commit hooks
    num_to_ack :: non_neg_integer(),
    num_to_read :: non_neg_integer(),
    prepare_time :: undefined | clock_time(),
    commit_time :: undefined | clock_time(),
    commit_protocol :: term(),
    state :: active | prepared | committing
    | committed | committed_read_only
    | undefined | aborted,
    operations :: undefined | list() | {update_objects, list()},
    return_accumulator :: list() | ok | {error, reason()},
    is_static :: boolean(),
    full_commit :: boolean(),
    properties :: txn_properties()
}).

-type state() :: #state{}.

%%%===================================================================
%%% States
%%%===================================================================

%%%== init

%% @doc Initialize the state.
init([]) ->
    {ok, wait_for_start_transaction, ignore}.


wait_for_start_transaction({call, Sender}, {start_tx, ClientClock, Properties}, _State) ->
    BaseState = init_state(false, false, Properties),
    {ok, State} = start_tx_internal(ClientClock, Properties, BaseState),
    TxnId = (State#state.transaction)#transaction.txn_id,
    {next_state, execute_op, State, {reply, Sender, {ok, TxnId}}}.


%%%== execute_op

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
%% internal state timeout
-spec execute_op(state_timeout, timeout, state()) -> gen_statem:event_handler_result(state());
    ({call, pid()}, undefined | list() | {update_objects, list()} | {update, list()}, state()) -> gen_statem:event_handler_result(state()).
execute_op(state_timeout, timeout, State = #state{operations = Operations, from = From}) ->
    execute_op({call, From}, Operations, State);

%% update kept for backwards compatibility with tests.
execute_op({call, Sender}, {update, Args}, State) ->
    execute_op({call, Sender}, {update_objects, [Args]}, State);

execute_op({call, Sender}, {OpType, Args}, State) ->
    execute_command(OpType, Args, Sender, State).

%%%== receive_prepared

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared(cast, {prepared, ReceivedPrepareTime}, State) ->
    process_prepared(ReceivedPrepareTime, State);

receive_prepared(cast, abort, State) ->
    receive_prepared(cast, timeout, State);

receive_prepared(cast, timeout, State) ->
    abort(State);

%% capture regular events (e.g. logging_vnode responses)
receive_prepared(info, {_EventType, EventValue}, State) ->
    receive_prepared(cast, EventValue, State).


%%%== committing

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected
committing({call, Sender}, commit, State = #state{transaction = Transaction,
    updated_partitions = UpdatedPartitions,
    commit_time = Commit_time}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            reply_to_client(State#state{state = committed_read_only, from = Sender});
        _ ->
            ok = ?CLOCKSI_VNODE:commit(UpdatedPartitions, Transaction, Commit_time),
            {next_state, receive_committed,
                State#state{num_to_ack = NumToAck, from = Sender, state = committing}}
    end.

%%%== single_committing

%% @doc TODO
-spec single_committing(cast, {committed | clock_time()} | abort | timeout, state()) -> gen_statem:event_handler_result(state());
    (info, {any(), any()}, state()) -> gen_statem:event_handler_result(state()).
single_committing(cast, {committed, CommitTime}, State = #state{from = From, full_commit = FullCommit}) ->
    case FullCommit of
        false ->
            {next_state, committing_single, State#state{commit_time = CommitTime, state = committing},
                [{reply, From, {ok, CommitTime}}]};
        true ->
            reply_to_client(State#state{prepare_time = CommitTime, commit_time = CommitTime, state = committed})
    end;

single_committing(cast, abort, State) ->
    single_committing(cast, timeout, State);

single_committing(cast, timeout, State) ->
    abort(State);

%% capture regular events (e.g. logging_vnode responses)
single_committing(info, {_EventType, EventValue}, State) ->
    single_committing(cast, EventValue, State).


%%%== receive_aborted

%% @doc the fsm waits for acks indicating that each partition has successfully
%%      aborted the tx and finishes operation.
%%      Should we retry sending the aborted message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_aborted(cast, ack_abort, State = #state{num_to_ack = NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(State#state{state = aborted});
        _ ->
            {next_state, receive_aborted, State#state{num_to_ack = NumToAck - 1}}
    end;

receive_aborted(cast, _, State) -> {next_state, receive_aborted, State};

%% capture regular events (e.g. logging_vnode responses)
receive_aborted(info, {_EventType, EventValue}, State) ->
    receive_aborted(cast, EventValue, State).


%%%== receive_read_objects_result

%% @doc After asynchronously reading a batch of keys, collect the responses here
receive_read_objects_result(cast, {ok, {Key, Type, Snapshot}}, CoordState = #state{
    num_to_read = NumToRead,
    return_accumulator = ReadKeys
}) ->
    %% Apply local updates to the read snapshot
    UpdatedSnapshot = apply_tx_updates_to_snapshot(Key, CoordState, Type, Snapshot),

    %% Swap keys with their appropriate read values
    ReadValues = replace_first(ReadKeys, Key, UpdatedSnapshot),

    %% Loop back to the same state until we process all the replies
    case NumToRead > 1 of
        true ->
            {next_state, receive_read_objects_result, CoordState#state{
                num_to_read = NumToRead - 1,
                return_accumulator = ReadValues
            }};

        false ->
            {next_state, execute_op, CoordState#state{num_to_read = 0},
                [{reply, CoordState#state.from, {ok, lists:reverse(ReadValues)}}]}
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_read_objects_result(info, {_EventType, EventValue}, State) ->
    receive_read_objects_result(cast, EventValue, State).


%%%== receive_logging_responses

%% internal state timeout
receive_logging_responses(state_timeout, timeout, State) ->
    receive_logging_responses(cast, timeout, State);
%% @doc This state reached after an execute_op(update_objects[Params]).
%% update_objects calls the perform_update function, which asynchronously
%% sends a log operation per update, to the vnode responsible of the updated
%% key. After sending all those messages, the coordinator reaches this state
%% to receive the responses of the vnodes.
receive_logging_responses(cast, Response, State = #state{
    is_static = IsStatic,
    num_to_read = NumToReply,
    return_accumulator = ReturnAcc
}) ->

    NewAcc = case Response of
                 {error, _r} = Err -> Err;
                 {ok, _OpId} -> ReturnAcc;
                 timeout -> ReturnAcc
             end,

    %% Loop back to the same state until we process all the replies
    case NumToReply > 1 of
        true ->
            {next_state, receive_logging_responses, State#state{
                num_to_read=NumToReply - 1,
                return_accumulator=NewAcc
            }};

        false ->
            case NewAcc of
                ok ->
                    case IsStatic of
                        true ->
                            prepare(State);
                        false ->
                            {next_state, execute_op, State#state{num_to_read=0, return_accumulator=[]},
                                [{reply, State#state.from, NewAcc}]}
                    end;

                _ ->
                    abort(State)
            end
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_logging_responses(info, {_EventType, EventValue}, State) ->
    receive_logging_responses(cast, EventValue, State).


%%%== receive_committed

%% @doc the fsm waits for acks indicating that each partition has successfully
%%      committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_committed(cast, committed, State = #state{num_to_ack = NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(State#state{state = committed});
        _ ->
            {next_state, receive_committed, State#state{num_to_ack = NumToAck - 1}}
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_committed(info, {_EventType, EventValue}, State) ->
    receive_committed(cast, EventValue, State).


%%%== committing_single

%% @doc There was only a single partition with an update in this transaction
%%      so the transaction has already been committed
%%      so just wait for the commit message from the client
committing_single({call, Sender}, commit, State = #state{commit_time = Commit_time}) ->
    reply_to_client(State#state{
        prepare_time = Commit_time,
        from = Sender,
        commit_time = Commit_time,
        state = committed
    }).

%% =============================================================================

%% TODO add to all state functions
%%handle_sync_event(stop, _From, _StateName, StateData) -> {stop, normal, ok, StateData}.

%%handle_call(From, stop, Data) ->
%%    {stop_and_reply, normal,  {reply, From, ok}, Data}.
%%
%%handle_info(Info, StateName, Data) ->
%%    {stop, {shutdown, {unexpected, Info, StateName}}, StateName, Data}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) -> ok.

callback_mode() -> state_functions.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc TODO
-spec init_state(boolean(), boolean(), proplists:proplist()) -> state().
init_state(FullCommit, IsStatic, Properties) ->
    #state{
        from = undefined,
        transaction = undefined,
        updated_partitions = [],
        client_ops = [],
        num_to_ack = 0,
        num_to_read = 0,
        prepare_time = 0,
        operations = undefined,
        return_accumulator = [],
        is_static = IsStatic,
        full_commit = FullCommit,
        properties = Properties
    }.


%% @doc TODO
-spec start_tx_internal(snapshot_time(), proplists:proplist(), state()) -> {ok, state()} | {error, any()}.
start_tx_internal(ClientClock, Properties, State = #state{}) ->
    TransactionRecord = create_transaction_record(ClientClock, false, Properties),
    % a new transaction was started, increment metrics
    ?STATS(open_transaction),
    {ok, State#state{transaction = TransactionRecord, num_to_read = 0, properties = Properties}}.


%% @doc TODO
-spec create_transaction_record(snapshot_time() | ignore, boolean(), txn_properties()) -> tx().
%%noinspection ErlangUnresolvedFunction
create_transaction_record(ClientClock, _IsStatic, Properties) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    _Res = rand:seed(exsplus, {erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()}),
    {ok, SnapshotTime} = case ClientClock of
                             ignore ->
                                 get_snapshot_time();
                             _ ->
                                 case antidote:get_txn_property(update_clock, Properties) of
                                     update_clock ->
                                         get_snapshot_time(ClientClock);
                                     no_update_clock ->
                                         {ok, ClientClock}
                                 end
                         end,
    DcId = ?DC_META_UTIL:get_my_dc_id(),
    LocalClock = ?VECTORCLOCK:get(DcId, SnapshotTime),
    TransactionId = #tx_id{local_start_time = LocalClock, server_pid = self()},
    #transaction{snapshot_time_local = LocalClock,
        vec_snapshot_time = SnapshotTime,
        txn_id = TransactionId,
        properties = Properties}.


%% @doc Execute the commit protocol
-spec execute_command(atom(), term(), pid(), state()) -> gen_statem:event_handler_result(state()).
execute_command(prepare, Protocol, Sender, State0) ->
    State = State0#state{from=Sender, commit_protocol=Protocol},
    prepare(State);

%% @doc Abort the current transaction
execute_command(abort, _Protocol, Sender, State) ->
    abort(State#state{from=Sender});

%% @doc Perform a single read, synchronous
execute_command(read, {Key, Type}, Sender, State = #state{
    transaction=Transaction,
    updated_partitions=UpdatedPartitions
}) ->
    case perform_read({Key, Type}, UpdatedPartitions, Transaction, Sender) of
        {error, _} ->
            abort(State);
        ReadResult ->
            {next_state, execute_op, State, {reply, Sender, {ok, ReadResult}}}
    end;

%% @doc Read a batch of objects, asynchronous
execute_command(read_objects, Objects, Sender, State = #state{transaction=Transaction}) ->
    ExecuteReads = fun({Key, Type}, AccState) ->
        ?STATS(operation_read_async),
        Partition = ?LOG_UTIL:get_key_partition(Key),
        ok = clocksi_vnode:async_read_data_item(Partition, Transaction, Key, Type),
        ReadKeys = AccState#state.return_accumulator,
        AccState#state{return_accumulator=[Key | ReadKeys]}
                   end,

    NewCoordState = lists:foldl(
        ExecuteReads,
        State#state{num_to_read = length(Objects), return_accumulator=[]},
        Objects
    ),

    {next_state, receive_read_objects_result, NewCoordState#state{from = Sender}};

%% @doc Perform update operations on a batch of Objects
execute_command(update_objects, UpdateOps, Sender, State = #state{transaction=Transaction}) ->
    ExecuteUpdates = fun(Op, AccState=#state{
        client_ops = ClientOps0,
        updated_partitions = UpdatedPartitions0
    }) ->
        case perform_update(Op, UpdatedPartitions0, Transaction, Sender, ClientOps0) of
            {error, _} = Err ->
                AccState#state{return_accumulator = Err};

            {UpdatedPartitions, ClientOps} ->
                NumToRead = AccState#state.num_to_read,
                AccState#state{
                    client_ops=ClientOps,
                    num_to_read=NumToRead + 1,
                    updated_partitions=UpdatedPartitions
                }
        end
                     end,

    NewCoordState = lists:foldl(
        ExecuteUpdates,
        State#state{num_to_read=0, return_accumulator=ok},
        UpdateOps
    ),

    LoggingState = NewCoordState#state{from=Sender},
    case LoggingState#state.num_to_read > 0 of
        true ->
            {next_state, receive_logging_responses, LoggingState};
        false ->
            {next_state, receive_logging_responses, LoggingState, [{state_timeout, 0, timeout}]}
    end.


%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
reply_to_client(State = #state{
    from=From,
    state=TxState,
    is_static=IsStatic,
    client_ops=ClientOps,
    commit_time=CommitTime,
    transaction=Transaction,
    return_accumulator=ReturnAcc
}) ->
    TxId = Transaction#transaction.txn_id,
    _ = case From of
        undefined ->
            ok;

        Node ->

            Reply = case TxState of
                        committed_read_only ->
                            case IsStatic of
                                false ->
                                    {ok, {TxId, Transaction#transaction.vec_snapshot_time}};
                                true ->
                                    {ok, {TxId, ReturnAcc, Transaction#transaction.vec_snapshot_time}}
                            end;

                        committed ->
                            %% Execute post_commit_hooks
                            _Result = execute_post_commit_hooks(ClientOps),
                            %% TODO: What happens if commit hook fails?
                            DcId = ?DC_META_UTIL:get_my_dc_id(),
                            CausalClock = ?VECTORCLOCK:set(DcId, CommitTime, Transaction#transaction.vec_snapshot_time),
                            case IsStatic of
                                false ->
                                    {ok, {TxId, CausalClock}};
                                true ->
                                    {ok, CausalClock}
                            end;

                        aborted ->
                            ?STATS(transaction_aborted),
                            case ReturnAcc of
                                {error, Reason} ->
                                    {error, Reason};
                                _ ->
                                    {error, aborted}
                            end

                        %% can never match (dialyzer)
%%                        Reason ->
%%                            {TxId, Reason}
                    end,
            case is_pid(Node) of
                false ->
                    gen_statem:reply(Node, Reply);
                true ->
                    From ! Reply
            end
    end,

    % transaction is finished, decrement count
    ?STATS(transaction_finished),


    {stop, normal, State}.


%% @doc The following function is used to apply the updates that were performed by the running
%% transaction, to the result returned by a read.
-spec apply_tx_updates_to_snapshot(key(), state(), type(), snapshot()) -> snapshot().
apply_tx_updates_to_snapshot(Key, CoordState, Type, Snapshot)->
    Partition = ?LOG_UTIL:get_key_partition(Key),
    Found = lists:keyfind(Partition, 1, CoordState#state.updated_partitions),

    case Found of
        false ->
            Snapshot;

        {Partition, WS} ->
            FilteredAndReversedUpdates=clocksi_vnode:reverse_and_filter_updates_per_key(WS, Key),
            clocksi_materializer:materialize_eager(Type, Snapshot, FilteredAndReversedUpdates)
    end.


%%@doc Set the transaction Snapshot Time to the maximum value of:
%%     1.ClientClock, which is the last clock of the system the client
%%       starting this transaction has seen, and
%%     2.machine's local time, as returned by erlang:now().
-spec get_snapshot_time(snapshot_time()) -> {ok, snapshot_time()}.
get_snapshot_time(ClientClock) ->
    wait_for_clock(ClientClock).


-spec get_snapshot_time() -> {ok, snapshot_time()}.
get_snapshot_time() ->
    Now = dc_utilities:now_microsec() - ?OLD_SS_MICROSEC,
    {ok, VecSnapshotTime} = ?DC_UTIL:get_stable_snapshot(),
    DcId = ?DC_META_UTIL:get_my_dc_id(),
    SnapshotTime = vectorclock:set(DcId, Now, VecSnapshotTime),
    {ok, SnapshotTime}.


-spec wait_for_clock(snapshot_time()) -> {ok, snapshot_time()}.
wait_for_clock(Clock) ->
    {ok, VecSnapshotTime} = get_snapshot_time(),
    case vectorclock:ge(VecSnapshotTime, Clock) of
        true ->
            %% No need to wait
            {ok, VecSnapshotTime};
        false ->
            %% wait for snapshot time to catch up with Client Clock
            %TODO Refactor into constant
            timer:sleep(10),
            wait_for_clock(Clock)
    end.


%% Replaces the first occurrence of an entry;
%% yields error if there the element to be replaced is not in the list
replace_first([], _, _) ->
    error;

replace_first([Key|Rest], Key, NewKey) ->
    [NewKey|Rest];

replace_first([NotMyKey|Rest], Key, NewKey) ->
    [NotMyKey|replace_first(Rest, Key, NewKey)].


perform_read({Key, Type}, UpdatedPartitions, Transaction, Sender) ->
    ?STATS(operation_read),
    Partition = ?LOG_UTIL:get_key_partition(Key),

    WriteSet = case lists:keyfind(Partition, 1, UpdatedPartitions) of
                   false ->
                       [];
                   {Partition, WS} ->
                       WS
               end,

    case ?CLOCKSI_VNODE:read_data_item(Partition, Transaction, Key, Type, WriteSet) of
        {ok, Snapshot} ->
            Snapshot;

        {error, Reason} ->
            case Sender of
                undefined -> ok;
                _ -> gen_statem:reply(Sender, {error, Reason})
            end,
            {error, Reason}
    end.


perform_update(Op, UpdatedPartitions, Transaction, _Sender, ClientOps) ->
    ?STATS(operation_update),
    {Key, Type, Update} = Op,
    Partition = ?LOG_UTIL:get_key_partition(Key),

    WriteSet = case lists:keyfind(Partition, 1, UpdatedPartitions) of
                   false ->
                       [];
                   {Partition, WS} ->
                       WS
               end,

    %% Execute pre_commit_hook if any
    case antidote_hooks:execute_pre_commit_hook(Key, Type, Update) of
        {error, Reason} ->
            ?LOG_DEBUG("Execute pre-commit hook failed ~p", [Reason]),
            {error, Reason};

        {Key, Type, PostHookUpdate} ->

            %% Generate the appropriate state operations based on older snapshots
            GenerateResult = ?CLOCKSI_DOWNSTREAM:generate_downstream_op(
                Transaction,
                Partition,
                Key,
                Type,
                PostHookUpdate,
                WriteSet
            ),

            case GenerateResult of
                {error, Reason} ->
                    {error, Reason};

                {ok, DownstreamOp} ->
                    ok = async_log_propagation(Partition, Transaction#transaction.txn_id, Key, Type, DownstreamOp),

                    %% Append to the write set of the updated partition
                    GeneratedUpdate = {Key, Type, DownstreamOp},
                    NewUpdatedPartitions = append_updated_partitions(
                        UpdatedPartitions,
                        WriteSet,
                        Partition,
                        GeneratedUpdate
                    ),

                    UpdatedOps = [{Key, Type, PostHookUpdate} | ClientOps],
                    {NewUpdatedPartitions, UpdatedOps}
            end
    end.

%% @doc Add new updates to the write set of the given partition.
%%
%%      If there's no write set, create a new one.
%%
append_updated_partitions(UpdatedPartitions, [], Partition, Update) ->
    [{Partition, [Update]} | UpdatedPartitions];

append_updated_partitions(UpdatedPartitions, WriteSet, Partition, Update) ->
    %% Update the write set entry with the new record
    AllUpdates = {Partition, [Update | WriteSet]},
    lists:keyreplace(Partition, 1, UpdatedPartitions, AllUpdates).


-spec async_log_propagation(index_node(), txid(), key(), type(), effect()) -> ok.
async_log_propagation(Partition, TxId, Key, Type, Record) ->
    LogRecord = #log_operation{
        op_type=update,
        tx_id=TxId,
        log_payload=#update_log_payload{key=Key, type=Type, op=Record}
    },

    LogId = ?LOG_UTIL:get_logid_from_key(Key),
    ?LOGGING_VNODE:asyn_append(Partition, LogId, LogRecord, {fsm, undefined, self()}).


%% @doc this function sends a prepare message to all updated partitions and goes
%%      to the "receive_prepared"state.
-spec prepare(state()) -> gen_statem:event_handler_result(state()).
prepare(State = #state{
    num_to_read=NumToRead,
    full_commit=FullCommit,
    transaction=Transaction,
    updated_partitions = UpdatedPartitions,
    commit_protocol = CommitProtocol
}) ->
    case UpdatedPartitions of
        [] ->
            if
                CommitProtocol == two_phase orelse NumToRead == 0 -> %TODO explain this condition, it makes no sense
                    case FullCommit of
                        true ->
                            prepare_done(State, commit_read_only);
                        false ->
                            Transaction = State#state.transaction,
                            SnapshotTimeLocal = Transaction#transaction.snapshot_time_local,
                            prepare_done(State, {reply_and_then_commit, SnapshotTimeLocal})
                    end;
                true ->
                    {next_state, receive_prepared, State#state{state = prepared}}
            end;

        [_] when CommitProtocol /= two_phase ->
            prepare_done(State, single_committing);
        [_|_] ->
            ok = ?CLOCKSI_VNODE:prepare(UpdatedPartitions, Transaction),
            Num_to_ack = length(UpdatedPartitions),
            {next_state, receive_prepared, State#state{num_to_ack = Num_to_ack, state = prepared}}
    end.


%% This function is called when we are done with the prepare phase.
%% There are different options to continue the commit phase:
%% single_committing: special case for when we just touched a single partition
%% commit_read_only: special case for when we have not updated anything
%% {reply_and_then_commit, clock_time()}: first reply that we have successfully committed and then try to commit TODO rly?
%% {normal_commit, clock_time(): wait until all participants have acknowledged the commit and then reply to the client
-spec prepare_done(state(), Action) -> gen_statem:event_handler_result(state())
    when Action :: single_committing | commit_read_only | {reply_and_then_commit, clock_time()} | {normal_commit, clock_time()}.
prepare_done(State, Action) ->
    case Action of
        single_committing ->
            UpdatedPartitions = State#state.updated_partitions,
            Transaction = State#state.transaction,
            ok = ?CLOCKSI_VNODE:single_commit(UpdatedPartitions, Transaction),
            {next_state, single_committing, State#state{state = committing, num_to_ack = 1}};
        commit_read_only ->
            reply_to_client(State#state{state = committed_read_only});
        {reply_and_then_commit, CommitSnapshotTime} ->
            From = State#state.from,
            {next_state, committing, State#state{
                state = committing,
                commit_time = CommitSnapshotTime},
                [{reply, From, {ok, CommitSnapshotTime}}]};
        {normal_commit, MaxPrepareTime} ->
            UpdatedPartitions = State#state.updated_partitions,
            Transaction = State#state.transaction,
                        ok = ?CLOCKSI_VNODE:commit(UpdatedPartitions, Transaction, MaxPrepareTime),
            {next_state, receive_committed,
                            State#state{
                                num_to_ack = length(UpdatedPartitions),
                                commit_time = MaxPrepareTime,
                                state = committing}}
    end.




process_prepared(ReceivedPrepareTime, State = #state{num_to_ack = NumToAck,
    full_commit = FullCommit,
    prepare_time = PrepareTime}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of
        1 ->
            % this is the last ack we expected
            case FullCommit of
                true ->
                    prepare_done(State, {normal_commit, MaxPrepareTime});
                false ->
                    prepare_done(State, {reply_and_then_commit, MaxPrepareTime})
        end;
        _ ->
            {next_state, receive_prepared, State#state{num_to_ack = NumToAck - 1, prepare_time = MaxPrepareTime}}
    end.


%% @doc when an error occurs or an updated partition
%% does not pass the certification check, the transaction aborts.
abort(State = #state{transaction = Transaction,
    updated_partitions = UpdatedPartitions}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            reply_to_client(State#state{state = aborted});
        _ ->
            ok = ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
            {next_state, receive_aborted, State#state{num_to_ack = NumToAck, state = aborted}}
    end.


execute_post_commit_hooks(Ops) ->
    lists:foreach(fun({Key, Type, Update}) ->
        case antidote_hooks:execute_post_commit_hook(Key, Type, Update) of
            {error, Reason} ->
                ?LOG_INFO("Post commit hook failed. Reason ~p", [Reason]);
            _ -> ok
        end
                  end, lists:reverse(Ops)).

%%%===================================================================
%%% Unit Tests
%%%===================================================================

-ifdef(TEST).

main_test_() ->
    {foreach,
        fun setup/0,
        fun cleanup/1,
        [
            fun empty_prepare_test/1,
            fun timeout_test/1,

            fun update_single_abort_test/1,
            fun update_single_success_test/1,
            fun update_multi_abort_test1/1,
            fun update_multi_abort_test2/1,
            fun update_multi_success_test/1,

            fun read_single_fail_test/1,
            fun read_success_test/1,

            fun downstream_fail_test/1,
            fun get_snapshot_time_test/0,
            fun wait_for_clock_test/0
        ]}.

% Setup and Cleanup
setup() ->
    {ok, Pid} = clocksi_interactive_coord:start_link(),
    {ok, _Tx} = gen_statem:call(Pid, {start_tx, ignore, []}),
    Pid.

cleanup(Pid) ->
    case process_info(Pid) of undefined -> io:format("Already cleaned");
        _ -> clocksi_interactive_coord:stop(Pid) end.

empty_prepare_test(Pid) ->
    fun() ->
        ?assertMatch({ok, _}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

timeout_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {timeout, nothing, nothing}}, infinity)),
        ?assertMatch({error, aborted}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_single_abort_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({error, aborted}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_single_success_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {single_commit, nothing, nothing}}, infinity)),
        ?assertMatch({ok, _}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test1(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({error, aborted}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test2(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({error, aborted}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_multi_success_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertMatch({ok, _}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

read_single_fail_test(Pid) ->
    fun() ->
        ?assertEqual({error, mock_read_fail},
            gen_statem:call(Pid, {read, {read_fail, nothing}}, infinity))
    end.

read_success_test(Pid) ->
    fun() ->
        {ok, State} = gen_statem:call(Pid, {read, {counter, antidote_crdt_counter_pn}}, infinity),
        ?assertEqual({ok, 2},
            {ok, antidote_crdt_counter_pn:value(State)}),
        ?assertEqual({ok, [a]},
            gen_statem:call(Pid, {read, {set, antidote_crdt_set_go}}, infinity)),
        ?assertEqual({ok, mock_value},
            gen_statem:call(Pid, {read, {mock_type, mock_partition_fsm}}, infinity)),
        ?assertMatch({ok, _}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

downstream_fail_test(Pid) ->
    fun() ->
        ?assertMatch({error, _},
            gen_statem:call(Pid, {update, {downstream_fail, nothing, nothing}}, infinity))
    end.


get_snapshot_time_test() ->
    {ok, SnapshotTime} = get_snapshot_time(),
    ?assertMatch([{mock_dc, _}], vectorclock:to_list(SnapshotTime)).

wait_for_clock_test() ->
    {ok, SnapshotTime} = wait_for_clock(vectorclock:from_list([{mock_dc, 10}])),
    ?assertMatch([{mock_dc, _}], vectorclock:to_list(SnapshotTime)),
    VecClock = dc_utilities:now_microsec(),
    {ok, SnapshotTime2} = wait_for_clock(vectorclock:from_list([{mock_dc, VecClock}])),
    ?assertMatch([{mock_dc, _}], vectorclock:to_list(SnapshotTime2)).

-endif.
