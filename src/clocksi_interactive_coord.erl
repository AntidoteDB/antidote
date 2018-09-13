%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% @doc The coordinator for a given Clock SI interactive transaction.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. When a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(clocksi_interactive_coord).

-behavior(gen_statem).

-include("antidote.hrl").


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("lock_mgr_es.hrl").
-include("lock_mgr.hrl").
-define(LOCK_MGR,mock_partition).
-define(LOCK_MGR_ES,mock_partition).
-define(DC_META_UTIL, mock_partition).
-define(DC_UTIL, mock_partition).
-define(VECTORCLOCK, mock_partition).
-define(LOG_UTIL, mock_partition).
-define(CLOCKSI_VNODE, mock_partition).
-define(CLOCKSI_DOWNSTREAM, mock_partition).
-define(LOGGING_VNODE, mock_partition).
-define(PROMETHEUS_GAUGE, mock_partition).
-define(PROMETHEUS_COUNTER, mock_partition).


-else.
-include("lock_mgr_es.hrl").
-include("lock_mgr.hrl").
-define(LOCK_MGR,lock_mgr).
-define(LOCK_MGR_ES,lock_mgr_es).
-define(DC_META_UTIL, dc_meta_data_utilities).
-define(DC_UTIL, dc_utilities).
-define(VECTORCLOCK, vectorclock).
-define(LOG_UTIL, log_utilities).
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(CLOCKSI_DOWNSTREAM, clocksi_downstream).
-define(LOGGING_VNODE, logging_vnode).
-define(PROMETHEUS_GAUGE, prometheus_gauge).
-define(PROMETHEUS_COUNTER, prometheus_counter).
-endif.


%% API
-export([
    start_link/1,
    start_link/2,
    start_link/3,
    start_link/4,
    start_link/5,
    generate_name/1,
    perform_singleitem_operation/4,
    perform_singleitem_operation/5,
    perform_singleitem_update/5,
    finish_op/3
]).

%% gen_statem callbacks
-export([
    init/1,
    code_change/4,
    callback_mode/0,
    terminate/3,
    stop/1
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
    start_tx/3,
    wait_for_clock/1,
    committing_2pc/3,
    committing/3,
    committing_single/3,

    get_locks/3,
    get_locks/4,
    release_locks/2
]).

%% for lock manager
-export([
    get_locks_helper/4,
    get_locks_helper_es/5
]).

%%%===================================================================
%%% API
%%%===================================================================

%% used in cure.erl
-spec generate_name(pid()) -> atom().
generate_name(From) -> list_to_atom(pid_to_list(From) ++ "interactive_cord").

-spec start_link(pid(), clock_time() | ignore, txn_properties(), boolean(), [op_param()]) -> {ok, pid()}.
start_link(From, Clientclock, Properties, StayAlive, Operations) ->
    case StayAlive of
        true ->
            gen_statem:start_link({local, generate_name(From)}, ?MODULE, [From, Clientclock, Properties, StayAlive, Operations], []);
        false ->
            gen_statem:start_link(?MODULE, [From, Clientclock, Properties, StayAlive, Operations], [])
    end.

-spec start_link(pid(), clock_time() | ignore, txn_properties(), boolean()) -> {ok, pid()}.
start_link(From, Clientclock, Properties, StayAlive) ->
    case StayAlive of
        true ->
            gen_statem:start_link({local, generate_name(From)}, ?MODULE, [From, Clientclock, Properties, StayAlive], []);
        false ->
            gen_statem:start_link(?MODULE, [From, Clientclock, Properties, StayAlive], [])
    end.

-spec start_link(pid(), clock_time() | ignore) -> {ok, pid()}.
start_link(From, Clientclock) ->
    start_link(From, Clientclock, antidote:get_default_txn_properties()).

-spec start_link(pid(), clock_time() | ignore, txn_properties()) -> {ok, pid()}.
start_link(From, Clientclock, Properties) ->
    start_link(From, Clientclock, Properties, false).

-spec start_link(pid()) -> {ok, pid()}.
start_link(From) ->
    start_link(From, ignore, antidote:get_default_txn_properties()).

%% TODO spec
stop(Pid) ->
    gen_statem:stop(Pid).

%% @doc This is a standalone function for directly contacting the read
%%      server located at the vnode of the key being read.  This read
%%      is supposed to be light weight because it is done outside of a
%%      transaction fsm and directly in the calling thread.
%%      It either returns the object value or the object state.
-spec perform_singleitem_operation(snapshot_time() | ignore, key(), type(), clocksi_readitem_server:read_property_list()) ->
    {ok, val() | term(), snapshot_time()} | {error, reason()}.
perform_singleitem_operation(Clock, Key, Type, Properties) ->
    {Transaction, _TransactionId} = create_transaction_record(Clock, false, undefined, true, Properties),
    %%OLD: {Transaction, _TransactionId} = create_transaction_record(ignore, update_clock, false, undefined, true),
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case clocksi_readitem_server:read_data_item(IndexNode, Key, Type, Transaction, []) of
        {error, Reason} ->
            {error, Reason};
        {ok, Snapshot} ->
            %% Read only transaction has no commit, hence return the snapshot time
            CommitTime = Transaction#transaction.vec_snapshot_time,
            {ok, Snapshot, CommitTime}
    end.

perform_singleitem_operation(Clock, Key, Type, Operation, Properties) ->
    {Transaction, _TransactionId} = create_transaction_record(Clock, false, undefined, true, Properties),
    %%OLD: {Transaction, _TransactionId} = create_transaction_record(ignore, update_clock, false, undefined, true),
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case clocksi_readitem_server:read_data_item(IndexNode, Key, Type, Operation, Transaction, []) of
        {error, Reason} ->
            {error, Reason};
        {ok, Snapshot} ->
            %% Read only transaction has no commit, hence return the snapshot time
            CommitTime = Transaction#transaction.vec_snapshot_time,
            case Type:is_operation(Operation) of
                true ->
                    Value = Type:value(Operation, Snapshot),
                    {ok, Snapshot, Value, CommitTime};
                false ->
                    {error, {function_not_supported, Operation}}
            end
    end.

%% @doc This is a standalone function for directly contacting the update
%%      server vnode.  This is lighter than creating a transaction
%%      because the update/prepare/commit are all done at one time
-spec perform_singleitem_update(snapshot_time() | ignore, key(), type(), {atom(), term()}, list()) -> {ok, {txid(), [], snapshot_time()}} | {error, term()}.
perform_singleitem_update(Clock, Key, Type, Params, Properties) ->
    {Transaction, _TransactionId} = create_transaction_record(Clock, false, undefined, true, Properties),

    %% Execute pre_commit_hook if any
    case antidote_hooks:execute_pre_commit_hook(Key, Type, Params) of
        PostHookUpdates when is_list(PostHookUpdates) ->
            UpdatedPartitions = lists:foldl(fun({Key1, Type1, Params1}, AccUpdatedPartitions) ->
                case AccUpdatedPartitions of
                    {error, _} -> AccUpdatedPartitions;
                    _Else ->
                        Partition = ?LOG_UTIL:get_key_partition(Key1),
                        case ?CLOCKSI_DOWNSTREAM:generate_downstream_op(Transaction, Partition, Key1, Type1, Params1, [], []) of
                            {ok, DownstreamRecord} ->
                                TxId = Transaction#transaction.txn_id,
                                LogRecord = #log_operation{
                                    tx_id=TxId,
                                    op_type=update,
                                    log_payload=#update_log_payload{key=Key1, type=Type1, op=DownstreamRecord}
                                },

                                LogId = ?LOG_UTIL:get_logid_from_key(Key1),
                                case ?LOGGING_VNODE:append(Partition, LogId, LogRecord) of
                                    {ok, _} ->
                                        lists:append(AccUpdatedPartitions, [{Partition, [{Key1, Type1, DownstreamRecord}]}]);
                                    Error ->
                                        {error, Error}
                                end;
                            {error, Reason} ->
                                {error, Reason}
                        end
                end
            end, [], PostHookUpdates),

            case UpdatedPartitions of
                {error, Reason} ->
                    {error, Reason};
                UpdatedPartitions ->
                    case ?CLOCKSI_VNODE:single_commit_sync(UpdatedPartitions, Transaction) of
                        {committed, CommitTime} ->

                            %% Execute post commit hooks
                            lists:foreach(fun({Key1, Type1, Params1}) ->
                                case antidote_hooks:execute_post_commit_hook(Key1, Type1, Params1) of
                                    {error, Reason} ->
                                        lager:info("Post commit hook failed. Reason ~p", [Reason]);
                                    _ ->
                                        ok
                                end
                            end, PostHookUpdates),

                            TxId = Transaction#transaction.txn_id,
                            DcId = ?DC_META_UTIL:get_my_dc_id(),

                            CausalClock = ?VECTORCLOCK:set_clock_of_dc(
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
                    end
            end;

        {Key, Type, Params1} ->
            Partition = ?LOG_UTIL:get_key_partition(Key),
            case ?CLOCKSI_DOWNSTREAM:generate_downstream_op(Transaction, Partition, Key, Type, Params1, [], []) of
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
                                            lager:info("Post commit hook failed. Reason ~p", [Reason]);
                                        _ ->
                                            ok
                                    end,

                                    TxId = Transaction#transaction.txn_id,
                                    DcId = ?DC_META_UTIL:get_my_dc_id(),

                                    CausalClock = ?VECTORCLOCK:set_clock_of_dc(
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
%%                number of partitions that have acked.
%%    num_to_read: when sending read requests
%%                 number of partitions that are asked.
%%    prepare_time: transaction prepare time.
%%    commit_time: transaction commit time.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------

-record(coord_state, {
    from :: undefined | {pid(), term()} | pid(),
    transaction :: undefined | tx(),
    updated_partitions :: list(),
    client_ops :: list(), % list of upstream updates, used for post commit hooks
    num_to_ack :: non_neg_integer(),
    num_to_read :: non_neg_integer(),
    prepare_time :: clock_time(),
    commit_time :: undefined | clock_time(),
    commit_protocol :: term(),
    state :: active | prepared | committing
    | committed | committed_read_only
    | undefined | aborted,
    operations :: undefined | list() | {update_objects, list()},
    return_accumulator :: list() | ok | {error, reason()},
    internal_read_set :: orddict:orddict(),
    is_static :: boolean(),
    full_commit :: boolean(),
    properties :: txn_properties(),
    stay_alive :: boolean(),
    transactionid :: txid()   % #Locks
}).

%%%===================================================================
%%% States
%%%===================================================================

%%%== init

%% @doc Initialize the state.
%% #Locks
init([From, ClientClock, Properties, StayAlive]) ->
    BaseState = init_state(StayAlive, false, false, Properties),

    Locks = lists:keyfind(locks,1,Properties),
    Shared_Locks = lists:keyfind(shared_locks,1,Properties),
    Exclusive_Locks = lists:keyfind(exclusive_locks,1,Properties),
    State = case {Locks,Shared_Locks,Exclusive_Locks} of
        {false,false,false} ->
            start_tx_internal(From, ClientClock, Properties, BaseState);
        {{locks,A},{shared_locks,B},{exclusive_locks,C}} when (is_list(A) and is_list(B) and is_list(C))  ->
            case ((A ++ B ++ C) == []) of
                false ->
                    start_tx_internal_with_locks(From, ClientClock, Properties, BaseState,A,B,C);
                true ->
                    start_tx_internal(From, ClientClock, Properties, BaseState)
            end;
        {false,{shared_locks,B},{exclusive_locks,C}} when (is_list(B) and is_list(C))  ->
            case ((B ++ C) == []) of
                false ->
                    start_tx_internal_with_locks(From, ClientClock, Properties, BaseState,[],B,C);
                true ->
                    start_tx_internal(From, ClientClock, Properties, BaseState)
            end;
        {{locks,A},false,{exclusive_locks,C}} when (is_list(A) and is_list(C))  ->
            case ((A ++ C) == []) of
                false ->
                    start_tx_internal_with_locks(From, ClientClock, Properties, BaseState,A,[],C);
                true ->
                    start_tx_internal(From, ClientClock, Properties, BaseState)
            end;
        {{locks,A},{shared_locks,B},false} when (is_list(A) and is_list(B))  ->
            case ((A ++ B) == []) of
                false ->
                    start_tx_internal_with_locks(From, ClientClock, Properties, BaseState,A,B,[]);
                true ->
                    start_tx_internal(From, ClientClock, Properties, BaseState)
            end;
        {{locks,A},false,false} when (is_list(A))  ->
            case ((A) == []) of
                false ->
                    start_tx_internal_with_locks(From, ClientClock, Properties, BaseState,A,[],[]);
                true ->
                    start_tx_internal(From, ClientClock, Properties, BaseState)
            end;
        {false,{shared_locks,B},false} when (is_list(B))  ->
            case ((B) == []) of
                false ->
                    start_tx_internal_with_locks(From, ClientClock, Properties, BaseState,[],B,[]);
                true ->
                    start_tx_internal(From, ClientClock, Properties, BaseState)
            end;
        {false,false,{exclusive_locks,C}} when (is_list(C))  ->
            case ((C) == []) of
                false ->
                    start_tx_internal_with_locks(From, ClientClock, Properties, BaseState,[],[],C);
                true ->
                    start_tx_internal(From, ClientClock, Properties, BaseState)
            end
    end,
    {ok, execute_op, State};

%% @doc Initialize static transaction with Operations.
%% #Locks
init([From, ClientClock, Properties, StayAlive, Operations]) ->
    BaseState = init_state(StayAlive, true, true, Properties),

    Locks = lists:keyfind(locks,1,Properties),
    Shared_Locks = lists:keyfind(shared_locks,1,Properties),
    Exclusive_Locks = lists:keyfind(exclusive_locks,1,Properties),
    State = if
        ((Locks == false) and (Shared_Locks == false) and (Exclusive_Locks == false)) ->
            start_tx_internal(From, ClientClock, Properties, BaseState);
        true ->
            Locks1 = case Locks of false -> []; {locks,A} -> A end,
            Shared_Locks1 = case Shared_Locks of false -> []; {shared_locks,B} -> B end,
            Exclusive_Locks1 = case Exclusive_Locks of false -> []; {exclusive_locks,C} -> C end,
            start_tx_internal_with_locks(From, ClientClock, Properties, BaseState,Locks1,Shared_Locks1,Exclusive_Locks1)
    end,
    {ok, execute_op, State#coord_state{operations = Operations, from = From}, [{state_timeout, 0, timeout}]}.


%%%== execute_op

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
%% internal state timeout
execute_op(state_timeout, timeout, State = #coord_state{operations = Operations, from = From}) ->
    execute_op({call, From}, Operations, State);

%% update kept for backwards compatibility with tests.
execute_op({call, Sender}, {update, Args}, State) ->
    execute_op({call, Sender}, {update_objects, [Args]}, State);

execute_op({call, Sender}, {OpType, Args}, State) ->
    case execute_command(OpType, Args, Sender, State) of
        {committing_2pc, Data, Actions = [{reply, _, _}]} ->
            {next_state, committing_2pc, Data, Actions};
        {receive_prepared, Data} ->
            {next_state, receive_prepared, Data};
        {start_tx, Data} ->
            {next_state, start_tx, Data};
        {committing, Data, Actions = [{reply, _, _}]} ->
            {next_state, committing, Data, Actions};
        {single_committing, Data} ->
            {next_state, single_committing, Data};
        {receive_aborted, Data} ->
            {next_state, receive_aborted, Data};
        {receive_read_objects_result, Data} ->
            {next_state, receive_read_objects_result, Data};
        {receive_logging_responses, Data} ->
            {next_state, receive_logging_responses, Data};
        {receive_logging_responses, Data, 0} ->
            {next_state, receive_logging_responses, Data, [{state_timeout, 0, timeout}]};

        {Reply, execute_op, Data} ->
            {next_state, execute_op, Data, [{reply, Sender, Reply}]};

        {stop, normal, Data} ->
            {stop, normal, Data}
    end.

%%%== committing_2pc

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state expects other process to sen the commit message to
%%      start the commit phase.
committing_2pc({call, Sender}, commit, State = #coord_state{transaction = Transaction,
    updated_partitions = UpdatedPartitions,
    commit_time = CommitTime}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            case reply_to_client(State#coord_state{state = committed_read_only, from = Sender}) of
                {start_tx, Data} -> {next_state, start_tx, Data};
                {stop, normal, Data} -> {stop, normal, Data}
            end;
        _ ->
            ok = ?CLOCKSI_VNODE:commit(UpdatedPartitions, Transaction, CommitTime),
            {next_state, receive_committed, State#coord_state{num_to_ack = NumToAck, from = Sender, state = committing}}
    end.

%%%== receive_prepared

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared(cast, {prepared, ReceivedPrepareTime}, State) ->
    case process_prepared(ReceivedPrepareTime, State) of
        {receive_committed, Data} ->
            {next_state, receive_committed, Data};
        {receive_prepared, Data} ->
            {next_state, receive_prepared, Data};
        {committing_2pc, Data, Actions = [{reply, _, _}]} ->
            {next_state, committing_2pc, Data, Actions};
        {committing, Data, Actions = [{reply, _, _}]} ->
            {next_state, committing, Data, Actions}
    end;

receive_prepared(cast, abort, State) ->
    receive_prepared(cast, timeout, State);

receive_prepared(cast, timeout, State) ->
    case abort(State) of
        {receive_aborted, NewState} -> {next_state, receive_aborted, NewState};
        {start_tx, NewState} -> {next_state, start_tx, NewState};
        {stop, normal, Data} -> {stop, normal, Data}
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_prepared(info, {_EventType, EventValue}, State) ->
    receive_prepared(cast, EventValue, State).


%%%== start_tx

%% @doc TODO
start_tx(cast, {start_tx, From, ClientClock, Properties}, State) ->
    {next_state, execute_op, start_tx_internal(From, ClientClock, Properties, State)};

%% Used by static update and read transactions
start_tx(cast, {start_tx, From, ClientClock, Properties, Operation}, State) ->
    {next_state, execute_op, start_tx_internal(From, ClientClock, Properties,
        State#coord_state{is_static = true, operations = Operation, from = From}), [{state_timeout, 0, timeout}]};

%% capture regular events (e.g. logging_vnode responses)
start_tx(info, {_EventType, EventValue}, State) ->
    start_tx(cast, EventValue, State).


%%%== committing

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected
committing({call, Sender}, commit, State = #coord_state{transaction = Transaction,
    updated_partitions = UpdatedPartitions,
    commit_time = Commit_time}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            case reply_to_client(State#coord_state{state = committed_read_only, from = Sender}) of
                {start_tx, Data} -> {next_state, start_tx, Data};
                {stop, normal, Data} -> {stop, normal, Data}
            end;
        _ ->
            ok = ?CLOCKSI_VNODE:commit(UpdatedPartitions, Transaction, Commit_time),
            {next_state, receive_committed,
                State#coord_state{num_to_ack = NumToAck, from = Sender, state = committing}}
    end.

%%%== single_committing

%% @doc TODO
single_committing(cast, {committed, CommitTime}, State = #coord_state{from = From, full_commit = FullCommit}) ->
    case FullCommit of
        false ->
            {next_state, committing_single, State#coord_state{commit_time = CommitTime, state = committing},
                [{reply, From, {ok, CommitTime}}]};
        true ->
            case reply_to_client(State#coord_state{prepare_time = CommitTime, commit_time = CommitTime, state = committed}) of
                {start_tx, Data} -> {next_state, start_tx, Data};
                {stop, normal, Data} -> {stop, normal, Data}
            end
    end;

single_committing(cast, abort, State) ->
    single_committing(cast, timeout, State);

single_committing(cast, timeout, State) ->
    case abort(State) of
        {receive_aborted, NewState} -> {next_state, receive_aborted, NewState};
        {start_tx, NewState} -> {next_state, start_tx, NewState};
        {stop, normal, Data} -> {stop, normal, Data}
    end;

%% capture regular events (e.g. logging_vnode responses)
single_committing(info, {_EventType, EventValue}, State) ->
    single_committing(cast, EventValue, State).


%%%== receive_aborted

%% @doc the fsm waits for acks indicating that each partition has successfully
%%      aborted the tx and finishes operation.
%%      Should we retry sending the aborted message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_aborted(cast, ack_abort, State = #coord_state{num_to_ack = NumToAck}) ->
    case NumToAck of
        1 ->
            case reply_to_client(State#coord_state{state = aborted}) of
                {start_tx, Data} -> {next_state, start_tx, Data};
                {stop, normal, Data} -> {stop, normal, Data}
            end;
        _ ->
            {next_state, receive_aborted, State#coord_state{num_to_ack = NumToAck - 1}}
    end;

receive_aborted(cast, _, State) -> {next_state, receive_aborted, State};

%% capture regular events (e.g. logging_vnode responses)
receive_aborted(info, {_EventType, EventValue}, State) ->
    receive_aborted(cast, EventValue, State).


%%%== receive_read_objects_result

%% @doc After asynchronously reading a batch of keys, collect the responses here
receive_read_objects_result(cast, {ok, Response}, CoordState = #coord_state{
    num_to_read = NumToRead,
    return_accumulator = ReadKeys,
    internal_read_set = ReadSet
}) ->
    %% TODO: type is hard-coded..

    {ReqNum, Key, Type, Snapshot, Operation} =
        case Response of
            {N, K, T, F, S} -> {N, K, T, S, F};
            {N, K, T, S} -> {N, K, T, S, undefined}
        end,

    UpdatedSnapshot = apply_tx_updates_to_snapshot(Key, CoordState, Type, Snapshot),
    {ReadValues, NewReadSet} =
        case Operation of
            undefined ->
                %% Swap keys with their appropriate read values
                {replace_first(ReadKeys, Key, {{ReqNum, Key}, {state, UpdatedSnapshot}}),
                    orddict:store(Key, UpdatedSnapshot, ReadSet)};
            Operation ->
                case Type:is_operation(Operation) of
                    true ->
                        Value = Type:value(Operation, UpdatedSnapshot),
                        {replace_first(ReadKeys, Key, {{ReqNum, Key}, {value, Value}}),
                            orddict:store(Key, UpdatedSnapshot, ReadSet)};
                    false ->
                        ErrorMsg = {error, {function_not_supported, Operation}},
                        {replace_first(ReadKeys, Key, {{ReqNum, Key}, {value, ErrorMsg}}),
                            orddict:store(Key, UpdatedSnapshot, ReadSet)}
                end
        end,

    %% Loop back to the same state until we process all the replies
    case NumToRead > 1 of
        true ->
            {next_state, receive_read_objects_result, CoordState#coord_state{
                num_to_read = NumToRead - 1,
                return_accumulator = ReadValues,
                internal_read_set = NewReadSet
            }};

        false ->
            {next_state, execute_op, CoordState#coord_state{num_to_read = 0, internal_read_set = NewReadSet},
                [{reply, CoordState#coord_state.from, {ok, lists:reverse(ReadValues)}}]}
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
receive_logging_responses(cast, Response, State = #coord_state{
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
            {next_state, receive_logging_responses, State#coord_state{
                num_to_read=NumToReply - 1,
                return_accumulator=NewAcc
            }};

        false ->
            case NewAcc of
                ok ->
                    case IsStatic of
                        true ->
                            case prepare(State) of
                                {receive_prepared, Data} ->
                                    {next_state, receive_prepared, Data};
                                {start_tx, Data} ->
                                    {next_state, start_tx, Data};
                                {committing, Data, Actions = [{reply, _, _}]} ->
                                    {next_state, committing, Data, Actions};
                                {single_committing, Data} ->
                                    {next_state, single_committing, Data};
                                {stop, normal, Data} ->
                                    {stop, normal, Data}
                            end;
                        false ->
                            {next_state, execute_op, State#coord_state{num_to_read=0, return_accumulator=[]},
                                [{reply, State#coord_state.from, NewAcc}]}
                    end;

                _ ->
                    case abort(State) of
                        {receive_aborted, NewState} -> {next_state, receive_aborted, NewState};
                        {start_tx, NewState} -> {next_state, start_tx, NewState};
                        {stop, normal, Data} -> {stop, normal, Data}
                    end
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
receive_committed(cast, committed, State = #coord_state{num_to_ack = NumToAck}) ->
    case NumToAck of
        1 ->
            case reply_to_client(State#coord_state{state = committed}) of
                {start_tx, Data} -> {next_state, start_tx, Data};
                {stop, normal, Data} -> {stop, normal, Data}
            end;
        _ ->
            {next_state, receive_committed, State#coord_state{num_to_ack = NumToAck - 1}}
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_committed(info, {_EventType, EventValue}, State) ->
    receive_committed(cast, EventValue, State).


%%%== committing_single

%% @doc There was only a single partition with an update in this transaction
%%      so the transaction has already been committed
%%      so just wait for the commit message from the client
committing_single({call, Sender}, commit, State = #coord_state{commit_time = Commit_time}) ->
    case reply_to_client(State#coord_state{
        prepare_time = Commit_time,
        from = Sender,
        commit_time = Commit_time,
        state = committed
    }) of
        {start_tx, Data} -> {next_state, start_tx, Data};
        {stop, normal, Data} -> {stop, normal, Data}
    end.

%% =============================================================================

%% TODO add to all state functions
%%handle_sync_event(stop, _From, _StateName, StateData) -> {stop, normal, ok, StateData}.

%%handle_call(From, stop, Data) ->
%%    {stop_and_reply, normal,  {reply, From, ok}, Data}.
%%
%%handle_info(Info, StateName, Data) ->
%%    {stop, {shutdown, {unexpected, Info, StateName}}, StateName, Data}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

%% #Locks
%% Release the locks when the transaction terminates
terminate(_Reason, _SN = #coord_state{properties = Properties, transactionid = TransactionId}, _SD) ->

    Locks = lists:keyfind(locks,1,Properties),
    case Locks of
        false -> ok;
        {locks,_Locks} ->
            ?LOCK_MGR:release_locks(TransactionId),
            ok
    end,
    Shared_Locks = lists:keyfind(shared_locks,1,Properties),
    Exclusive_Locks = lists:keyfind(exclusive_locks,1,Properties),
    case {Shared_Locks,Exclusive_Locks} of
        {false,false} -> ok;
        _ ->
            ?LOCK_MGR_ES:release_locks(TransactionId),
            ok
    end;
terminate(_Reason, _SN, _SD) -> ok.
callback_mode() -> state_functions.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc TODO
%% #Locks
init_state(StayAlive, FullCommit, IsStatic, Properties) ->
    #coord_state{
        transaction = undefined,
        updated_partitions = [],
        client_ops = [],
        prepare_time = 0,
        num_to_read = 0,
        num_to_ack = 0,
        operations = undefined,
        from = undefined,
        full_commit = FullCommit,
        is_static = IsStatic,
        return_accumulator = [],
        internal_read_set = orddict:new(),
        stay_alive = StayAlive,
        properties = Properties,
        transactionid = 0
    }.
%% @doc TODO
%% #Locks
%% Does the same is start_tx_internal with the addition of requesting the locks,shared_locks and/or exclusive_locks from lock_mgr and lock_mgr_es respectively.
start_tx_internal_with_locks(From, ClientClock, Properties, State = #coord_state{stay_alive = StayAlive, is_static = IsStatic},Locks,Shared_Locks,Exclusive_Locks) ->
    case create_transaction_record_with_locks(ClientClock, StayAlive, From, false, Properties, Locks,Shared_Locks,Exclusive_Locks) of
        {ok,Transaction,TransactionId} ->
            case IsStatic of
                true ->
                    ok;
                false ->
                    From ! {ok, TransactionId}
            end,
            % a new transaction was started, increment metrics
            ?PROMETHEUS_GAUGE:inc(antidote_open_transactions),
            State#coord_state{transaction = Transaction, num_to_read = 0, properties = Properties, transactionid = TransactionId};
        {locks_not_available,Missing_Locks} ->    % TODO is this the right way to abort the transaction if it was not possible to aquire the locks
            From ! {error,Missing_Locks},
            {stop, "Missing Locks: "++lists:flatten(io_lib:format("~p",[Missing_Locks]))};
        {locks_in_use,Tx_Using_The_Locks} ->    % TODO is this the right way to abort the transaction if it was not possible to aquire the locks
            From ! {error,Tx_Using_The_Locks},
            {stop, "Transaction using the locks: "++lists:flatten(io_lib:format("~p",[Tx_Using_The_Locks]))};   %%TODO
        {lock_not_available_or_in_use,Error_Info}->
            From ! {error,Error_Info},
            {stop, "Transaction using the locks ore are missing: "++lists:flatten(io_lib:format("~p",[Error_Info]))}   %%TODO
    end.


%% @doc This function tries to aquire the specified locks, if this fails it will retry after GET_LOCKS_INTERVALms
%% for a maximum number of times (Timeout div GET_LOCKS_INTERVAL).
%% #Locks
-spec get_locks(non_neg_integer(),txid(),[key()]) -> {ok,snapshot_time()} | {locks_not_available,[key()]} | {missing_locks, [{txid(),[key()]}]}.
get_locks(Timeout,TransactionId,Locks) ->
    Result = ?LOCK_MGR:get_locks(Locks, TransactionId),
    case Result of
        {ok,Snapshot_Time} -> {ok,Snapshot_Time};
        {locks_in_use,Tx_Using_The_Locks} ->
            case Timeout > 0 of
                true ->
                    timer:sleep(?GET_LOCKS_INTERVAL),
                    NewTimeout1 = Timeout-1,
                    get_locks(NewTimeout1, TransactionId, Locks);
                false ->
                    case ?GET_LOCKS_FINAL_TRY_OPTION_ES of
                        true -> get_locks_extra(TransactionId, Locks);
                        false-> {locks_in_use,Tx_Using_The_Locks}
                    end
            end;
        {missing_locks, Missing_Locks} ->
            case Timeout > 0 of
                true ->
                    timer:sleep(?GET_LOCKS_INTERVAL),
                    NewTimeout2 = Timeout-1,
                    get_locks(NewTimeout2, TransactionId, Locks);
                false ->
                    case ?GET_LOCKS_FINAL_TRY_OPTION_ES of
                        true -> get_locks_extra(TransactionId, Locks);
                        false-> {locks_not_available,Missing_Locks}
                    end
            end
    end.
get_locks_extra(TransactionId,Locks)->
    timer:sleep(?GET_LOCKS_FINAL_TRY_WAIT),
    Result = ?LOCK_MGR:get_locks(Locks, TransactionId),
    case Result of
        {ok,Snapshot_Time} -> {ok,Snapshot_Time};
        {locks_in_use,Tx_Using_The_Locks} ->
            {locks_in_use,Tx_Using_The_Locks};
        {missing_locks, Missing_Locks} ->
            {locks_not_available,Missing_Locks}
    end.
get_locks_extra(TransactionId,Shared_Locks,Exclusive_Locks)->
    timer:sleep(?GET_LOCKS_FINAL_TRY_WAIT_ES),
    Result = ?LOCK_MGR_ES:get_locks(Shared_Locks,Exclusive_Locks, TransactionId),
    case Result of
        {ok,Snapshot_Time} -> {ok,Snapshot_Time};
        {locks_in_use,Tx_Using_The_Locks} ->
            {locks_in_use,Tx_Using_The_Locks};
        {missing_locks, Missing_Locks} ->
            {locks_not_available,Missing_Locks}
    end.

%% @doc This function tries to aquire the specified exclusive and hared locks, if this fails it will retry after GET_LOCKS_INTERVALms
%% for a maximum number of times (Timeout div GET_LOCKS_INTERVAL).
%% #Locks
-spec get_locks(non_neg_integer(),txid(),[key()],[key()]) -> {ok,snapshot_time()} | {locks_not_available,[key()]} | {missing_locks, [{txid(),[key()],[key()]}]}.
get_locks(Timeout,TransactionId,Shared_Locks,Exclusive_Locks) ->
    Result = ?LOCK_MGR_ES:get_locks(Shared_Locks,Exclusive_Locks, TransactionId),
    case Result of
        {ok,Snapshot_Time} -> {ok,Snapshot_Time};
        {locks_in_use,Tx_Using_The_Locks} ->
            case Timeout > 0 of
                true ->
                    timer:sleep(?GET_LOCKS_INTERVAL_ES),
                    NewTimeout1 = Timeout-1,
                    get_locks(NewTimeout1, TransactionId, Shared_Locks,Exclusive_Locks);
                false ->
                    case ?GET_LOCKS_FINAL_TRY_OPTION_ES of
                        true -> get_locks_extra(TransactionId,Shared_Locks,Exclusive_Locks);
                        false-> {locks_in_use,Tx_Using_The_Locks}
                    end
            end;
        {missing_locks, Missing_Locks} ->
            case Timeout > 0 of
                true ->
                    timer:sleep(?GET_LOCKS_INTERVAL_ES),
                    NewTimeout2 = Timeout-1,
                    get_locks(NewTimeout2, TransactionId, Shared_Locks,Exclusive_Locks);
                false ->
                    case ?GET_LOCKS_FINAL_TRY_OPTION_ES of
                        true -> get_locks_extra(TransactionId,Shared_Locks,Exclusive_Locks);
                        false-> {locks_not_available,Missing_Locks}
                    end
            end
    end.



get_locks_helper(Timeout, TransactionId,Locks,Caller) ->
    Return_Value = get_locks(Timeout, TransactionId, Locks),
    Caller ! {locks,Return_Value}.
get_locks_helper_es(Timeout, TransactionId,Shared_Locks,Exclusive_Locks,Caller) ->
    Return_Value2 = get_locks(Timeout, TransactionId, Shared_Locks,Exclusive_Locks),
    Caller ! {es_locks,Return_Value2}.

-spec release_locks(locks | es_locks, txid()) -> ok.
release_locks(locks, TransactionId) ->
    ?LOCK_MGR:release_locks(TransactionId);
release_locks(es_locks, TransactionId) ->
    ?LOCK_MGR_ES:release_locks(TransactionId).

%% @doc TODO
%%noinspection ErlangUnresolvedFunction
%% #Locks
create_transaction_record_with_locks(ClientClock, StayAlive, From, _IsStatic, Properties,Locks,Shared_Locks,Exclusive_Locks) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    _Res = rand_compat:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
    {ok, SnapshotTime} = case ClientClock of
                             ignore ->
                                 get_snapshot_time();
                             _ ->
                                 {ok,ClientClock}
                         end,
    DcId = ?DC_META_UTIL:get_my_dc_id(),
    LocalClock = ?VECTORCLOCK:get_clock_of_dc(DcId, SnapshotTime),
    Name = case StayAlive of
               true ->
                   generate_name(From);
               false ->
                   self()
           end,
    TransactionId = #tx_id{local_start_time = LocalClock, server_pid = Name},
    case {Locks,Shared_Locks,Exclusive_Locks} of
        {_,[],[]}->
            case get_locks(?How_LONG_TO_WAIT_FOR_LOCKS, TransactionId, Locks) of
                {ok,Snapshots} ->
                    % Get the maximum snapshot of this dc and all locks in use
                    New_Snapshot = vectorclock:max([SnapshotTime|Snapshots]),
                    wait_for_clock(New_Snapshot),
                    Transaction = #transaction{snapshot_time = LocalClock,
                        vec_snapshot_time = New_Snapshot,
                        txn_id = TransactionId,
                        properties = Properties},
                    {ok,Transaction, TransactionId};
                {locks_not_available,Missing_Locks} ->
                    {locks_not_available,Missing_Locks};
                {locks_in_use,Tx_Using_The_Locks} ->
                    {locks_in_use,Tx_Using_The_Locks}
            end;
        {[],_,_}->
            case get_locks(?How_LONG_TO_WAIT_FOR_LOCKS_ES, TransactionId, Shared_Locks,Exclusive_Locks) of
                {ok,Snapshots} ->
                    % Get the maximum snapshot of this dc and all locks in use
                    New_Snapshot = vectorclock:max([SnapshotTime|Snapshots]),
                    wait_for_clock(New_Snapshot),
                    Transaction = #transaction{snapshot_time = LocalClock,
                        vec_snapshot_time = New_Snapshot,
                        txn_id = TransactionId,
                        properties = Properties},
                    {ok,Transaction, TransactionId};
                {locks_not_available,Missing_Locks} ->
                    {locks_not_available,Missing_Locks};
                {locks_in_use,Tx_Using_The_Locks} ->
                    {locks_in_use,Tx_Using_The_Locks}
            end;
        _->
            spawn(clocksi_interactive_coord,get_locks_helper,[?How_LONG_TO_WAIT_FOR_LOCKS, TransactionId, Locks, self()]),
            spawn(clocksi_interactive_coord,get_locks_helper_es,[?How_LONG_TO_WAIT_FOR_LOCKS_ES, TransactionId, Shared_Locks, Exclusive_Locks, self()]),
            Result1 = receive
                {locks,Result_1} ->
                    Result_1
            end,
            Result2 = receive
                {es_locks,Result_2}->
                    Result_2
            end,
            case {Result1,Result2} of
                {{ok,Snapshots1},{ok,Snapshots2}} ->
                    % Get the maximum snapshot of this dc and all locks in use
                    New_Snapshot = vectorclock:max([SnapshotTime|Snapshots1]++Snapshots2),
                    wait_for_clock(New_Snapshot),
                    Transaction = #transaction{snapshot_time = LocalClock,
                        vec_snapshot_time = New_Snapshot,
                        txn_id = TransactionId,
                        properties = Properties},
                    {ok,Transaction, TransactionId};
                {{locks_not_available,Missing_Locks},{ok,_}} ->
                    ?LOCK_MGR_ES:release_locks(TransactionId),
                    {locks_not_available,Missing_Locks};
                {{locks_in_use,Tx_Using_The_Locks},{ok,_}} ->
                    ?LOCK_MGR_ES:release_locks(TransactionId),
                    {locks_in_use,Tx_Using_The_Locks};
                {{ok,_},{locks_not_available,Missing_Locks}} ->
                    ?LOCK_MGR:release_locks(TransactionId),
                    {locks_not_available,Missing_Locks};
                {{ok,_},{locks_in_use,Tx_Using_The_Locks}} ->
                    ?LOCK_MGR:release_locks(TransactionId),
                    {locks_in_use,Tx_Using_The_Locks};
                {{_,Error_Info1},{_,Error_Info2}}->
                    {lock_not_available_or_in_use,{Error_Info1,Error_Info2}}

            end
    end.



%% @doc TODO
start_tx_internal(From, ClientClock, Properties, State = #coord_state{stay_alive = StayAlive, is_static = IsStatic}) ->
    {Transaction, TransactionId} = create_transaction_record(ClientClock, StayAlive, From, false, Properties),
    case IsStatic of
        true -> ok;
        false -> From ! {ok, TransactionId}
    end,
    % a new transaction was started, increment metrics
    ?PROMETHEUS_GAUGE:inc(antidote_open_transactions),
    State#coord_state{transaction = Transaction, num_to_read = 0, properties = Properties}.


%% @doc TODO
-spec create_transaction_record(snapshot_time() | ignore,
    boolean(), pid() | undefined, boolean(), txn_properties()) -> {tx(), txid()}.
%%noinspection ErlangUnresolvedFunction
create_transaction_record(ClientClock, StayAlive, From, _IsStatic, Properties) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    _Res = rand_compat:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
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
    LocalClock = ?VECTORCLOCK:get_clock_of_dc(DcId, SnapshotTime),
    Name = case StayAlive of
               true ->
                   generate_name(From);
               false ->
                   self()
           end,
    TransactionId = #tx_id{local_start_time = LocalClock, server_pid = Name},
    Transaction = #transaction{snapshot_time = LocalClock,
        vec_snapshot_time = SnapshotTime,
        txn_id = TransactionId,
        properties = Properties},
    {Transaction, TransactionId}.


%% @doc Execute the commit protocol
execute_command(prepare, Protocol, Sender, State0) ->
    State = State0#coord_state{from=Sender, commit_protocol=Protocol},
    case Protocol of
        two_phase ->
            prepare_2pc(State);
        _ ->
            prepare(State)
    end;

%% @doc Abort the current transaction
execute_command(abort, _Protocol, Sender, State) ->
    abort(State#coord_state{from=Sender});

%% @doc Perform a single read, synchronous
execute_command(read, {Key, Type}, Sender, State = #coord_state{
    transaction=Transaction,
    internal_read_set=InternalReadSet,
    updated_partitions=UpdatedPartitions
}) ->
    case perform_read({Key, Type}, UpdatedPartitions, Transaction, Sender) of
        {error, _} ->
            abort(State);
        ReadResult ->
            NewInternalReadSet = orddict:store(Key, ReadResult, InternalReadSet),
            {{ok, ReadResult}, execute_op, State#coord_state{internal_read_set=NewInternalReadSet}}
    end;

%% @doc Read a batch of objects, asynchronous
execute_command(read_objects, Objects, Sender, State =
    #coord_state{transaction=Transaction, internal_read_set = ReadSet}) ->

    ExecuteReads = fun(Object, {ReqNum, AccState}) ->
        ?PROMETHEUS_COUNTER:inc(antidote_operations_total, [read_async]),
        case Object of
            {Key, Type, Function} ->
                Partition = ?LOG_UTIL:get_key_partition(Key),
                case orddict:find(Key, ReadSet) of
                    {ok, Snapshot} ->
                        gen_statem:cast(self(), {ok, {ReqNum, Key, Type, Function, Snapshot}});
                    error ->
                        ok = clocksi_vnode:async_read_data_function(Partition, Transaction, ReqNum, Key, Type, Function)
                end,

                %ok = clocksi_object_function:async_execute_object_function(
                %    {fsm, self()}, Transaction, Partition, ReqNum, Key, Type, Function, WriteSet, ReadSet),
                ReadKeys = AccState#coord_state.return_accumulator,
                {ReqNum + 1, AccState#coord_state{return_accumulator=[Key | ReadKeys]}};
            {Key, Type} ->
                Partition = ?LOG_UTIL:get_key_partition(Key),
                case orddict:find(Key, ReadSet) of
                    {ok, Snapshot} ->
                        gen_statem:cast(self(), {ok, {ReqNum, Key, Type, Snapshot}});
                    error ->
                        ok = clocksi_vnode:async_read_data_item(Partition, Transaction, ReqNum, Key, Type)
                end,

                ReadKeys = AccState#coord_state.return_accumulator,
                {ReqNum + 1, AccState#coord_state{return_accumulator=[Key | ReadKeys]}}
        end
    end,

    {_, NewCoordState} = lists:foldl(
        ExecuteReads,
        {0, State#coord_state{num_to_read = length(Objects), return_accumulator=[]}},
        Objects
    ),

    {receive_read_objects_result, NewCoordState#coord_state{from=Sender}};

%% @doc Perform update operations on a batch of Objects
execute_command(update_objects, UpdateOps, Sender, State = #coord_state{transaction=Transaction}) ->
    ExecuteUpdates = fun(Op, AccState=#coord_state{
        client_ops = ClientOps0,
        internal_read_set = ReadSet,
        updated_partitions = UpdatedPartitions0
    }) ->
        case perform_update(Op, UpdatedPartitions0, Transaction, Sender, ClientOps0, ReadSet) of
            {error, _} = Err ->
                AccState#coord_state{return_accumulator = Err};

            {UpdatedPartitions, ClientOps, NumOfUpds} ->
                NumToRead = AccState#coord_state.num_to_read,
                AccState#coord_state{
                    client_ops=ClientOps,
                    num_to_read=NumToRead + NumOfUpds,
                    updated_partitions=UpdatedPartitions
                }
        end
                     end,

    NewCoordState = lists:foldl(
        ExecuteUpdates,
        State#coord_state{num_to_read=0, return_accumulator=ok},
        UpdateOps
    ),

    LoggingState = NewCoordState#coord_state{from=Sender},

    case LoggingState#coord_state.num_to_read > 0 of
        true ->
            {receive_logging_responses, LoggingState};
        false ->
            {receive_logging_responses, LoggingState, 0}
    end.

%% @doc function called when 2pc is forced independently of the number of partitions
%%      involved in the txs.
prepare_2pc(State = #coord_state{
    transaction = Transaction,
    updated_partitions = UpdatedPartitions, full_commit = FullCommit, from = From}) ->
    case UpdatedPartitions of
        [] ->
            SnapshotTime = Transaction#transaction.snapshot_time,
            case FullCommit of
                false ->
                    {committing_2pc, State#coord_state{state = committing, commit_time = SnapshotTime},
                        [{reply, From, {ok, SnapshotTime}}]};
                true ->
                    reply_to_client(State#coord_state{state = committed_read_only})
            end;
        [_|_] ->
            ok = ?CLOCKSI_VNODE:prepare(UpdatedPartitions, Transaction),
            Num_to_ack = length(UpdatedPartitions),
            {receive_prepared,
                State#coord_state{num_to_ack = Num_to_ack, state = prepared}}
    end.


%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
%% #Locks
reply_to_client(State = #coord_state{
    from=From,
    state=TxState,
    is_static=IsStatic,
    stay_alive=StayAlive,
    client_ops=ClientOps,
    commit_time=CommitTime,
    full_commit=FullCommit,
    transaction=Transaction,
    return_accumulator=ReturnAcc,
    transactionid=TransactionId,
    properties=Properties
}) ->

    Locks = lists:keyfind(locks,1,Properties),
    case Locks of
        false -> ok;
        {locks,_Locks} ->
            ?LOCK_MGR:release_locks(TransactionId),
            ok
    end,
    Shared_Locks = lists:keyfind(shared_locks,1,Properties),
    Exclusive_Locks = lists:keyfind(exclusive_locks,1,Properties),
    case {Shared_Locks,Exclusive_Locks} of
        {false,false} -> ok;
        _ ->
            ?LOCK_MGR_ES:release_locks(TransactionId),
            ok
    end,

    case From of
        undefined ->
            ok;

        Node ->
            TxId = Transaction#transaction.txn_id,
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
                            CausalClock = ?VECTORCLOCK:set_clock_of_dc(DcId, CommitTime, Transaction#transaction.vec_snapshot_time),
                            case IsStatic of
                                false ->
                                    {ok, {TxId, CausalClock}};
                                true ->
                                    {ok, CausalClock}
                            end;

                        aborted ->
                            ?PROMETHEUS_COUNTER:inc(antidote_aborted_transactions_total),
                            case ReturnAcc of
                                {error, Reason} ->
                                    {error, Reason};
                                _ ->
                                    {error, {aborted, TxId}}
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
    ?PROMETHEUS_GAUGE:dec(antidote_open_transactions),

    case StayAlive of
        true ->
            {start_tx, init_state(StayAlive, FullCommit, IsStatic, [])};
        false ->
            {stop, normal, State}
    end.


%% @doc The following function is used to apply the updates that were performed by the running
%% transaction, to the result returned by a read.
-spec apply_tx_updates_to_snapshot (key(), #coord_state{}, type(), snapshot()) -> snapshot().
apply_tx_updates_to_snapshot(Key, CoordState, Type, Snapshot)->
    Partition = ?LOG_UTIL:get_key_partition(Key),
    Found = lists:keyfind(Partition, 1, CoordState#coord_state.updated_partitions),

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
    SnapshotTime = vectorclock:set_clock_of_dc(DcId, Now, VecSnapshotTime),
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
            lager:info("clocksi_coord ~p ms sleep",[10]),
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

%replace_last(List, Key, NewKey) ->
%    Res = replace_first(lists:reverse(List), Key, NewKey),
%    lists:reverse(Res).

perform_read({Key, Type}, UpdatedPartitions, Transaction, Sender) ->
    ?PROMETHEUS_COUNTER:inc(antidote_operations_total, [read]),
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


perform_update(Op, UpdatedPartitions, Transaction, _Sender, ClientOps, InternalReadSet) ->
    {Key, Type, Update} = Op,

    %% Execute pre_commit_hook if any
    case antidote_hooks:execute_pre_commit_hook(Key, Type, Update, {Transaction, InternalReadSet, UpdatedPartitions}) of
        {error, Reason} ->
            lager:debug("Execute pre-commit hook failed ~p", [Reason]),
            {error, Reason};

        PostHookUpdates when is_list(PostHookUpdates) ->
            lists:foldl(fun({Key1, Type1, Update1}, {CurrUpdatedPartitions, CurrUpdatedOps, TotalUpds}) ->
                ?PROMETHEUS_COUNTER:inc(antidote_operations_total, [update]),

                {NewUpdatedPartitions, UpdatedOps} = generate_new_state(Key1, Type1, Update1,
                    CurrUpdatedPartitions, Transaction, InternalReadSet, CurrUpdatedOps),
                {NewUpdatedPartitions, UpdatedOps, TotalUpds + 1}

            end, {UpdatedPartitions, ClientOps, 0}, PostHookUpdates);

        {Key, Type, PostHookUpdate} ->
            ?PROMETHEUS_COUNTER:inc(antidote_operations_total, [update]),

            {NewUpdatedPartitions, UpdatedOps} = generate_new_state(Key, Type, PostHookUpdate,
                UpdatedPartitions, Transaction, InternalReadSet, ClientOps),
            {NewUpdatedPartitions, UpdatedOps, 1}
    end.

generate_new_state(Key, Type, HookUpdate, Partitions, Transaction, ReadSet, Ops) ->
    Partition = ?LOG_UTIL:get_key_partition(Key),

    WriteSet = case lists:keyfind(Partition, 1, Partitions) of
                   false ->
                       [];
                   {Partition, WS} ->
                       WS
               end,

    %% Generate the appropriate state operations based on older snapshots
    GenerateResult = ?CLOCKSI_DOWNSTREAM:generate_downstream_op(
        Transaction,
        Partition,
        Key,
        Type,
        HookUpdate,
        WriteSet,
        ReadSet
    ),

    case GenerateResult of
        {error, Reason} ->
            {error, Reason};

        {ok, DownstreamOp} ->
            ok = async_log_propagation(Partition, Transaction#transaction.txn_id, Key, Type, DownstreamOp),

            %% Append to the write set of the updated partition
            GeneratedUpdate = {Key, Type, DownstreamOp},
            NewUpdatedPartitions = append_updated_partitions(
                Partitions,
                WriteSet,
                Partition,
                GeneratedUpdate
            ),

            UpdatedOps = [{Key, Type, HookUpdate} | Ops],
            {NewUpdatedPartitions, UpdatedOps}
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
prepare(State = #coord_state{
    from=From,
    num_to_read=NumToRead,
    full_commit=FullCommit,
    transaction=Transaction,
    updated_partitions=UpdatedPartitions
}) ->
    case UpdatedPartitions of
        [] ->
            SnapshotTime = Transaction#transaction.snapshot_time,
            case NumToRead of
                0 ->
                    case FullCommit of
                        true ->
                            reply_to_client(State#coord_state{state = committed_read_only});

                        false ->
                            {committing, State#coord_state{state = committing, commit_time = SnapshotTime},
                                [{reply, From, {ok, SnapshotTime}}]}
                    end;
                _ ->
                    {receive_prepared, State#coord_state{state = prepared}}
            end;

        [_] ->
            ok = ?CLOCKSI_VNODE:single_commit(UpdatedPartitions, Transaction),
            {single_committing, State#coord_state{state = committing, num_to_ack = 1}};

        [_|_] ->
            ok = ?CLOCKSI_VNODE:prepare(UpdatedPartitions, Transaction),
            Num_to_ack = length(UpdatedPartitions),
            {receive_prepared, State#coord_state{num_to_ack = Num_to_ack, state = prepared}}
    end.


process_prepared(ReceivedPrepareTime, State = #coord_state{num_to_ack = NumToAck,
    commit_protocol = CommitProtocol, full_commit = FullCommit,
    from = From, prepare_time = PrepareTime,
    transaction = Transaction,
    updated_partitions = UpdatedPartitions}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 1 ->
        case CommitProtocol of
            two_phase ->
                case FullCommit of
                    true ->
                        ok = ?CLOCKSI_VNODE:commit(UpdatedPartitions, Transaction, MaxPrepareTime),
                        {receive_committed,
                            State#coord_state{num_to_ack = length(UpdatedPartitions), commit_time = MaxPrepareTime, state = committing}};
                    false ->
                        {committing_2pc, State#coord_state{
                            prepare_time = MaxPrepareTime,
                            commit_time = MaxPrepareTime,
                            state = committing
                        }, [{reply, From, {ok, MaxPrepareTime}}]}
                end;
            _ ->
                case FullCommit of
                    true ->
                        ok = ?CLOCKSI_VNODE:commit(UpdatedPartitions, Transaction, MaxPrepareTime),
                        {receive_committed,
                            State#coord_state{
                                num_to_ack = length(UpdatedPartitions),
                                commit_time = MaxPrepareTime,
                                state = committing
                            }
                        };
                    false ->
                        {committing, State#coord_state{
                            prepare_time = MaxPrepareTime,
                            commit_time = MaxPrepareTime,
                            state = committing
                        }, [{reply, From, {ok, MaxPrepareTime}}]}
                end
        end;
        _ ->
            {receive_prepared, State#coord_state{num_to_ack = NumToAck - 1, prepare_time = MaxPrepareTime}}
    end.


%% @doc when an error occurs or an updated partition
%% does not pass the certification check, the transaction aborts.
abort(State = #coord_state{transaction = Transaction,
    updated_partitions = UpdatedPartitions}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            reply_to_client(State#coord_state{state = aborted});
        _ ->
            ok = ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
            {receive_aborted, State#coord_state{num_to_ack = NumToAck, state = aborted}}
    end.


execute_post_commit_hooks(Ops) ->
    lists:foreach(fun({Key, Type, Update}) ->
        case antidote_hooks:execute_post_commit_hook(Key, Type, Update) of
            {error, Reason} ->
                lager:info("Post commit hook failed. Reason ~p", [Reason]);
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
    {ok, Pid} = clocksi_interactive_coord:start_link(self(), ignore),
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
        ?assertMatch({error, {aborted , _}}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_single_abort_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({error, {aborted , _}}, gen_statem:call(Pid, {prepare, empty}, infinity))
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
        ?assertMatch({error, {aborted , _}}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test2(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({error, {aborted , _}}, gen_statem:call(Pid, {prepare, empty}, infinity))
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
    ?assertMatch([{mock_dc, _}], dict:to_list(SnapshotTime)).

wait_for_clock_test() ->
    {ok, SnapshotTime} = wait_for_clock(vectorclock:from_list([{mock_dc, 10}])),
    ?assertMatch([{mock_dc, _}], dict:to_list(SnapshotTime)),
    VecClock = dc_utilities:now_microsec(),
    {ok, SnapshotTime2} = wait_for_clock(vectorclock:from_list([{mock_dc, VecClock}])),
    ?assertMatch([{mock_dc, _}], dict:to_list(SnapshotTime2)).

% New #Locks tests
start_link_with_locks_available_test() ->
    Clientclock = ignore,
    Properties = [{locks,[a,b]}],
    StayAlive = false,
    {ok, Pid} = clocksi_interactive_coord:start_link(self(), Clientclock, Properties, StayAlive),
    ok = clocksi_interactive_coord:stop(Pid).

start_link_with_locks_not_available_test() ->
    Clientclock = ignore,
    StayAlive = false,
    Properties = [{locks,[b,c]}],
    {ok, Pid} = clocksi_interactive_coord:start_link(self(), Clientclock, Properties, StayAlive),
    case process_info(Pid) of undefined -> ok;
        _ -> clocksi_interactive_coord:stop(Pid),
             {error,"Process should not be up"}
    end.
start_link_with_locks_available_es_test() ->
    Clientclock = ignore,
    Properties = [{exclusive_locks,[a,b]}],
    StayAlive = false,
    {ok, Pid} = clocksi_interactive_coord:start_link(self(), Clientclock, Properties, StayAlive),
    ok = clocksi_interactive_coord:stop(Pid).

start_link_with_locks_not_available_es_test() ->
    Clientclock = ignore,
    StayAlive = false,
    Properties = [{exclusive_locks,[b,c]}],
    {ok, Pid} = clocksi_interactive_coord:start_link(self(), Clientclock, Properties, StayAlive),
    case process_info(Pid) of undefined -> ok;
        _ -> clocksi_interactive_coord:stop(Pid),
             {error,"Process should not be up"}
    end.
-endif.


