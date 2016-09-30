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
%%      involved key. when a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(clocksi_interactive_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

-ifdef(TEST).
-define(APPLICATION, mock_partition_fsm).
-include_lib("eunit/include/eunit.hrl").
-define(DC_META_UTIL, mock_partition_fsm).
-define(DC_UTIL, mock_partition_fsm).
-define(VECTORCLOCK, mock_partition_fsm).
-define(LOG_UTIL, mock_partition_fsm).
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(CLOCKSI_DOWNSTREAM, mock_partition_fsm).
-define(LOGGING_VNODE, mock_partition_fsm).
-else.
-define(DC_META_UTIL, dc_meta_data_utilities).
-define(APPLICATION, application).
-define(DC_UTIL, dc_utilities).
-define(VECTORCLOCK, vectorclock).
-define(LOG_UTIL, log_utilities).
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(CLOCKSI_DOWNSTREAM, clocksi_downstream).
-define(LOGGING_VNODE, logging_vnode).
-endif.


%% API
-export([start_link/2,
         start_link/1,
         start_link/3,
         start_link/4]).

%% Callbacks
-export([init/1,
    code_change/4,
    handle_event/3,
    handle_info/3,
    handle_sync_event/4,
    terminate/3,
    stop/1]).

%% States
-export([create_transaction_record/6,
    start_tx/2,
    init_state/4,
    perform_update/3,
    perform_read/4,
    execute_op/3,
receive_read_objects_result/2,
    receive_logging_responses/2,
    finish_op/3,
    prepare/1,
    prepare_2pc/1,
    process_prepared/2,
    receive_prepared/2,
    single_committing/2,
    committing_2pc/3,
    committing_single/3,
    committing/3,
    receive_committed/2,
    receive_aborted/2,
    abort/1,
    abort/2,
    perform_singleitem_read/2,
%%    perform_singleitem_update/3,
    reply_to_client/1,
    generate_name/1]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock, UpdateClock, StayAlive) ->
    case StayAlive of
        true ->
            gen_fsm:start_link({local, generate_name(From)}, ?MODULE, [From, Clientclock, UpdateClock, StayAlive], []);
        false ->
            gen_fsm:start_link(?MODULE, [From, Clientclock, UpdateClock, StayAlive], [])
    end.

start_link(From, Clientclock) ->
    start_link(From, Clientclock, update_clock).

start_link(From,Clientclock,UpdateClock) ->
    start_link(From,Clientclock,UpdateClock,false).

start_link(From) ->
    start_link(From, ignore, update_clock).

finish_op(From, Key, Result) ->
    gen_fsm:send_event(From, {Key, Result}).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid, stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, UpdateClock, StayAlive]) ->
    {ok, Protocol} = ?APPLICATION:get_env(antidote, txn_prot),
    {ok, execute_op, start_tx_internal(From, ClientClock, UpdateClock, init_state(StayAlive, false, false, Protocol))}.


init_state(StayAlive, FullCommit, IsStatic, Protocol) ->
    #tx_coord_state{
       transactional_protocol = Protocol,
        transaction = undefined,
	    client_ops=[],
	    updated_partitions = [],
        prepare_time = 0,
        commit_time = 0,
        num_to_read = 0,
        num_to_ack = 0,
        operations = [],
        from = undefined,
        full_commit = FullCommit,
        is_static = IsStatic,
        return_accumulator= [],
        internal_read_set = orddict:new(),
        stay_alive = StayAlive,
        %% The following are needed by the physics protocol
        version_max = vectorclock:new()
    }.

-spec generate_name(pid()) -> atom().
generate_name(From) ->
    list_to_atom(pid_to_list(From) ++ "interactive_cord").

start_tx({start_tx, From, ClientClock, UpdateClock}, SD0) ->
    {next_state, execute_op, start_tx_internal(From, ClientClock, UpdateClock, SD0)}.

start_tx_internal(From, ClientClock, UpdateClock, SD = #tx_coord_state{stay_alive = StayAlive, transactional_protocol = Protocol}) ->
    {Transaction, TransactionId} = create_transaction_record(ClientClock, UpdateClock, StayAlive, From, false, Protocol),
    From ! {ok, TransactionId},
    SD#tx_coord_state{transaction=Transaction, num_to_read=0}.

-spec create_transaction_record(snapshot_time() | ignore, update_clock | no_update_clock,
  boolean(), pid() | undefined, boolean(), atom()) -> {transaction(), txid() | {error, term()}}.
create_transaction_record(ClientClock, UpdateClock, StayAlive, From, IsStatic, Protocol) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    _Res = random:seed(dc_utilities:now()),
    Name = case StayAlive of
               true ->
                   case IsStatic of
                       true ->
                           clocksi_static_tx_coord_fsm:generate_name(From);
                       false ->
                           generate_name(From)
                   end;
               false ->
                   self()
           end,
    case Protocol of
        physics ->
            PhysicsReadMetadata = #physics_read_metadata{
                dep_upbound = vectorclock:new(),
                commit_time_lowbound = vectorclock:new()},
            Now = clocksi_vnode:now_microsec(dc_utilities:now()),
            TransactionId = #tx_id{local_start_time= Now, server_pid = Name},
            Transaction = #transaction{
                transactional_protocol = Protocol,
                physics_read_metadata = PhysicsReadMetadata,
                txn_id = TransactionId},
            {Transaction, TransactionId};
        Protocol when ((Protocol == gr) or (Protocol == clocksi)) ->
            {ok, SnapshotTime} = case ClientClock of
                                     ignore ->
                                         get_snapshot_time();
                                     _ ->
                                         case UpdateClock of
                                             update_clock ->
                                                 get_snapshot_time(ClientClock);
                                             no_update_clock ->
                                                 {ok, ClientClock}
                                         end
                                 end,
            DcId = ?DC_UTIL:get_my_dc_id(),
            LocalClock = ?VECTORCLOCK:get_clock_of_dc(DcId, SnapshotTime),
            TransactionId = #tx_id{local_start_time = LocalClock, server_pid = Name},

            Transaction = #transaction{snapshot_clock = LocalClock,
                transactional_protocol = Protocol,
                snapshot_vc = SnapshotTime,
                txn_id = TransactionId},
            {Transaction, TransactionId}
    end.

%% @doc This is a standalone function for directly contacting the read
%%      server located at the vnode of the key being read.  This read
%%      is supposed to be light weight because it is done outside of a
%%      transaction fsm and directly in the calling thread.
-spec perform_singleitem_read(key(), type()) -> {ok, val(), snapshot_time()} | {error, reason()}.
perform_singleitem_read(Key, Type) ->
%%    todo: there should be a better way to get the Protocol.
    {ok, Protocol} = application:get_env(antidote, txn_prot),
    {Transaction, _TransactionId} = create_transaction_record(ignore, update_clock, false, undefined, true, Protocol),
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case clocksi_readitem_fsm:read_data_item(IndexNode, Key, Type, Transaction) of
        {error, Reason} ->
            {error, Reason};
        {ok, {Snapshot, _SnapshotCommitTime}} ->
            ReadResult = Type:value(Snapshot),
            %% Read only transaction has no commit, hence return the snapshot time
            CommitTime = Transaction#transaction.snapshot_vc,
            {ok, ReadResult, CommitTime}
    end.

%% @doc This is a standalone function for directly contacting the update
%%      server vnode.  This is lighter than creating a transaction
%%      because the update/prepare/commit are all done at one time
%%-spec perform_singleitem_update(key(), type(), {op(), term()}) -> {ok, {txid(), [], snapshot_time()}} | {error, term()}.
%%perform_singleitem_update(Key, Type, Params1)->
%%    {ok, Protocol}=application:get_env(antidote, txn_prot),
%%    {Transaction, TxId}=create_transaction_record(ignore, update_clock, false, undefined, true, Protocol),
%%    Preflist=log_utilities:get_preflist_from_key(Key),
%%    IndexNode=hd(Preflist),
%%    %% Todo: There are 3 messages sent to a vnode: 1 for downstream generation,
%%    %% todo: another for logging, and finally one for single commit.
%%    %% todo: couldn't we replace them for just 1, and do all that directly at the vnode?
%%    %% Execute pre_commit_hook if any
%%    case antidote_hooks:execute_pre_commit_hook(Key, Type, Params1) of
%%        {Key, Type, Params}->
%%            case ?CLOCKSI_DOWNSTREAM:generate_downstream_op(Transaction, IndexNode, Key, Type, Params, [], []) of
%%                {ok, DownstreamRecord, CommitRecordParameters}->
%%                    Updated_partition=
%%                        case Transaction#transaction.transactional_protocol of
%%                            physics->
%%                                {DownstreamRecordCommitVC, _, _}=CommitRecordParameters,
%%                                [{IndexNode, [{Key, Type, {DownstreamRecord, DownstreamRecordCommitVC}}]}];
%%                            Protocol when ((Protocol==gr)or(Protocol==clocksi))->
%%                                [{IndexNode, [{Key, Type, DownstreamRecord}]}]
%%                        end,
%%                    LogRecord=#log_operation{tx_id=TxId, op_type=update,
%%                        log_payload=#update_log_payload{key=Key, type=Type, op=DownstreamRecord}},
%%                    LogId=?LOG_UTIL:get_logid_from_key(Key),
%%                    [Node]=Preflist,
%%                    case ?LOGGING_VNODE:append(Node, LogId, LogRecord) of
%%                        {ok, _}->
%%                            case ?CLOCKSI_VNODE:single_commit_sync(Updated_partition, Transaction) of
%%                                {committed, CommitTime}->
%%                                    %% Execute post commit hook
%%                                    _Res=case antidote_hooks:execute_post_commit_hook(Key, Type, Params1) of
%%                                        {error, Reason}->
%%                                            lager:info("Post commit hook failed. Reason ~p", [Reason]);
%%                                        _->
%%                                            ok
%%                                    end,
%%                                    DcId=?DC_UTIL:get_my_dc_id(),
%%                                    SnapshotVC=
%%                                        case Transaction#transaction.transactional_protocol of
%%                                            physics->
%%                                                {_CommitVC, DepVC, _ReadTime}=CommitRecordParameters,
%%                                                DepVC;
%%                                            Protocol when ((Protocol==clocksi)or(Protocol==gr))->
%%                                                Transaction#transaction.snapshot_vc
%%                                        end,
%%                                    CausalClock=vectorclock:set_clock_of_dc(
%%                                        DcId, CommitTime, SnapshotVC),
%%                                    {ok, {TxId, [], CausalClock}};
%%                                abort->
%%                                    {error, aborted}
%%                            end;
%%                        {error, Reason}->
%%                            {error, Reason}
%%                    end;
%%                {error, Reason}->
%%                    {error, Reason}
%%            end;
%%        {error, Reason}->
%%            {error, Reason}
%%    end.

perform_read({Key, Type}, Updated_partitions, Transaction, Sender) ->
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    WriteSet = case lists:keyfind(IndexNode, 1, Updated_partitions) of
                   false ->
                       [];
                   {IndexNode, WS} ->
                       WS
               end,
    case ?CLOCKSI_VNODE:read_data_item(IndexNode, Transaction, Key, Type, WriteSet) of
        {error, Reason} ->
            case Sender of
                undefined ->
                    ok;
                _ ->
                    _Res = gen_fsm:reply(Sender, {error, Reason})
            end,
            {error, Reason};
        {ok, ReadResult} ->
            ReadResult
    end.
perform_update(UpdateArgs, _Sender, CoordState) ->
    {Key, Type, Param1} = UpdateArgs,
%%    lager:info("updating with the following paramaters: ~p~n",[Param]),
    UpdatedPartitions = CoordState#tx_coord_state.updated_partitions,
    Transaction = CoordState#tx_coord_state.transaction,
    TransactionalProtocol = Transaction#transaction.transactional_protocol,
    InternalReadSet = CoordState#tx_coord_state.internal_read_set,
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    ClientOps = CoordState#tx_coord_state.client_ops,
%%    Add the vnode to the updated partitions, if not already there.
    WriteSet = case lists:keyfind(IndexNode, 1, UpdatedPartitions) of
                   false ->
                       [];
                   {IndexNode, WS} ->
                       WS
               end,
	case antidote_hooks:execute_pre_commit_hook(Key, Type, Param1) of
		{Key, Type, Param} ->
    %% Todo: There are 3 messages sent to a vnode: 1 for downstream generation,
    %% todo: another for logging, and finally one (or two) for  commit.
    %% todo: couldn't we replace them for just 1, and do all that directly at the vnode?
            case ?CLOCKSI_DOWNSTREAM:generate_downstream_op(Transaction, IndexNode, Key, Type, Param, WriteSet, InternalReadSet) of
                {ok, DownstreamRecord, SnapshotParameters}->
%%                                lager:debug("DownstreamRecord ~p~n _SnapshotParameters ~p~n", [DownstreamRecord, SnapshotParameters]),
                    State1=case TransactionalProtocol of
                        physics->
                            {DownstreamOpCommitVC, _DepVC, _ReadTimeVC}=SnapshotParameters,
                            case WriteSet of
                                []->
                                    NewUpdatedPartitions=[{IndexNode, [{Key, Type, {DownstreamRecord, DownstreamOpCommitVC}}]}|UpdatedPartitions],
                                    NewCoordState = update_causal_snapshot_state(CoordState#tx_coord_state{updated_partitions = NewUpdatedPartitions}, SnapshotParameters, Key),
                                    NewCoordState#tx_coord_state{updated_partitions=NewUpdatedPartitions};
                                _->
                                    NewUpdatedPartitions=lists:keyreplace(IndexNode, 1, UpdatedPartitions,
                                        {IndexNode, [{Key, Type, {DownstreamRecord, DownstreamOpCommitVC}}|WriteSet]}),
                                    CoordState#tx_coord_state{updated_partitions=NewUpdatedPartitions}
                            end;
                        Prot when ((Prot==gr)or(Prot==clocksi))->
                            case WriteSet of
                                []->
                                    NewUpdatedPartitions=[{IndexNode, [{Key, Type, DownstreamRecord}]}|UpdatedPartitions],
                                    CoordState#tx_coord_state{updated_partitions=NewUpdatedPartitions};
                                _->
                                    NewUpdatedPartitions=lists:keyreplace(IndexNode, 1, UpdatedPartitions,
                                        {IndexNode, [{Key, Type, DownstreamRecord}|WriteSet]}),
                                    CoordState#tx_coord_state{updated_partitions=NewUpdatedPartitions}
                            end
                    end,
                    TxId=Transaction#transaction.txn_id,
                    LogRecord=#log_operation{tx_id=TxId, op_type=update,
                        log_payload=#update_log_payload{key=Key, type=Type, op=DownstreamRecord}},
                    LogId=?LOG_UTIL:get_logid_from_key(Key),
                    [Node]=Preflist,
                    ok=?LOGGING_VNODE:asyn_append(Node, LogId, LogRecord, self()),
                    State1#tx_coord_state{client_ops = [{Key, Type, Param} | ClientOps]};
                
                {error, Reason}->
                    {error, Reason}
            end;
        {error, Reason}->
            lager:debug("Execute pre-commit hook failed ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.

execute_op({update, Args}, Sender, SD0) ->
%%    lager:debug("got execute update"),
    execute_op({update_objects, [Args]}, Sender, SD0);

execute_op({OpType, Args}, Sender,
  SD0 = #tx_coord_state{transaction = Transaction,
      updated_partitions = Updated_partitions
  }) ->
    case OpType of
        prepare ->
            case Args of
                two_phase ->
                    prepare_2pc(SD0#tx_coord_state{from = Sender, commit_protocol = Args});
                _ ->
                    prepare(SD0#tx_coord_state{from = Sender, commit_protocol = Args})
            end;
        read ->
            {Key, Type} = Args,
            case perform_read({Key, Type}, Updated_partitions, Transaction, Sender) of
                {error, _Reason} ->
                    abort(SD0);
                ReadResult ->
                    {Snapshot, CommitParams} = ReadResult,
                    SD1 = case Transaction#transaction.transactional_protocol of
                              physics ->
                                  update_causal_snapshot_state(SD0, CommitParams, Key);
                              Protocol when ((Protocol == gr) or (Protocol == clocksi)) -> SD0
                          end,
                    InternalReadSet = orddict:store(key, ReadResult, SD1#tx_coord_state.internal_read_set),
                    {reply, {ok, Type:value(Snapshot)}, execute_op, SD1#tx_coord_state{internal_read_set = InternalReadSet}}
            end;
        read_objects ->
%%            lager:debug("got to read: ~p", [Args]),
            NewTransaction = case Transaction#transaction.transactional_protocol of
                physics ->
                    case Transaction#transaction.physics_read_metadata#physics_read_metadata.commit_time_lowbound == [] of
                        true ->
                            PhysicsClock = vectorclock:set_clock_of_dc(dc_utilities:get_my_dc_id(), clocksi_vnode:now_microsec(dc_utilities:now()), vectorclock:new()),
                            PhysicsMetadata = #physics_read_metadata{dep_upbound = PhysicsClock, commit_time_lowbound = PhysicsClock},
                            Transaction#transaction{physics_read_metadata = PhysicsMetadata};
                        false ->
                            Transaction
                    end;
                Protocol when ((Protocol == gr) or (Protocol == clocksi)) ->
                    Transaction
            end,
            ExecuteReads = fun({Key, Type}, Acc) ->
                Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                IndexNode = hd(Preflist),
%%                lager:debug("async reading: ~n ~p ", [{IndexNode, NewTransaction, Key, Type}]),
                ok = clocksi_vnode:async_read_data_item(IndexNode, NewTransaction, Key, Type),
                ReadSet = Acc#tx_coord_state.return_accumulator,
                Acc#tx_coord_state{return_accumulator= [Key | ReadSet]}
                           end,
            NewCoordState = lists:foldl(ExecuteReads, SD0#tx_coord_state{num_to_read = length(Args), return_accumulator= []}, Args),
            {next_state, receive_read_objects_result, NewCoordState#tx_coord_state{from = Sender}};
        update_objects ->
%%            lager:debug("got update call with params: ~p",[Args]),
            ExecuteUpdates = fun({Key, Type, UpdateParams}, Acc) ->
%%                lager:debug("Executing this update: ~p",[{Key, Type, UpdateParams}]),
                Result = perform_update({Key, Type, UpdateParams}, Sender, Acc),
%%                lager:debug("got result: ~p",[Result]),
                case Result of
                    {error, Reason} ->
                        Acc#tx_coord_state{return_accumulator= {error, Reason}};
                    NewCoordinatorState ->
                        NewNumToRead = NewCoordinatorState#tx_coord_state.num_to_read,
%%                        lager:debug("Updated Number of responses expected: ~p",[NewNumToRead+1]),
                        NewCoordinatorState#tx_coord_state{num_to_read =  NewNumToRead+1}
                end
                             end,
            NewCoordState = lists:foldl(ExecuteUpdates, SD0#tx_coord_state{num_to_read = 0, return_accumulator= ok}, Args),
            case NewCoordState#tx_coord_state.num_to_read > 0 of
                true ->
%%                    lager:debug("num_to_read > 0"),
                    {next_state, receive_logging_responses, NewCoordState#tx_coord_state{from = Sender}};
                false ->
%%                    lager:debug("num_to_read <= 0"),
                    {next_state, receive_logging_responses, NewCoordState#tx_coord_state{from = Sender}, 0}
            end
    end.

receive_logging_responses(Response, S0 = #tx_coord_state{num_to_read = NumToReply,
    return_accumulator= ReturnAcc}) ->
%%    lager:debug("Waiting for log responses, missing: ~p",[NumToReply]),
    NewAcc = case Response of
                 {error, Reason} -> {error, Reason};
                 {ok, _OpId} -> ReturnAcc;
                 timeout -> ReturnAcc
             end,
    case NumToReply > 1 of
        false ->
            gen_fsm:reply(S0#tx_coord_state.from, NewAcc),
            case (NewAcc == ok) of
                true ->
                    {next_state, execute_op, S0#tx_coord_state{num_to_read = 0, return_accumulator= []}};
                false ->
                    abort(S0)
            end;
        true ->
            {next_state, receive_logging_responses,
                S0#tx_coord_state{num_to_read = NumToReply - 1, return_accumulator= NewAcc}}
    end.


receive_read_objects_result({ok, {Key, Type, {Snapshot, SnapshotCommitParams}}},
  S0 = #tx_coord_state{num_to_read = NumToRead,
      return_accumulator= ReadSet,
      internal_read_set = InternalReadSet,
      transactional_protocol = TransactionalProtocol}) ->
    %%TODO: type is hard-coded..
    Value = Type:value(Snapshot),
    ReadSet1 = clocksi_static_tx_coord_fsm:replace(ReadSet, Key, Value),
    NewInternalReadSet = orddict:store(Key, {Snapshot, SnapshotCommitParams}, InternalReadSet),
    SD1 = case TransactionalProtocol of
              physics ->
                  update_causal_snapshot_state(S0, SnapshotCommitParams, Key);
              Protocol when ((Protocol == gr) or (Protocol == clocksi)) -> S0
          end,
    case NumToRead of
        1 ->
            gen_fsm:reply(SD1#tx_coord_state.from, {ok, lists:reverse(ReadSet1)}),
            {next_state, execute_op, SD1#tx_coord_state{num_to_read = 0, internal_read_set = NewInternalReadSet}};
        _ ->
            {next_state, receive_read_objects_result,
                SD1#tx_coord_state{internal_read_set = NewInternalReadSet, return_accumulator= ReadSet1, num_to_read = NumToRead - 1}}
    end.




%% @doc Used by the causal snapshot for physics
%%      Updates the state of a transaction coordinator
%%      for maintaining the causal snapshot metadata
update_causal_snapshot_state(State, ReadMetadata, _Key) ->
    {CommitVC, DepVC, ReadTimeVC} = ReadMetadata,
    NewVC = vectorclock:new(),
    case CommitVC of
        NewVC ->
            State;
        _ ->
            Transaction = State#tx_coord_state.transaction,
            VersionMax = State#tx_coord_state.version_max,
            NewVersionMax = vectorclock:max_vc(VersionMax, CommitVC),
            CommitTimeLowbound = State#tx_coord_state.transaction#transaction.physics_read_metadata#physics_read_metadata.commit_time_lowbound,
            DepUpbound = State#tx_coord_state.transaction#transaction.physics_read_metadata#physics_read_metadata.dep_upbound,
            NewDepUpB = vectorclock:min_vc(ReadTimeVC, DepUpbound),
            NewCTLowB = vectorclock:max_vc(DepVC, CommitTimeLowbound),
%%            lager:debug("~nCommitVC = ~p~n DepVC = ~p~n ReadTimeVC = ~p", [CommitVC, DepVC, ReadTimeVC]),
%%            lager:debug("DepUpbound = ~p~n, CommitTimeLowbound = ~p", [DepUpbound, CommitTimeLowbound]),
%%            lager:debug("NewDepUpB = ~p~n, NewCTLowB = ~p", [NewDepUpB, NewCTLowB]),
%%            lager:debug("VersionMax = ~p~n, NewVersionMax = ~p", [VersionMax, NewVersionMax]),
            NewTransaction = Transaction#transaction{
                physics_read_metadata = #physics_read_metadata{
                    %%Todo: CHECK THE FOLLOWING LINE FOR THE MULTIPLE DC case.
                    dep_upbound = NewDepUpB,
                    commit_time_lowbound = NewCTLowB}},
            State#tx_coord_state{
%%        keys_access_time = NewKeysAccessTime,
                version_max = NewVersionMax, transaction = NewTransaction}
    end.


%% @doc this state sends a prepare message to all updated partitions and goes
%%      to the "receive_prepared"state.
prepare(SD0 = #tx_coord_state{
    transaction = Transaction, num_to_read = NumToRead,
    updated_partitions = UpdatedPartitions, full_commit = FullCommit, from = From}) ->
    case UpdatedPartitions of
        [] ->
            Snapshot_time = Transaction#transaction.snapshot_clock,
            case NumToRead of
                0 ->
                    case FullCommit of
                        true ->
                            reply_to_client(SD0#tx_coord_state{state = committed_read_only});
                        false ->
                            gen_fsm:reply(From, {ok, Snapshot_time}),
                            {next_state, committing, SD0#tx_coord_state{state = committing, commit_time = Snapshot_time}}
                    end;
                _ ->
                    {next_state, receive_prepared,
                        SD0#tx_coord_state{state = prepared}}
            end;
        [_] ->
            ok = ?CLOCKSI_VNODE:single_commit(UpdatedPartitions, Transaction),
            {next_state, single_committing,
                SD0#tx_coord_state{state = committing, num_to_ack = 1}};
        [_ | _] ->
            ok = ?CLOCKSI_VNODE:prepare(UpdatedPartitions, Transaction),
            Num_to_ack = length(UpdatedPartitions),
            {next_state, receive_prepared,
                SD0#tx_coord_state{num_to_ack = Num_to_ack, state = prepared}}
    end.

%% @doc state called when 2pc is forced independently of the number of partitions
%%      involved in the txs.
prepare_2pc(SD0 = #tx_coord_state{
    transaction = Transaction,
    updated_partitions = Updated_partitions, full_commit = FullCommit, from = From}) ->
    case Updated_partitions of
        [] ->
            Snapshot_time = Transaction#transaction.snapshot_clock,
            case FullCommit of
                false ->
                    gen_fsm:reply(From, {ok, Snapshot_time}),
                    {next_state, committing_2pc,
                        SD0#tx_coord_state{state = committing, commit_time = Snapshot_time}};
                true ->
                    reply_to_client(SD0#tx_coord_state{state = committed_read_only})
            end;
        [_ | _] ->
            ok = ?CLOCKSI_VNODE:prepare(Updated_partitions, Transaction),
            Num_to_ack = length(Updated_partitions),
            {next_state, receive_prepared,
                SD0#tx_coord_state{num_to_ack = Num_to_ack, state = prepared}}
    end.

process_prepared(ReceivedPrepareTime, S0 = #tx_coord_state{num_to_ack = NumToAck,
    commit_protocol = CommitProtocol, full_commit = FullCommit,
    from = From, prepare_time = PrepareTime,
    transaction = Transaction,
    version_max = DependencyVC,
    updated_partitions = Updated_partitions}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    PrepareParams = case Transaction#transaction.transactional_protocol of
                        physics ->
                            {MaxPrepareTime, DependencyVC};
                        Protocol when ((Protocol == gr) or (Protocol == clocksi)) ->
                            {MaxPrepareTime, tyler_broke_this}
                    end,
    case NumToAck of 1 ->
        case CommitProtocol of
            two_phase ->
                case FullCommit of
                    true ->

                        ok = ?CLOCKSI_VNODE:commit(Updated_partitions, Transaction, PrepareParams),
                        {next_state, receive_committed,
                            S0#tx_coord_state{num_to_ack = NumToAck, commit_time = MaxPrepareTime, state = committing}};
                    false ->
                        gen_fsm:reply(From, {ok, PrepareParams}),
                        {next_state, committing_2pc,
                            S0#tx_coord_state{prepare_time = MaxPrepareTime, commit_time = MaxPrepareTime, state = committing}}
                end;
            _ ->
                case FullCommit of
                    true ->
                        ok = ?CLOCKSI_VNODE:commit(Updated_partitions, Transaction, PrepareParams),
                        {next_state, receive_committed,
                            S0#tx_coord_state{num_to_ack = NumToAck, commit_time = MaxPrepareTime, state = committing}};
                    false ->
                        gen_fsm:reply(From, {ok, PrepareParams}),
                        {next_state, committing,
                            S0#tx_coord_state{prepare_time = MaxPrepareTime, commit_time = MaxPrepareTime, state = committing}}
                end
        end;
        _ ->
            {next_state, receive_prepared,
                S0#tx_coord_state{num_to_ack = NumToAck - 1, prepare_time = MaxPrepareTime}}
    end.


%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared({prepared, ReceivedPrepareTime}, S0) ->
    process_prepared(ReceivedPrepareTime, S0);

receive_prepared(abort, S0) ->
    abort(S0);

receive_prepared(timeout, S0) ->
    abort(S0).

single_committing({committed, CommitTime}, S0 = #tx_coord_state{from = From, full_commit = FullCommit}) ->
    case FullCommit of
        false ->
            gen_fsm:reply(From, {ok, CommitTime}),
            {next_state, committing_single,
                S0#tx_coord_state{commit_time = CommitTime, state = committing}};
        true ->
            reply_to_client(S0#tx_coord_state{prepare_time = CommitTime, commit_time = CommitTime, state = committed})
    end;

single_committing(abort, S0 = #tx_coord_state{from = _From}) ->
    abort(S0);

single_committing(timeout, S0 = #tx_coord_state{from = _From}) ->
    abort(S0).


%% @doc There was only a single partition with an update in this transaction
%%      so the transaction has already been committed
%%      so just wait for the commit message from the client
committing_single(commit, Sender, SD0 = #tx_coord_state{transaction = _Transaction,
    commit_time = Commit_time}) ->
    reply_to_client(SD0#tx_coord_state{prepare_time = Commit_time, from = Sender, commit_time = Commit_time, state = committed}).

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state expects other process to sen the commit message to
%%      start the commit phase.
committing_2pc(commit, Sender, SD0 = #tx_coord_state{transaction = Transaction,
    updated_partitions = Updated_partitions,
    commit_time = Commit_time,
    version_max = DependencyVC}) ->
    NumToAck = length(Updated_partitions),
    case NumToAck of
        0 ->
            reply_to_client(SD0#tx_coord_state{state = committed_read_only, from = Sender});
        _ ->
            ok = ?CLOCKSI_VNODE:commit(Updated_partitions, Transaction, {Commit_time, DependencyVC}),
            {next_state, receive_committed,
                SD0#tx_coord_state{num_to_ack = NumToAck, from = Sender, state = committing}}
    end.

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected
committing(commit, Sender, SD0 = #tx_coord_state{transaction = Transaction,
    updated_partitions = Updated_partitions,
    commit_time = Commit_time,
    version_max = DependencyVC}) ->
    NumToAck = length(Updated_partitions),
    case NumToAck of
        0 ->
            reply_to_client(SD0#tx_coord_state{state = committed_read_only, from = Sender});
        _ ->
            ok = ?CLOCKSI_VNODE:commit(Updated_partitions, Transaction, {Commit_time, DependencyVC}),
            {next_state, receive_committed,
                SD0#tx_coord_state{num_to_ack = NumToAck, from = Sender, state = committing}}
    end.

%% @doc the fsm waits for acks indicating that each partition has successfully
%%	committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_committed(committed, S0 = #tx_coord_state{num_to_ack = NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(S0#tx_coord_state{state = committed});
        _ ->
            {next_state, receive_committed, S0#tx_coord_state{num_to_ack = NumToAck - 1}}
    end.

%% @doc when an error occurs or an updated partition
%% does not pass the certification check, the transaction aborts.
abort(SD0 = #tx_coord_state{transaction = Transaction,
                            updated_partitions = UpdatedPartitions}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            reply_to_client(SD0#tx_coord_state{state = aborted});
        _ ->
            ok = ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
            {next_state, receive_aborted,
                SD0#tx_coord_state{num_to_ack = NumToAck, state = aborted}}
    end.

abort(abort, SD0 = #tx_coord_state{transaction = _Transaction,
                                   updated_partitions = _UpdatedPartitions}) ->
    abort(SD0);

abort({prepared, _}, SD0 = #tx_coord_state{transaction = _Transaction,
                                           updated_partitions = _UpdatedPartitions}) ->
    abort(SD0);

abort(_, SD0 = #tx_coord_state{transaction = _Transaction,
                               updated_partitions = _UpdatedPartitions}) ->
    abort(SD0).

%% @doc the fsm waits for acks indicating that each partition has successfully
%%	aborted the tx and finishes operation.
%%      Should we retry sending the aborted message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_aborted(ack_abort, S0 = #tx_coord_state{num_to_ack = NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(S0#tx_coord_state{state = aborted});
        _ ->
            {next_state, receive_aborted, S0#tx_coord_state{num_to_ack = NumToAck - 1}}
    end;

receive_aborted(_, S0) ->
    {next_state, receive_aborted, S0}.

%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
reply_to_client(SD = #tx_coord_state{from = From, transaction = Transaction, return_accumulator= ReturnReadSet,
    state = TxState, commit_time = CommitTime, full_commit = FullCommit, transactional_protocol = Protocol,
    is_static = IsStatic, stay_alive = StayAlive, client_ops = ClientOps}) ->
    if undefined =/= From ->
        TxId = Transaction#transaction.txn_id,
        Reply = case Transaction#transaction.transactional_protocol of
                    Protocol when ((Protocol == gr) or (Protocol == clocksi)) ->
                        case TxState of
                            committed_read_only ->
                                case IsStatic of
                                    false ->
                                        {ok, {TxId, Transaction#transaction.snapshot_vc}};
                                    true ->
                                        {ok, {TxId, lists:reverse(ReturnReadSet), Transaction#transaction.snapshot_vc}}
                                end;
                            committed ->
	                            %% Execute post_commit_hooks
	                            _Result = execute_post_commit_hooks(ClientOps),
	                            %% TODO: What happens if commit hook fails?
                                DcId = ?DC_META_UTIL:get_my_dc_id(),
                                CausalClock = ?VECTORCLOCK:set_clock_of_dc(
                                    DcId, CommitTime, Transaction#transaction.snapshot_vc),
                                case IsStatic of
                                    false ->
                                        {ok, {TxId, CausalClock}};
                                    true ->
                                        {ok, {TxId, lists:reverse(ReturnReadSet), CausalClock}}
                                end;
                            aborted ->
                                {aborted, TxId};
                            Reason ->
                                {TxId, Reason}
                        end;
                    physics ->
                        TxnDependencyVC = case Transaction#transaction.physics_read_metadata#physics_read_metadata.commit_time_lowbound of
                                              ignore ->
                                                  lager:info("got ignore as dependency VC"),
                                                  vectorclock:new();
                                              VC -> VC
                                          end,
                        case TxState of
                            committed_read_only ->
                                case IsStatic of
                                    false ->
                                        {ok, {TxId, TxnDependencyVC}};
                                    true ->
                                        {ok, {TxId, lists:reverse(ReturnReadSet), TxnDependencyVC}}
                                end;
                            committed ->
                                DcId = ?DC_UTIL:get_my_dc_id(),
                                CausalClock = ?VECTORCLOCK:set_clock_of_dc(
                                    DcId, CommitTime, TxnDependencyVC),
                                case IsStatic of
                                    false ->
                                        {ok, {TxId, CausalClock}};
                                    true ->
                                        {ok, {TxId, lists:reverse(ReturnReadSet), CausalClock}}
                                end;
                            aborted ->
                                {aborted, TxId};
                            Reason ->
                                {TxId, Reason}
                        end

                end,
            _Res = gen_fsm:reply(From, Reply);
       true -> ok
    end,
    case StayAlive of
        true ->
            {next_state, start_tx, init_state(StayAlive, FullCommit, IsStatic, Protocol)};
        false ->
            {stop, normal, SD}
    end.

execute_post_commit_hooks(Ops) ->
    lists:foreach(
      fun({Key, Type, Update}) ->
              case antidote_hooks:execute_post_commit_hook(Key, Type, Update) of
                  {error, Reason} ->
                      lager:info("Post commit hook failed. Reason ~p", [Reason]);
                  _ -> ok
              end
      end, lists:reverse(Ops)).

%% =============================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(stop, _From, _StateName, StateData) ->
    {stop, normal, ok, StateData};

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%%@doc Set the transaction Snapshot Time to the maximum value of:
%%     1.ClientClock, which is the last clock of the system the client
%%       starting this transaction has seen, and
%%     2.machine's local time, as returned by erlang:now().
-spec get_snapshot_time(snapshot_time()) -> {ok, snapshot_time()}.
get_snapshot_time(ClientClock) ->
    wait_for_clock(ClientClock).

-spec get_snapshot_time() -> {ok, snapshot_time()}.
get_snapshot_time() ->
    Now = clocksi_vnode:now_microsec(dc_utilities:now()) - ?OLD_SS_MICROSEC,
    {ok, VecSnapshotTime} = ?DC_UTIL:get_stable_snapshot(),
    DcId = ?DC_META_UTIL:get_my_dc_id(),
    SnapshotTime = vectorclock:set_clock_of_dc(DcId, Now, VecSnapshotTime),
%%    lager:debug("MY SNAPSHOT TIME IS : ~p",[SnapshotTime]),
    {ok, SnapshotTime}.


-spec wait_for_clock(snapshot_time()) -> {ok, snapshot_time()}.
wait_for_clock(Clock) ->
    {ok, VecSnapshotTime} = get_snapshot_time(),
    case vectorclock:ge(VecSnapshotTime, Clock) of
        true ->
            %% No need to wait
            {ok, VecSnapshotTime};
        false ->
            lager:info("waiting for clock"),
            %% wait for snapshot time to catch up with Client Clock
            timer:sleep(3),
            wait_for_clock(Clock)
    end.

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
    {ok, Pid} = clocksi_interactive_tx_coord_fsm:start_link(self(), ignore),
    Pid.
cleanup(Pid) ->
    case process_info(Pid) of undefined -> io:format("Already cleaned");
        _ -> clocksi_interactive_tx_coord_fsm:stop(Pid) end.

empty_prepare_test(Pid) ->
    fun() ->
        ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

timeout_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {timeout, nothing, nothing}}, infinity)),
        ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_single_abort_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_single_success_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {single_commit, nothing, nothing}}, infinity)),
        ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test1(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test2(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_multi_success_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

read_single_fail_test(Pid) ->
    fun() ->
        ?assertEqual({error, mock_read_fail},
            gen_fsm:sync_send_event(Pid, {read, {read_fail, nothing}}, infinity))
    end.

read_success_test(Pid) ->
    fun() ->
        ?assertEqual({ok, 2},
            gen_fsm:sync_send_event(Pid, {read, {counter, riak_dt_gcounter}}, infinity)),
        ?assertEqual({ok, [a]},
            gen_fsm:sync_send_event(Pid, {read, {set, riak_dt_gset}}, infinity)),
        ?assertEqual({ok, mock_value},
            gen_fsm:sync_send_event(Pid, {read, {mock_type, mock_partition_fsm}}, infinity)),
        ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

downstream_fail_test(Pid) ->
    fun() ->
        ?assertEqual({error, mock_downstream_fail},
            gen_fsm:sync_send_event(Pid, {update, {downstream_fail, nothing, nothing}}, infinity))
    end.


get_snapshot_time_test() ->
    {ok, SnapshotTime} = get_snapshot_time(),
    ?assertMatch([{mock_dc, _}], SnapshotTime).

wait_for_clock_test() ->
    {ok, SnapshotTime} = wait_for_clock([{mock_dc, 10}]),
    ?assertMatch([{mock_dc, _}], SnapshotTime),
    VecClock = clocksi_vnode:now_microsec(dc_utilities:now()),
    {ok, SnapshotTime2} = wait_for_clock([{mock_dc, VecClock}]),
    ?assertMatch([{mock_dc, _}], SnapshotTime2).


-endif.
