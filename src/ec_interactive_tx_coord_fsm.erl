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
%%      by sending each operation to the responsible ec_vnode of the
%%      involved key. when a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(ec_interactive_tx_coord_fsm).

-behavior(gen_fsm).

-include("ec_antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DC_UTIL, mock_partition_fsm).
-define(VECTORCLOCK, mock_partition_fsm).
-define(LOG_UTIL, mock_partition_fsm).
-define(ec_VNODE, mock_partition_fsm).
-define(ec_DOWNSTREAM, mock_partition_fsm).
-define(LOGGING_VNODE, mock_partition_fsm).
-else.
-define(DC_UTIL, dc_utilities).
-define(VECTORCLOCK, vectorclock).
-define(LOG_UTIL, log_utilities).
-define(ec_VNODE, ec_vnode).
-define(ec_DOWNSTREAM, ec_downstream).
-define(LOGGING_VNODE, logging_vnode).
-endif.


%% API
-export([
    start_link/1]).

%% Callbacks
-export([init/1,
    code_change/4,
    handle_event/3,
    handle_info/3,
    handle_sync_event/4,
    terminate/3,
    stop/1]).

%% States
-export([%create_transaction_record/1,
    perform_update/3,
    perform_read/3,
    execute_op/3,
    finish_op/3,
    prepare/1,
    % prepare_2pc/0,
    process_prepared/1,
    receive_prepared/2,
    single_committing/2,
    committing_2pc/3,
    committing_single/3,
    committing/3,
    receive_committed/2,
    abort/1,
    abort/2,
    perform_singleitem_read/2,
    perform_singleitem_update/3,
    reply_to_client/1]).

%%%===================================================================
%%% API
%%%===================================================================


start_link(From) ->
    gen_fsm:start_link(?MODULE, [From, ignore], []).

finish_op(From, Key, Result) ->
    gen_fsm:send_event(From, {Key, Result}).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid, stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From]) ->
    SD = #tx_coord_state{
        updated_partitions = [],
        prepare_time = 0,
        full_commit = false,
        is_static = false
    },
    From ! {ok, {no_dc_id, no_snapshot_time, self()}},
    {ok, execute_op, SD}.

%%-spec create_transaction_record(snapshot_time() | ignore) -> {tx(), txid()}.
%%create_transaction_record(ClientClock) ->
%%  %% Seed the random because you pick a random read server, this is stored in the process state
%%  _Res = random:seed(now()),
%%  {ok, SnapshotTime} = case ClientClock of
%%                         ignore ->
%%                           get_snapshot_time();
%%                         _ ->
%%                           get_snapshot_time(ClientClock)
%%                       end,
%%  DcId = ?DC_UTIL:get_my_dc_id(),
%%  {ok, LocalClock} = ?VECTORCLOCK:get_clock_of_dc(DcId, SnapshotTime),
%%  TransactionId = #tx_id{snapshot_time = LocalClock, server_pid = self()},
%%  Transaction = #transaction{snapshot_time = LocalClock,
%%    vec_snapshot_time = SnapshotTime,
%%    txn_id = TransactionId},
%%  {Transaction, TransactionId}.

%% @doc This is a standalone function for directly contacting the read
%%      server located at the vnode of the key being read.  This read
%%      is supposed to be light weight because it is done outside of a
%%      transaction fsm and directly in the calling thread.
-spec perform_singleitem_read(key(), type()) -> {ok, val()} | {error, reason()}.
perform_singleitem_read(Key, Type) ->
%%  {Transaction, _TransactionId} = create_transaction_record(ignore),
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case ec_readitem_fsm:read_data_item(IndexNode, Key, Type) of
        {error, Reason} ->
            {error, Reason};
        {ok, Snapshot} ->
            ReadResult = Type:value(Snapshot),
            {ok, ReadResult}
    end.


%% @doc This is a standalone function for directly contacting the update
%%      server vnode.  This is lighter than creating a transaction
%%      because the update/prepare/commit are all done at one time
-spec perform_singleitem_update(key(), type(), {op(), term()}) -> {ok, {txid(), [], snapshot_time()}} | {error, term()}.
perform_singleitem_update(Key, Type, Params) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case ?ec_DOWNSTREAM:generate_downstream_op(IndexNode, Key, Type, Params, []) of
        {ok, DownstreamRecord} ->
            UpdatedPartitions = [{IndexNode, [{Key, Type, DownstreamRecord}]}],
            TxId = no_tx_id,
            LogRecord = #log_record{op_type = update,
                op_payload = {Key, Type, DownstreamRecord}},
            LogId = ?LOG_UTIL:get_logid_from_key(Key),
            [Node] = Preflist,
            case ?LOGGING_VNODE:append(Node, LogId, LogRecord) of
                {ok, _} ->
                    case ?ec_VNODE:single_commit_sync(UpdatedPartitions) of
                        {committed, CommitTime} ->
                            {ok, {TxId, [], CommitTime}};
                        abort ->
                            {error, aborted};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                Error ->
                    {error, Error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


perform_read({Key, Type}, UpdatedPartitions, Sender) ->
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    WriteSet = case lists:keyfind(IndexNode, 1, UpdatedPartitions) of
                   false ->
                       [];
                   {IndexNode, WS} ->
                       WS
               end,
    lager:info("interactive_coord: about to read Key ~p from node ~p with type ~p and writeset ~p",[Key, IndexNode, Type, WriteSet]),
    case ?ec_VNODE:read_data_item(IndexNode, Key, Type, WriteSet) of
        {error, Reason} ->
            case Sender of
                undefined ->
                    {error, Reason};
                _ ->
                    gen_fsm:reply(Sender, {error, Reason})
            end;
        {ok, Snapshot} ->
            Type:value(Snapshot)
    end.


perform_update(Args, UpdatedPartitions, Sender) ->
    {Key, Type, Param} = Args,
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    WriteSet = case lists:keyfind(IndexNode, 1, UpdatedPartitions) of
                   false ->
                       [];
                   {IndexNode, WS} ->
                       WS
               end,
    case ?ec_DOWNSTREAM:generate_downstream_op(IndexNode, Key, Type, Param, WriteSet) of
        {ok, DownstreamRecord} ->
            NewUpdatedPartitions = case WriteSet of
                                       [] ->
                                           [{IndexNode, [{Key, Type, DownstreamRecord}]} | UpdatedPartitions];
                                       _ ->
                                           lists:keyreplace(IndexNode, 1, UpdatedPartitions,
                                               {IndexNode, [{Key, Type, DownstreamRecord} | WriteSet]})
                                   end,
            case Sender of
                undefined ->
                    ok;
                _ ->
                    gen_fsm:reply(Sender, ok)
            end,
            LogRecord = #log_record{op_type = update,
                op_payload = {Key, Type, DownstreamRecord}},
            LogId = ?LOG_UTIL:get_logid_from_key(Key),
            [Node] = Preflist,
            case ?LOGGING_VNODE:append(Node, LogId, LogRecord) of
                {ok, _} ->
                    NewUpdatedPartitions;
                Error ->
                    case Sender of
                        undefined ->
                            ok;
                        _ ->
                            _Res = gen_fsm:reply(Sender, {error, Error})
                    end,
                    {error, Error}
            end;
        {error, Reason} ->
            case Sender of
                undefined ->
                    ok;
                _ ->
                    _Res = gen_fsm:reply(Sender, {error, Reason})
            end,
            {error, Reason}
    end.


%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_op({OpType, Args}, Sender,
  SD0 = #tx_coord_state{
      updated_partitions = UpdatedPartitions
  }) ->
    case OpType of
        prepare ->
            prepare(SD0#tx_coord_state{from = Sender, commit_protocol = Args});
        read ->
            case perform_read(Args, UpdatedPartitions, Sender) of
                {error, _Reason} ->
                    abort(SD0);
                ReadResult ->
                    {reply, {ok, ReadResult}, execute_op, SD0}
            end;
        update ->
            case perform_update(Args, UpdatedPartitions, Sender) of
                {error, _Reason} ->
                    abort(SD0);
                NewUpdatedPartitions ->
                    {next_state, execute_op,
                        SD0#tx_coord_state{updated_partitions = NewUpdatedPartitions}}
            end
    end.


%% @doc this state sends a prepare message to all updated partitions and goes
%%      to the "receive_prepared"state.
prepare(SD0 = #tx_coord_state{
    updated_partitions = UpdatedPartitions, full_commit = FullCommit, from = From}) ->
    case UpdatedPartitions of
        [] ->
            case FullCommit of
                false ->
                    gen_fsm:reply(From, {ok, no_snapshot_time}),
                    {next_state, committing, SD0#tx_coord_state{state = committing, commit_time = no_commit_time}};
                true ->
                    reply_to_client(SD0#tx_coord_state{state = committed_read_only})
            end;
        [_] ->
            ok = ?ec_VNODE:single_commit(UpdatedPartitions),
            {next_state, single_committing,
                SD0#tx_coord_state{state = committing, num_to_ack = 1}};
        [_ | _] -> %% the transaction coordinator proposes imposes his commit time.
            PrepareTime = ec_vnode:now_microsec(erlang:now()),
            ok = ?ec_VNODE:prepare(UpdatedPartitions, PrepareTime),
            Num_to_ack = length(UpdatedPartitions),
            {next_state, receive_prepared,
                SD0#tx_coord_state{num_to_ack = Num_to_ack, state = prepared, prepare_time = PrepareTime, commit_time = PrepareTime}}
    end.

process_prepared(S0 = #tx_coord_state{num_to_ack = NumToAck,
    commit_protocol = CommitProtocol, full_commit = FullCommit,
    from = From, commit_time = CommitTime,
    updated_partitions = UpdatedPartitions}) ->
    case NumToAck of 1 ->
        case CommitProtocol of
            two_phase ->
                case FullCommit of
                    true ->
                        ok = ?ec_VNODE:commit(UpdatedPartitions, CommitTime),
                        {next_state, receive_committed,
                            S0#tx_coord_state{num_to_ack = NumToAck, state = committing}};
                    false ->
                        gen_fsm:reply(From, {ok, CommitTime}),
                        {next_state, committing_2pc,
                            S0#tx_coord_state{state = committing}}
                end;
            _ ->
                case FullCommit of
                    true ->
                        ok = ?ec_VNODE:commit(UpdatedPartitions, CommitTime),
                        {next_state, receive_committed,
                            S0#tx_coord_state{num_to_ack = NumToAck, state = committing}};
                    false ->
                        gen_fsm:reply(From, {ok, CommitTime}),
                        {next_state, committing,
                            S0#tx_coord_state{state = committing}}
                end
        end;
        _ ->
            {next_state, receive_prepared,
                S0#tx_coord_state{num_to_ack = NumToAck - 1}}
    end.


%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared({prepared}, S0) ->
    process_prepared(S0);

receive_prepared(abort, S0) ->
    abort(S0);

receive_prepared(timeout, S0) ->
    abort(S0).

single_committing({committed, CommitTime}, S0 = #tx_coord_state{from = From, full_commit = FullCommit}) ->
    case FullCommit of
        false ->
            gen_fsm:reply(From, {ok, CommitTime}),
            {next_state, committing_single,
                S0#tx_coord_state{state = committing}};
        true ->
            reply_to_client(S0#tx_coord_state{state = committed})
    end;

single_committing(abort, S0 = #tx_coord_state{from = _From}) ->
    abort(S0);

single_committing(timeout, S0 = #tx_coord_state{from = _From}) ->
    abort(S0).


%% @doc There was only a single partition with an update in this transaction
%%      so the transaction has already been committed
%%      so just wait for the commit message from the client
committing_single(commit, Sender, SD0) ->
    reply_to_client(SD0#tx_coord_state{from = Sender, state = committed}).

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state expects other process to sen the commit message to 
%%      start the commit phase.
committing_2pc(commit, Sender, SD0 = #tx_coord_state{
    updated_partitions = UpdatedPartitions,
    commit_time = Commit_time}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            reply_to_client(SD0#tx_coord_state{state = committed_read_only, from = Sender});
        _ ->
            ok = ?ec_VNODE:commit(UpdatedPartitions, Commit_time),
            {next_state, receive_committed,
                SD0#tx_coord_state{num_to_ack = NumToAck, from = Sender, state = committing}}
    end.

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected 
committing(commit, Sender, SD0 = #tx_coord_state{
    updated_partitions = UpdatedPartitions,
    commit_time = Commit_time}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            reply_to_client(SD0#tx_coord_state{state = committed_read_only, from = Sender});
        _ ->
            ok = ?ec_VNODE:commit(UpdatedPartitions, Commit_time),
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
abort(SD0 = #tx_coord_state{
    updated_partitions = UpdatedPartitions}) ->
    ok = ?ec_VNODE:abort(UpdatedPartitions),
    reply_to_client(SD0#tx_coord_state{state = aborted}).

abort(abort, SD0 = #tx_coord_state{
    updated_partitions = UpdatedPartitions}) ->
    ok = ?ec_VNODE:abort(UpdatedPartitions),
    reply_to_client(SD0#tx_coord_state{state = aborted});

abort({prepared, _}, SD0 = #tx_coord_state{
    updated_partitions = UpdatedPartitions}) ->
    ok = ?ec_VNODE:abort(UpdatedPartitions),
    reply_to_client(SD0#tx_coord_state{state = aborted});

abort(_, SD0 = #tx_coord_state{
    updated_partitions = UpdatedPartitions}) ->
    ok = ?ec_VNODE:abort(UpdatedPartitions),
    reply_to_client(SD0#tx_coord_state{state = aborted}).

%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
reply_to_client(SD = #tx_coord_state{from = From, read_set = ReadSet,
    state = TxState, commit_time = CommitTime,
    is_static = IsStatic}) ->
    if undefined =/= From ->
        Reply = case TxState of
                    committed_read_only ->
                        case IsStatic of
                            false ->
                                ok;
                            true ->
                                {ok, lists:reverse(ReadSet)}
                        end;
                    committed ->
                        case IsStatic of
                            false ->
                                {ok, CommitTime};
                            true ->
                                {ok, {lists:reverse(ReadSet), CommitTime}}
                        end;
                    aborted ->
                        {aborted};
                    Reason ->
                        Reason
                end,
        _Res = gen_fsm:reply(From, Reply);
        true -> ok
    end,
    {stop, normal, SD}.

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
%%-ifdef(TEST).
%%
%%main_test_() ->
%%    {foreach,
%%        fun setup/0,
%%        fun cleanup/1,
%%        [
%%            fun empty_prepare_test/1,
%%            fun timeout_test/1,
%%
%%            fun update_single_abort_test/1,
%%            fun update_single_success_test/1,
%%            fun update_multi_abort_test1/1,
%%            fun update_multi_abort_test2/1,
%%            fun update_multi_success_test/1,
%%
%%            fun read_single_fail_test/1,
%%            fun read_success_test/1,
%%
%%            fun downstream_fail_test/1
%%        ]}.
%%
%%% Setup and Cleanup
%%setup() ->
%%    {ok, Pid} = ec_interactive_tx_coord_fsm:start_link(self()),
%%    Pid.
%%cleanup(Pid) ->
%%    case process_info(Pid) of undefined -> io:format("Already cleaned");
%%        _ -> ec_interactive_tx_coord_fsm:stop(Pid) end.
%%
%%empty_prepare_test(Pid) ->
%%    fun() ->
%%        ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%    end.
%%
%%timeout_test(Pid) ->
%%    fun() ->
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {timeout, nothing, nothing}}, infinity)),
%%        ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%    end.
%%
%%update_single_abort_test(Pid) ->
%%    fun() ->
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
%%        ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%    end.
%%
%%update_single_success_test(Pid) ->
%%    fun() ->
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {single_commit, nothing, nothing}}, infinity)),
%%        ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%    end.
%%
%%update_multi_abort_test1(Pid) ->
%%    fun() ->
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
%%        ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%    end.
%%
%%update_multi_abort_test2(Pid) ->
%%    fun() ->
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
%%        ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%    end.
%%
%%update_multi_success_test(Pid) ->
%%    fun() ->
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
%%        ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
%%        ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%    end.
%%
%%read_single_fail_test(Pid) ->
%%    fun() ->
%%        ?assertEqual({error, mock_read_fail},
%%            gen_fsm:sync_send_event(Pid, {read, {read_fail, nothing}}, infinity))
%%    end.
%%
%%read_success_test(Pid) ->
%%    fun() ->
%%        ?assertEqual({ok, 2},
%%            gen_fsm:sync_send_event(Pid, {read, {counter, riak_dt_gcounter}}, infinity)),
%%        ?assertEqual({ok, [a]},
%%            gen_fsm:sync_send_event(Pid, {read, {set, riak_dt_gset}}, infinity)),
%%        ?assertEqual({ok, mock_value},
%%            gen_fsm:sync_send_event(Pid, {read, {mock_type, mock_partition_fsm}}, infinity)),
%%        ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%    end.
%%
%%downstream_fail_test(Pid) ->
%%    fun() ->
%%        ?assertEqual({error, mock_downstream_fail},
%%            gen_fsm:sync_send_event(Pid, {update, {downstream_fail, nothing, nothing}}, infinity))
%%    end.
%%
%%
%%
%%-endif.

