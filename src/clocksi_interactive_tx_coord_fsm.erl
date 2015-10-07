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
-include_lib("eunit/include/eunit.hrl").
-define(DC_UTIL, mock_partition_fsm).
-define(VECTORCLOCK, mock_partition_fsm).
-define(LOG_UTIL, mock_partition_fsm).
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(CLOCKSI_DOWNSTREAM, mock_partition_fsm).
-define(LOGGING_VNODE, mock_partition_fsm).
-else.
-define(DC_UTIL, dc_utilities).
-define(VECTORCLOCK, vectorclock).
-define(LOG_UTIL, log_utilities).
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(CLOCKSI_DOWNSTREAM, clocksi_downstream).
-define(LOGGING_VNODE, logging_vnode).
-endif.


%% API
-export([start_link/2,
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
-export([create_transaction_record/1,
	 perform_update/5,
	 perform_read/5,
	 execute_op/3,
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
	 abort/1,
	 abort/2,
	 perform_singleitem_read/2,
	 perform_singleitem_update/3,
         reply_to_client/1]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock], []).

start_link(From) ->
    gen_fsm:start_link(?MODULE, [From, ignore], []).

finish_op(From, Key, Result) ->
    gen_fsm:send_event(From, {Key, Result}).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid, stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock]) ->
    {Transaction, TransactionId} = create_transaction_record(ClientClock),
    SD = #tx_coord_state{
            transaction = Transaction,
	    external_snapshots = [],
            updated_partitions=[],
            prepare_time=0,
	    full_commit=false,
	    is_static=false
           },
    From ! {ok, TransactionId},
    {ok, execute_op, SD}.

-spec create_transaction_record(snapshot_time() | ignore) -> {tx(), txid()}.
create_transaction_record(ClientClock) ->
    {ok, SnapshotTime} = case ClientClock of
			     ignore ->
				 get_snapshot_time(vectorclock:new(),localTransaction);
			     _ ->
				 get_snapshot_time(ClientClock,localTransaction)
			 end,
    DcId = ?DC_UTIL:get_my_dc_id(),
    {ok, LocalClock} = ?VECTORCLOCK:get_clock_of_dc(DcId, SnapshotTime),
    TransactionId = #tx_id{snapshot_time = LocalClock, server_pid = self()},
    Transaction = #transaction{snapshot_time = LocalClock,
        vec_snapshot_time = SnapshotTime,
        txn_id = TransactionId},
    {Transaction, TransactionId}.

%% @doc This is a standalone function for directly contacting the read
%%      server located at the vnode of the key being read.  This read
%%      is supposed to be light weight because it is done outside of a
%%      transaction fsm and directly in the calling thread.
-spec perform_singleitem_read(key(), type()) -> {ok, val()} | {error, reason()}.
perform_singleitem_read(Key, Type) ->
    {Transaction, _TransactionId} = create_transaction_record(ignore),
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case clocksi_vnode:read_data_item(IndexNode, Transaction, Key, Type, [], []) of
	{error, Reason} ->
	    {error, Reason};
	{ok, _Snapshot, ResultSnapshot} ->
	    ReadResult = Type:value(ResultSnapshot),
	    {ok, ReadResult}
    end.


%% @doc This is a standalone function for directly contacting the update
%%      server vnode.  This is lighter than creating a transaction
%%      because the update/prepare/commit are all done at one time
-spec perform_singleitem_update(key(), type(), {op(), term()}) -> {ok, {txid(), [], snapshot_time()}} | {error, term()}.
perform_singleitem_update(Key, Type, Params) ->
    {Transaction, _TransactionId} = create_transaction_record(ignore),
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    Result = case replication_check:is_replicated_here(Key) of
		 true ->
		     case ?CLOCKSI_DOWNSTREAM:generate_downstream_op(Transaction, IndexNode, Key, Type, Params, [], [], local) of
			 {ok, Ds} ->
			     {ok, Ds, isReplicated, update};
			 {error, Res} ->
			     {error, Res}
		     end;
		 false ->
		     {ok, Params, notReplicated, nonRepUpdate}
	     end,
    case Result of
	{ok, DownstreamRecord, Replicated, OpType} ->
	    Updated_partitions = [{IndexNode,[{Replicated,Key,Type,DownstreamRecord}]}],
	    TxId = Transaction#transaction.txn_id,
	    LogRecord = #log_record{tx_id=TxId, op_type=OpType,
				    op_payload={Key, Type, DownstreamRecord}},
	    LogId = ?LOG_UTIL:get_logid_from_key(Key),
	    case ?LOGGING_VNODE:append(IndexNode,LogId,LogRecord) of
		{ok, _} ->
		    case ?CLOCKSI_VNODE:single_commit_sync(Updated_partitions, Transaction) of
			{committed, CommitTime} ->
			    TxId = Transaction#transaction.txn_id,
			    DcId = ?DC_UTIL:get_my_dc_id(),
			    CausalClock = ?VECTORCLOCK:set_clock_of_dc(
					     DcId, CommitTime, Transaction#transaction.vec_snapshot_time),
			    {ok, {TxId, [], CausalClock}};
			{error, Reason} ->
			    {error, Reason}
		    end;
		Error ->
		    {error, Error}
	    end;
	{error, Reason} ->	    
	    {error,Reason}
    end.


perform_read(Args,Updated_partitions,ExternalReads,Transaction,Sender) ->
    {Key, Type}=Args,
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    WriteSet = case lists:keyfind(IndexNode, 1, Updated_partitions) of
		   false ->
		       [];
		   {IndexNode, WS} ->
		       WS
	       end,
    case ?CLOCKSI_VNODE:read_data_item(IndexNode, Transaction, Key, Type, WriteSet,ExternalReads) of
	{error, Reason} ->
	    case Sender of
		undefined ->
		    ok;
		_ ->
		    _Res = gen_fsm:reply(Sender, {error, Reason})
	    end,
	    {error, Reason};
	%% {ok, Snapshot, internal} ->
	%%     {ok, Type:value(Snapshot), ExternalReads};
	%% {ok, Snapshot, ResultSnapshot, external} ->
	{ok, Snapshot, ResultSnapshot} ->
	    {ok, Type:value(ResultSnapshot), lists:keystore(Key,1,ExternalReads,{Key,Snapshot})}
    end.


perform_update(Args,Updated_partitions,ExternalReads,Transaction,Sender) ->
    {Key, Type, Param}=Args,
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    WriteSet = case lists:keyfind(IndexNode, 1, Updated_partitions) of
		   false ->
		       [];
		   {IndexNode, WS} ->
		       WS
	       end,
    Result =
	case replication_check:is_replicated_here(Key) or
	    lists:keymember(Key,1,ExternalReads) of
	    true ->
		case ?CLOCKSI_DOWNSTREAM:generate_downstream_op(Transaction, IndexNode, Key, Type, Param, WriteSet, ExternalReads, local) of
		    {ok, Ds} ->
			{ok, Ds, isReplicated, update};
		    {error, Res} ->
			{error, Res}
		end;
	    false ->
		{ok, Param, notReplicated, nonRepUpdate}
	end,
    Result2 = case Result of
		  {ok, DownstreamRecord, Replicated, OpType} ->
		      NewUpdatedPartitions =
			  case WriteSet of
			      [] ->
				  [{IndexNode,[{Replicated,Key,Type,DownstreamRecord}]}|Updated_partitions];
			      _ ->
				  lists:keyreplace(IndexNode,1,Updated_partitions,
						   {IndexNode,[{Replicated,Key,Type,DownstreamRecord}|WriteSet]})
			  end,
		      case Sender of
			  undefined ->
			      ok;
			  _ ->
			      gen_fsm:reply(Sender, ok)
		      end,
		      TxId = Transaction#transaction.txn_id,
		      LogRecord = #log_record{tx_id=TxId, op_type=OpType,
					      op_payload={Key, Type, DownstreamRecord}},
		      LogId = ?LOG_UTIL:get_logid_from_key(Key),
		      case ?LOGGING_VNODE:append(IndexNode,LogId,LogRecord) of
			  {ok, _} ->
			      {ok, NewUpdatedPartitions};
			  Error ->
			      {error, Error}
		      end;
		  Err ->
		      Err
	      end,
    case Result2 of
	{error, Err2} ->
	    case Sender of
		undefined ->
		    ok;
		_ ->
		    _Res = gen_fsm:reply(Sender, {error, Err2})
	    end,
	    {error, Err2};
	{ok, UpdatedPart} ->
	    {ok, UpdatedPart}
    end.
	    


%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_op({OpType, Args}, Sender,
           SD0=#tx_coord_state{transaction=Transaction,
			       external_snapshots=ExternalReads,
			       updated_partitions=Updated_partitions
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
            case perform_read(Args,Updated_partitions,ExternalReads,Transaction,Sender) of
                {error, _Reason} ->
		    abort(SD0);
                {ok, ReadResult,NewExternal} ->
                    {reply, {ok, ReadResult}, execute_op, SD0#tx_coord_state{external_snapshots=NewExternal}}
	    end;
        update ->
	    case perform_update(Args,Updated_partitions,ExternalReads,Transaction,Sender) of
		{error, _Reason} ->
		    abort(SD0);
		{ok, NewUpdatedPartitions} ->
		    {next_state, execute_op,
		     SD0#tx_coord_state{updated_partitions= NewUpdatedPartitions}}
	    end
    end.

%% @doc this state sends a prepare message to all updated partitions and goes
%%      to the "receive_prepared"state.
prepare(SD0 = #tx_coord_state{
    transaction = Transaction,
    updated_partitions = Updated_partitions, full_commit = FullCommit, from = From}) ->
    case length(Updated_partitions) of
        0 ->
            Snapshot_time = Transaction#transaction.snapshot_time,
            case FullCommit of
                false ->
                    gen_fsm:reply(From, {ok, Snapshot_time}),
                    {next_state, committing, SD0#tx_coord_state{state = committing, commit_time = Snapshot_time}};
                true ->
                    reply_to_client(SD0#tx_coord_state{state = committed_read_only})
            end;
        1 ->
            ok = ?CLOCKSI_VNODE:single_commit(Updated_partitions, Transaction),
            {next_state, single_committing,
                SD0#tx_coord_state{state = committing, num_to_ack = 1}};
        _ ->
            ok = ?CLOCKSI_VNODE:prepare(Updated_partitions, Transaction),
            Num_to_ack = length(Updated_partitions),
            {next_state, receive_prepared,
                SD0#tx_coord_state{num_to_ack = Num_to_ack, state = prepared}}
    end.

%% @doc state called when 2pc is forced independently of the number of partitions
%%      involved in the txs.
prepare_2pc(SD0 = #tx_coord_state{
    transaction = Transaction,
    updated_partitions = Updated_partitions, full_commit = FullCommit, from = From}) ->
    case length(Updated_partitions) of
        0 ->
            Snapshot_time = Transaction#transaction.snapshot_time,
            case FullCommit of
                false ->
                    gen_fsm:reply(From, {ok, Snapshot_time}),
                    {next_state, committing_2pc,
                        SD0#tx_coord_state{state = committing, commit_time = Snapshot_time}};
                true ->
                    reply_to_client(SD0#tx_coord_state{state = committed_read_only})
            end;
        _ ->
            ok = ?CLOCKSI_VNODE:prepare(Updated_partitions, Transaction),
            Num_to_ack = length(Updated_partitions),
            {next_state, receive_prepared,
                SD0#tx_coord_state{num_to_ack = Num_to_ack, state = prepared}}
    end.

process_prepared(ReceivedPrepareTime, S0 = #tx_coord_state{num_to_ack = NumToAck,
    commit_protocol = CommitProtocol, full_commit = FullCommit,
    from = From, prepare_time = PrepareTime,
    transaction = Transaction,
    updated_partitions = Updated_partitions}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 1 ->
        case CommitProtocol of
            two_phase ->
                case FullCommit of
                    true ->
                        ok = ?CLOCKSI_VNODE:commit(Updated_partitions, Transaction, MaxPrepareTime),
                        {next_state, receive_committed,
                            S0#tx_coord_state{num_to_ack = NumToAck, commit_time = MaxPrepareTime, state = committing}};
                    false ->
                        gen_fsm:reply(From, {ok, MaxPrepareTime}),
                        {next_state, committing_2pc,
                            S0#tx_coord_state{prepare_time = MaxPrepareTime, commit_time = MaxPrepareTime, state = committing}}
                end;
            _ ->
                case FullCommit of
                    true ->
                        ok = ?CLOCKSI_VNODE:commit(Updated_partitions, Transaction, MaxPrepareTime),
                        {next_state, receive_committed,
                            S0#tx_coord_state{num_to_ack = NumToAck, commit_time = MaxPrepareTime, state = committing}};
                    false ->
                        gen_fsm:reply(From, {ok, MaxPrepareTime}),
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
    commit_time = Commit_time}) ->
    NumToAck = length(Updated_partitions),
    case NumToAck of
        0 ->
            reply_to_client(SD0#tx_coord_state{state = committed_read_only, from = Sender});
        _ ->
            ok = ?CLOCKSI_VNODE:commit(Updated_partitions, Transaction, Commit_time),
            {next_state, receive_committed,
                SD0#tx_coord_state{num_to_ack = NumToAck, from = Sender, state = committing}}
    end.

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected 
committing(commit, Sender, SD0 = #tx_coord_state{transaction = Transaction,
    updated_partitions = Updated_partitions,
    commit_time = Commit_time}) ->
    NumToAck = length(Updated_partitions),
    case NumToAck of
        0 ->
            reply_to_client(SD0#tx_coord_state{state = committed_read_only, from = Sender});
        _ ->
            ok = ?CLOCKSI_VNODE:commit(Updated_partitions, Transaction, Commit_time),
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
    ok = ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
    reply_to_client(SD0#tx_coord_state{state = aborted}).

abort(abort, SD0 = #tx_coord_state{transaction = Transaction,
    updated_partitions = UpdatedPartitions}) ->
    ok = ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
    reply_to_client(SD0#tx_coord_state{state = aborted});

abort({prepared, _}, SD0 = #tx_coord_state{transaction = Transaction,
    updated_partitions = UpdatedPartitions}) ->
    ok = ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
    reply_to_client(SD0#tx_coord_state{state = aborted});

abort(_, SD0 = #tx_coord_state{transaction = Transaction,
    updated_partitions = UpdatedPartitions}) ->
    ok = ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
    reply_to_client(SD0#tx_coord_state{state = aborted}).


%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
reply_to_client(SD = #tx_coord_state{from = From, transaction = Transaction, read_set = ReadSet,
    state = TxState, commit_time = CommitTime,
    is_static = IsStatic}) ->
    if undefined =/= From ->
        TxId = Transaction#transaction.txn_id,
        Reply = case TxState of
                    committed_read_only ->
                        case IsStatic of
                            false ->
                                {ok, {TxId, Transaction#transaction.vec_snapshot_time}};
                            true ->
                                {ok, {TxId, lists:reverse(ReadSet), Transaction#transaction.vec_snapshot_time}}
                        end;
                    committed ->
                        DcId = ?DC_UTIL:get_my_dc_id(),
                        CausalClock = ?VECTORCLOCK:set_clock_of_dc(
                            DcId, CommitTime, Transaction#transaction.vec_snapshot_time),
                        case IsStatic of
                            false ->
                                {ok, {TxId, CausalClock}};
                            true ->
                                {ok, {TxId, lists:reverse(ReadSet), CausalClock}}
                        end;
                    aborted ->
                        {aborted, TxId};
                    Reason ->
                        {TxId, Reason}
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

%%@doc Set the transaction Snapshot Time to the maximum value of:
%%     1.ClientClock, which is the last clock of the system the client
%%       starting this transaction has seen, and
%%     2.machine's local time, as returned by erlang:now().
%% In parital replication, this should only be called when doing a
%% read coming from an external DC

-spec get_snapshot_time(ClientClock :: vectorclock:vectorclock(), term())
                       -> {ok, vectorclock:vectorclock()} | {error,term()}.
get_snapshot_time(ClientClock, externalTransaction) ->
    vectorclock:wait_for_clock(ClientClock),
    get_snapshot_time(ClientClock, localTransaction);
%% This should update the safe_time of the vectorclock vnode as well
%% The reason is that any advances in the clock that have happened
%% locally are already safe to read because it means the safe clock
%% was already updated somewhere in this DC

get_snapshot_time(ClientClock, localTransaction) ->
    DcId = ?DC_UTIL:get_my_dc_id(),
    vectorclock:wait_for_local_clock(ClientClock, DcId),
    case vectorclock:update_safe_vector_local(ClientClock) of
	{ok, VecSnapshotTime} ->
	    Clock = vectorclock:now_microsec_behind(ClientClock, VecSnapshotTime, erlang:now(), DcId),
	    {ok, Clock};
	{error, Reason} ->
	    lager:error("Error getting snapshot time ~p", [Reason]),
	    {error, Reason}
    end.

%% generate_downstream_op(Txn, Key, Type, Param, WriteSet, ExternalReads) ->
%%     clocksi_downstream:generate_downstream_op(Txn, Key, Type, Param, WriteSet, local, ExternalReads).


%% -spec get_snapshot_time(snapshot_time())
%%                        -> {ok, snapshot_time()}.
%% get_snapshot_time(ClientClock) ->
%%     wait_for_clock(ClientClock).

%% -spec get_snapshot_time() -> {ok, snapshot_time()}.
%% get_snapshot_time() ->
%%     Now = clocksi_vnode:now_microsec(erlang:now()) - ?OLD_SS_MICROSEC,
%%     {ok, VecSnapshotTime} = ?VECTORCLOCK:get_stable_snapshot(),
%%     DcId = ?DC_UTIL:get_my_dc_id(),
%%     SnapshotTime = dict:update(DcId,
%% 			       fun (_Old) -> Now end,
%% 			       Now, VecSnapshotTime),
    
%%     {ok, SnapshotTime}.


%% -spec wait_for_clock(snapshot_time()) ->
%%                            {ok, snapshot_time()}.
%% wait_for_clock(Clock) ->
%%     {ok, VecSnapshotTime} = get_snapshot_time(),
%%     case vectorclock:ge(VecSnapshotTime, Clock) of
%% 	true ->
%% 	    %% No need to wait
%% 	    {ok, VecSnapshotTime};
%% 	false ->
%% 	    %% wait for snapshot time to catch up with Client Clock
%% 	    timer:sleep(10),
%% 	    wait_for_clock(Clock)
%%     end.


-ifdef(TEST).

%% main_test_() ->
%%     {foreach,
%%      fun setup/0,
%%      fun cleanup/1,
%%      [
%%       fun empty_prepare_test/1,
%%       fun timeout_test/1,

%%       fun update_single_abort_test/1,
%%       fun update_single_success_test/1,
%%       fun update_multi_abort_test1/1,
%%       fun update_multi_abort_test2/1,
%%       fun update_multi_success_test/1,

%%       fun read_single_fail_test/1,
%%       fun read_success_test/1,

%%       fun downstream_fail_test/1,
%%       %% fun get_snapshot_time_test/0,
%%       fun wait_for_clock_test/0
%%      ]}.

%% % Setup and Cleanup
%% setup()      -> {ok,Pid} = clocksi_interactive_tx_coord_fsm:start_link(self(), ignore), Pid. 
%% cleanup(Pid) -> case process_info(Pid) of undefined -> io:format("Already cleaned");
%%                                            _ -> clocksi_interactive_tx_coord_fsm:stop(Pid) end.

%% empty_prepare_test(Pid) ->
%%     fun() ->
%%             ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%     end.

%% timeout_test(Pid) ->
%%     fun() ->
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {timeout, nothing, nothing}}, infinity)),
%%             ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%     end.

%% update_single_abort_test(Pid) ->
%%     fun() ->
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
%%             ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%     end.

%% update_single_success_test(Pid) ->
%%     fun() ->
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {single_commit, nothing, nothing}}, infinity)),
%%             ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%     end.

%% update_multi_abort_test1(Pid) ->
%%     fun() ->
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
%%             ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%     end.

%% update_multi_abort_test2(Pid) ->
%%     fun() ->
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, nothing, nothing}}, infinity)),
%%             ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%     end.

%% update_multi_success_test(Pid) ->
%%     fun() ->
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
%%             ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, nothing, nothing}}, infinity)),
%%             ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%     end.

%% read_single_fail_test(Pid) ->
%%     fun() ->
%%             ?assertEqual({error, mock_read_fail}, 
%%                     gen_fsm:sync_send_event(Pid, {read, {read_fail, nothing}}, infinity))
%%     end.

%% read_success_test(Pid) ->
%%     fun() ->
%%             ?assertEqual({ok, 2}, 
%%                     gen_fsm:sync_send_event(Pid, {read, {counter, riak_dt_gcounter}}, infinity)),
%%             ?assertEqual({ok, [a]}, 
%%                     gen_fsm:sync_send_event(Pid, {read, {set, riak_dt_gset}}, infinity)),
%%             ?assertEqual({ok, mock_value}, 
%%                     gen_fsm:sync_send_event(Pid, {read, {mock_type, mock_partition_fsm}}, infinity)),
%%             ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
%%     end.

%% downstream_fail_test(Pid) ->
%%     fun() ->
%%             ?assertEqual({error, mock_downstream_fail}, 
%%                     gen_fsm:sync_send_event(Pid, {update, {downstream_fail, nothing, nothing}}, infinity))
%%     end.


%% %% get_snapshot_time_test() ->
%% %%     {ok, SnapshotTime} = get_snapshot_time(vectorclock:new(),localTransaction),
%% %%     ?assertMatch([{mock_dc,_}],dict:to_list(SnapshotTime)).

%% wait_for_clock_test() ->
%%     {ok, SnapshotTime} = vectorclock:wait_for_local_clock(vectorclock:from_list([{mock_dc,10}]), ?DC_UTIL:get_my_dc_id()),
%%     ?assertMatch([{mock_dc,_}],dict:to_list(SnapshotTime)),
%%     VecClock = clocksi_vnode:now_microsec(now()),
%%     {ok, SnapshotTime2} = vectorclock:wait_for_local_clock(vectorclock:from_list([{mock_dc, VecClock}]), ?DC_UTIL:get_my_dc_id()),
%%     ?assertMatch([{mock_dc,_}],dict:to_list(SnapshotTime2)).


-endif.


