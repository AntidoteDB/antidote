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
%% @doc The coordinator for a given Clock SI static transaction.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. When a tx is finalized (committed or aborted), the fsm
%%      also finishes.

-module(clocksi_static_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/3,
         start_link/2]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([execute_batch_ops/2,  prepare/2, receive_prepared/2, 
         single_committing/2, committing/2, receive_committed/2, abort/2,
         reply_to_client/2]).


%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------
-record(state, {
          from :: pid(),
          transaction :: tx(),
          updated_partitions :: list(),
          num_to_ack :: integer(),
          prepare_time :: integer(),
          commit_time :: integer(),
          commit_protocol :: term(),
          read_set :: [term()],
          operations :: list(),
          state:: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, ClientClock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, ClientClock, Operations], []).

start_link(From, Operations) ->
    gen_fsm:start_link(?MODULE, [From, ignore, Operations], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, Operations]) ->
    {ok, SnapshotTime} = case ClientClock of
        ignore ->
            get_snapshot_time();
        _ ->
            get_snapshot_time(ClientClock)
    end,
    DcId = dc_utilities:get_my_dc_id(),
    {ok, LocalClock} = vectorclock:get_clock_of_dc(DcId, SnapshotTime),
    TransactionId = #tx_id{snapshot_time=LocalClock, server_pid=self()},
    Transaction = #transaction{snapshot_time=LocalClock,
                               vec_snapshot_time=SnapshotTime,
                               txn_id=TransactionId},
    SD = #state{
            from = From,
            transaction = Transaction,
            operations = Operations,
            updated_partitions=[],
            prepare_time=0
           },
    {ok, execute_batch_ops, SD, 0}.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%      to execute the next operation.
execute_batch_ops(timeout, SD=#state{from = From,
                                     transaction = Transaction,
                                     operations = Operations}) ->
    Result = perform_operations(Operations, Transaction, [], []), 
    case Result of
        {error, Reason} ->
            From ! {error, Reason},
            {stop, normal, SD};
        {ReadSet, UpdatedPartitions} ->
            {next_state, prepare, SD#state{read_set=ReadSet, updated_partitions=UpdatedPartitions}, 0}
    end.


%% @doc this state sends a prepare message to all updated partitions and goes
%%      to the "receive_prepared"state.
prepare(timeout, SD0=#state{
                        transaction = Transaction,
                        updated_partitions=Updated_partitions, from=_From}) ->
    case length(Updated_partitions) of
        0->
            Snapshot_time=Transaction#transaction.snapshot_time,
            {next_state, committing,
            SD0#state{state=committing, commit_time=Snapshot_time}, 0};
        1-> 
            clocksi_vnode:single_commit(Updated_partitions, Transaction),
            {next_state, single_committing,
            SD0#state{state=committing, num_to_ack=1}};
        _->
            clocksi_vnode:prepare(Updated_partitions, Transaction),
            Num_to_ack=length(Updated_partitions),
            {next_state, receive_prepared,
            SD0#state{num_to_ack=Num_to_ack, state=prepared}}
    end.


%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared({prepared, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck,
                           commit_protocol=CommitProtocol,
                           from=From, prepare_time=PrepareTime}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 1 ->
            case CommitProtocol of
            two_phase ->
                gen_fsm:reply(From, {ok, MaxPrepareTime}),
                {next_state, committing_2pc,
                S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, state=committing}};
            _ ->
                {next_state, committing,
                S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, state=committing}, 0}
            end;
        _ ->
            {next_state, receive_prepared,
             S0#state{num_to_ack= NumToAck-1, prepare_time=MaxPrepareTime}}
    end;

receive_prepared(abort, S0) ->
    {next_state, abort, S0, 0};

receive_prepared(timeout, S0) ->
    {next_state, abort, S0, 0}.

single_committing({committed, CommitTime}, S0=#state{from=_From}) ->
    {next_state, reply_to_client, S0#state{prepare_time=CommitTime, commit_time=CommitTime, state=committed}, 0}.
    

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected 
committing(timeout, SD0=#state{transaction = Transaction,
                              updated_partitions=Updated_partitions,
                              commit_time=Commit_time}) ->
    NumToAck=length(Updated_partitions),
    case NumToAck of
        0 ->
            {next_state, reply_to_client,
             SD0#state{state=committed},0};
        _ ->
            clocksi_vnode:commit(Updated_partitions, Transaction, Commit_time),
            {next_state, receive_committed,
             SD0#state{num_to_ack=NumToAck, state=committing}}
    end.


%% @doc the fsm waits for acks indicating that each partition has successfully
%%  committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_committed(committed, S0=#state{num_to_ack= NumToAck}) ->
    case NumToAck of
        1 ->
            {next_state, reply_to_client, S0#state{state=committed}, 0};
        _ ->
           {next_state, receive_committed, S0#state{num_to_ack= NumToAck-1}}
    end.

%% @doc when an error occurs or an updated partition 
%% does not pass the certification check, the transaction aborts.
abort(timeout, SD0=#state{transaction = Transaction,
                          updated_partitions=UpdatedPartitions}) ->
    clocksi_vnode:abort(UpdatedPartitions, Transaction),
    {next_state, reply_to_client, SD0#state{state=aborted},0};

abort(abort, SD0=#state{transaction = Transaction,
                        updated_partitions=UpdatedPartitions}) ->
    clocksi_vnode:abort(UpdatedPartitions, Transaction),
    {next_state, reply_to_client, SD0#state{state=aborted},0}.

%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
reply_to_client(timeout, SD=#state{from=From, transaction=Transaction, read_set=ReadSet,
                                   state=TxState, commit_time=CommitTime}) ->
    if undefined =/= From ->
        TxId = Transaction#transaction.txn_id,
        Reply = case TxState of
            committed ->
                DcId = dc_utilities:get_my_dc_id(),
                CausalClock = vectorclock:set_clock_of_dc(
                  DcId, CommitTime, Transaction#transaction.vec_snapshot_time),
                {ok, {TxId, ReadSet, CausalClock}};
            aborted->
                {error, commit_fail}
                end,
        From ! Reply
    end,
    {stop, normal, SD}.



%% =============================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

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
-spec get_snapshot_time(ClientClock :: vectorclock:vectorclock())
                       -> {ok, vectorclock:vectorclock()} | {error,term()}.
get_snapshot_time(ClientClock) ->
    wait_for_clock(ClientClock).


perform_operations([], _Transaction, ReadSet, Partitions) ->
    {ReadSet, Partitions};
perform_operations([{read, Key, Type}|Rest], Transaction, ReadSet, Partitions) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case clocksi_vnode:read_data_item(IndexNode, Transaction, Key, Type) of
        {error, Reason} ->
            {error, Reason};
        {ok, Snapshot} ->
            Value = Type:value(Snapshot),
            perform_operations(Rest, Transaction, ReadSet++[Value], Partitions)
    end;
perform_operations([{update, Key, Type, Param}|Rest], Transaction, ReadSet, Partitions) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case generate_downstream_op(Transaction, IndexNode, Key, Type, Param) of
        {ok, DownstreamRecord} ->
            case clocksi_vnode:update_data_item(IndexNode, Transaction,
                Key, Type, DownstreamRecord) of
                ok ->
                    case lists:member(IndexNode, Partitions) of
                        false ->
                            perform_operations(Rest, Transaction, ReadSet, Partitions++[IndexNode]);
                        true->
                            perform_operations(Rest, Transaction, ReadSet, Partitions)
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


-spec get_snapshot_time() -> {ok, vectorclock:vectorclock()} | {error, term()}.
get_snapshot_time() ->
    Now = clocksi_vnode:now_microsec(erlang:now()),
    case vectorclock:get_stable_snapshot() of
        {ok, VecSnapshotTime} ->
            DcId = dc_utilities:get_my_dc_id(),
            SnapshotTime = dict:update(DcId,
                                       fun (_Old) -> Now end,
                                       Now, VecSnapshotTime),
            {ok, SnapshotTime};
        {error, Reason} ->
            {error, Reason}
    end.

-spec wait_for_clock(Clock :: vectorclock:vectorclock()) ->
                           {ok, vectorclock:vectorclock()} | {error, term()}.
wait_for_clock(Clock) ->
   case get_snapshot_time() of
       {ok, VecSnapshotTime} ->
           case vectorclock:ge(VecSnapshotTime, Clock) of
               true ->
                   %% No need to wait
                   {ok, VecSnapshotTime};
               false ->
                   %% wait for snapshot time to catch up with Client Clock
                   timer:sleep(10),
                   wait_for_clock(Clock)
           end;
       {error, Reason} ->
          {error, Reason}
  end.

generate_downstream_op(Txn, IndexNode, Key, Type, Param) ->
    clocksi_downstream:generate_downstream_op(Txn, IndexNode, Key, Type, Param).

