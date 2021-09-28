%%%-------------------------------------------------------------------
%%% @author ayush
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Sep 2021 4:05 PM
%%%-------------------------------------------------------------------
-module(clocksi_interactive_coord).
-author("ayush").
-include("antidote.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_statem).

%% API
-export([perform_static_operation/4]).
-export([start_link/0]).

%% gen_statem callbacks
-export([
  init/1,
  wait_for_start_transaction/3,
  format_status/2,
  terminate/3,
  code_change/4,
  callback_mode/0,
  stop/1
]).
%% States
-export([execute_op/3, execute_commit/3, receive_prepared/3, receive_logging_responses/3, receive_committed/3,receive_aborted/3, receive_read_objects_result/3]).

-define(SERVER, ?MODULE).
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
  partition_writesets :: list(),
  client_ops :: list(), % list of upstream updates, used for post commit hooks
  num_ack_pending :: non_neg_integer(),
  num_read_pending :: non_neg_integer(),
  num_agents_affected :: non_neg_integer(),
  prepare_time :: undefined | clock_time(),
  commit_time :: undefined | clock_time(),
  commit_protocol :: term(),
  state :: active | prepared | committing
  | committed | committed_read_only
  | undefined | aborted,
  operations :: undefined | list() | {update_objects, list()},
  return_accumulator :: list() | ok | {error, reason()},
  is_static :: boolean(),
  properties :: txn_properties(), commit_type_required:: read_only | normal
}).

-type state() :: #state{}.

%%%===================================================================
%%% Static operations
%%%===================================================================

%% @doc This is a standalone function for directly contacting the read
%%      server located at the vnode of the key being read.  This read
%%      is supposed to be light weight because it is done outside of a
%%      transaction fsm and directly in the calling thread.
%%      It either returns the object value or the object state.
-spec perform_static_operation(snapshot_time() | ignore, key(), type(), clocksi_readitem:read_property_list()) ->
  {ok, val() | term(), snapshot_time()} | {error, reason()}.
perform_static_operation(Clock, Key, Type, Properties) ->
  Transaction = clocksi_interactive_coord_helpers:create_transaction_record(Clock, true, Properties),
  Preflist = antidote_riak_utilities:get_preflist_from_key(Key),
  IndexNode = hd(Preflist),
  case clocksi_readitem:read_data_item(IndexNode, Key, Type, Transaction, []) of
    {error, Reason} ->
      {error, Reason};
    {ok, Snapshot} ->
        CommitTime = Transaction#transaction.vec_snapshot_time,
      {ok, Snapshot, CommitTime}
  end.


%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([]) ->
  {ok, wait_for_start_transaction, #state{}}.


% This is the state in which the coordinator lives when started, Until a call is received for the start_tx.
% After which it moves into the execute_op state.

wait_for_start_transaction({call, Sender}, {start_tx, ClientClock, Properties}, _State) ->
  BaseState = init_state(Properties),
  {ok, TransactionRecord} = start_tx_internal(ClientClock, Properties),
  TxnId = TransactionRecord#transaction.txn_id,
  {next_state, execute_op, BaseState#state{transaction = TransactionRecord}, {reply, Sender, {ok, TxnId}}}.



%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
%% internal state timeout
-spec execute_op({call, gen_statem:from()}, {update_objects | read_objects | read | abort | prepare, list()}, state()) -> gen_statem:event_handler_result(state()).
% Invoked for read, update, perpare, commit etc and a relevant internal callback is triggered using execute command.
execute_op({call, Sender}, {OpType, Args}, State) ->
  execute_command(OpType, Args, Sender, State).

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
%% internal state timeout
-spec execute_commit({call, gen_statem:from()}, {commit, non_neg_integer()}, state()) -> gen_statem:event_handler_result(state()).
% Invoked for read, update, perpare, commit etc and a relevant internal callback is triggered using execute command.
execute_commit({call, Sender}, {commit, PrepareTime}, State) ->
    execute_command(commit, PrepareTime, Sender, State).


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

%%%== receive_committed

%% @doc the fsm waits for acks indicating that each partition has successfully
%%      committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_committed(cast, committed, State = #state{num_ack_pending = NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(State#state{state = committed});
        _ ->
            {next_state, receive_committed, State#state{num_ack_pending = NumToAck - 1}}
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_committed(info, {_EventType, EventValue}, State) ->
    receive_committed(cast, EventValue, State).


%%%== receive_aborted

%% @doc the fsm waits for acks indicating that each partition has successfully
%%      aborted the tx and finishes operation.
%%      Should we retry sending the aborted message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_aborted(cast, aborted, State = #state{num_ack_pending = NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(State#state{state = aborted});
        _ ->
            {next_state, receive_aborted, State#state{num_ack_pending = NumToAck - 1}}
    end;

receive_aborted(cast, _, State) -> {next_state, receive_aborted, State};

%% capture regular events (e.g. logging_vnode responses)
receive_aborted(info, {_EventType, EventValue}, State) ->
    receive_aborted(cast, EventValue, State).





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
    num_agents_affected = NumToReply,
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
              num_agents_affected = NumToReply - 1,
            return_accumulator=NewAcc
          }};

        false ->
          case NewAcc of
            ok ->
              {next_state, execute_op, State#state{num_agents_affected = 0, return_accumulator=[]},
                [{reply, State#state.from, NewAcc}]};
            _ ->
              abort(State)
          end
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_logging_responses(info, {_EventType, EventValue}, State) ->
  receive_logging_responses(cast, EventValue, State).



%%%== receive_read_objects_result

%% @doc After asynchronously reading a batch of keys, collect the responses here
receive_read_objects_result(cast, {ok, {Key, _Type, Snapshot}}, CoordState = #state{
    num_read_pending = NumToRead,
    return_accumulator = ReadKeys
}) ->

    %% Swap keys with their appropriate read values
    ReadValues = replace_key_with_snapshot(ReadKeys, Key, Snapshot),

    %% Loop back to the same state until we process all the replies
    case NumToRead > 1 of
        true ->
            {next_state, receive_read_objects_result, CoordState#state{
                num_read_pending = NumToRead - 1,
                return_accumulator = ReadValues
            }};
        false ->
            {next_state, execute_op, CoordState#state{num_read_pending = 0},
                [{reply, CoordState#state.from, {ok, lists:reverse(ReadValues)}}]}
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_read_objects_result(info, {_EventType, EventValue}, State) ->
    receive_read_objects_result(cast, EventValue, State).



stop(Pid) -> gen_statem:stop(Pid).

%%%===================================================================
%%% Command Execution
%%%===================================================================

%% @doc Execute the commit protocol
-spec execute_command(atom(), term(), gen_statem:from(), state()) -> gen_statem:event_handler_result(state()).
execute_command(prepare, CommitProtocol, Sender, State0) ->
  State = State0#state{from=Sender, commit_protocol= CommitProtocol},
  prepare(State);

%% @doc Read a batch of objects, asynchronous
execute_command(read_objects, Objects, Sender, State = #state{transaction=Transaction}) ->
    ExecuteReads = fun({Key, Type}, AccState) ->
        ?STATS(operation_read_async),
        Partition = antidote_riak_utilities:get_key_partition(Key),
        % This call is forwarded to gingko through clocksi_readitem.
        ok = clocksi_readitem:async_read_data_item(Partition, Key, Type,Transaction, {fsm, self()}),
        ReadKeys = AccState#state.return_accumulator,
        AccState#state{return_accumulator=[Key | ReadKeys]}
                   end,

    NewCoordState = lists:foldl(
        ExecuteReads,
        State#state{num_read_pending = length(Objects), return_accumulator=[]},
        Objects
    ),

    {next_state, receive_read_objects_result, NewCoordState#state{from = Sender}};


%% @doc Perform update operations on a batch of Objects
execute_command(update_objects, UpdateOps, Sender, State = #state{transaction=Transaction}) ->
  ExecuteUpdates =
    fun(Op, AccState=#state{ client_ops = ClientOps0,partition_writesets = UpdatedPartitions0}) ->
      case perform_update(Op, UpdatedPartitions0, Transaction, Sender, ClientOps0) of
        {error, _} = Err ->
          AccState#state{return_accumulator = Err};
        {UpdatedPartitions, ClientOps} ->
          NumAgentsAffected = AccState#state.num_agents_affected,
          AccState#state{
            client_ops=ClientOps,
            num_agents_affected=NumAgentsAffected + 1,
              partition_writesets=UpdatedPartitions
          }
      end
    end,
  % Folds on a list of updates and executes them one at a time.
  NewCoordState = lists:foldl(
    ExecuteUpdates,
    State#state{num_agents_affected=0, return_accumulator=ok},
    UpdateOps
  ),
  LoggingState = NewCoordState#state{from=Sender},

  case LoggingState#state.num_agents_affected > 0 of
    true ->
      {next_state, receive_logging_responses, LoggingState};
    false ->
      {next_state, receive_logging_responses, LoggingState, [{state_timeout, 0, timeout}]}
  end;


execute_command(commit, _PrepareTime, Sender, State = #state{
    commit_type_required = CommitType,
    partition_writesets = PartitionsAffected,
    transaction = Transaction,
    prepare_time = PrepareTime}) ->
    case CommitType of
        read_only ->
            reply_to_client(State#state{from = Sender, state = committed_read_only});
        normal ->
            clocksi_vnode:commit(PartitionsAffected, Transaction, PrepareTime),
            {next_state, receive_committed,
                State#state{
                    from = Sender,
                    num_ack_pending = length(PartitionsAffected),
                    commit_time = PrepareTime,
                    state = committing}}
    end;

%% @doc Abort the current transaction
execute_command(abort, _Protocol, Sender, State) ->
    abort(State#state{from=Sender}).
%%%===================================================================
%%% API
%%%===================================================================

%% @doc Creates a gen_statem process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
% called by clocksi_interactive_coord_sup:start_fm
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_statem:start_link(?MODULE, [], []).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%% @doc this function sends a prepare message to all updated partitions and goes
%%      to the "receive_prepared"state.
-spec prepare(state()) -> gen_statem:event_handler_result(state()).
prepare(State = #state{
    from = From,
    transaction=Transaction,
    partition_writesets = UpdatedPartitions
}) ->
  case UpdatedPartitions of
    [] ->
        PrepareTime = dc_utilities:now_microsec(),
        send_prepared_ack(From, PrepareTime),
        {next_state, execute_commit, State#state{from = From, num_ack_pending = 0, prepare_time = PrepareTime, state= prepared, commit_type_required = read_only}};
    [_|_] ->
      ok = clocksi_vnode:prepare(UpdatedPartitions, Transaction),
      NewNumAffectedAgents = length(UpdatedPartitions),
      {next_state, receive_prepared, State#state{from = From, num_ack_pending = NewNumAffectedAgents, state = prepared}}
  end.



%% @doc when an error occurs or an updated partition
%% does not pass the certification check, the transaction aborts.
abort(State = #state{transaction = Transaction,
    partition_writesets = UpdatedPartitions}) ->
    NumPendingAck = length(UpdatedPartitions),
    case NumPendingAck of
        0 ->
            reply_to_client(State#state{state = aborted});
        _ ->
            ok = clocksi_vnode:abort(UpdatedPartitions, Transaction),
            {next_state, receive_aborted, State#state{num_ack_pending = NumPendingAck, state = aborted}}
    end.


%% @private
%% @doc This function is called by a gen_statem when it needs to find out
%% the callback mode of the callback module.
callback_mode() ->
  state_functions.

%% @private
%% @doc Called (1) whenever sys:get_status/1,2 is called by gen_statem or
%% (2) when gen_statem terminates abnormally.
%% This callback is optional.
format_status(_Opt, [_PDict, _StateName, _State]) ->
  Status = some_term,
  Status.

%% @private
%% @doc This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
terminate(_Reason, _StateName, _State = #state{}) ->
  ok.

%% @private
%% @doc Convert process state when code is changed
code_change(_OldVsn, StateName, State = #state{}, _Extra) ->
  {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_state(proplists:proplist()) -> state().
init_state(Properties) ->
  #state{
    from = undefined,
    transaction = undefined,
      partition_writesets = [],
    client_ops = [],
    num_ack_pending = 0,
    num_agents_affected = 0,
    prepare_time = 0,
    operations = undefined,
    return_accumulator = [],
    properties = Properties
  }.


%% @doc TODO
-spec start_tx_internal(snapshot_time(), proplists:proplist()) -> {ok, state()} | {error, any()}.
start_tx_internal(ClientClock, Properties) ->
  TransactionRecord = clocksi_interactive_coord_helpers:create_transaction_record(ClientClock, false, Properties),
  % a new transaction was started, increment metrics
  ?STATS(open_transaction),
  {ok, TransactionRecord}.



perform_update({Object, OpType, Update}, PartitionWritesets, Transaction, _Sender, ClientOps) ->
  ?STATS(operation_update),
    {Key, _ObjectType} = Object,
  Partition = antidote_riak_utilities:get_key_partition(Key),

  WriteSet = case lists:keyfind(Partition, 1, PartitionWritesets) of
               false ->
                 [];
               {Partition, WS} ->
                 WS
             end,

  %% Execute pre_commit_hook if any
  case antidote_hooks:execute_pre_commit_hook(Key, OpType, Update) of
    {error, Reason} ->
      ?LOG_DEBUG("Execute pre-commit hook failed ~p", [Reason]),
      {error, Reason};

    {Key, Type, PostHookUpdate} ->
        %TODO: Generate a downstream operation for the type.
        ok = gingko_vnode:update(Key, Type, Transaction#transaction.txn_id, Update, {fsm, undefined, self()}),
        UpdatedPartitionWritesets = append_updated_partitions(
          PartitionWritesets,
          WriteSet,
          Partition,
          Key
        ),
        UpdatedOps = [{Key, Type, PostHookUpdate} | ClientOps],
        {UpdatedPartitionWritesets, UpdatedOps}
  end.


%% @doc Add new updates to the write set of the given partition.
%%
%%      If there's no write set, create a new one.
%%
append_updated_partitions(PartitionWritesets, [], Partition, Key) ->
  [{Partition, [Key]} | PartitionWritesets];

append_updated_partitions(PartitionWritesets, ModifiedKeys, Partition, Key) ->
  %% Update the write set entry with the new record
  AllUpdates = {Partition, [Key | ModifiedKeys]},
  lists:keyreplace(Partition, 1, PartitionWritesets, AllUpdates).



process_prepared(ReceivedPrepareTime, State = #state{
    from = From,
    num_ack_pending = NumPendingAck,
    prepare_time = PrepareTime}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumPendingAck of
        1 ->
            send_prepared_ack(From, MaxPrepareTime),
            {next_state, execute_commit, State#state{num_ack_pending = 0, prepare_time = MaxPrepareTime, state= prepared, commit_type_required = normal}};
        _ ->
            {next_state, receive_prepared, State#state{num_ack_pending = NumPendingAck - 1, prepare_time = MaxPrepareTime}}
    end.

send_prepared_ack(To, PrepareTime) ->
    gen_statem:reply(To, {ok, PrepareTime}).



%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
reply_to_client(State = #state{
  from=From,
  state=TxState,
  client_ops=ClientOps,
  commit_time=CommitTime,
  transaction=Transaction,
  return_accumulator=ReturnAcc
}) ->
  TxId = Transaction#transaction.txn_id,
  _ = case From of
        undefined ->
          ok;
        {_Pid, _Tag} ->

          Reply = case TxState of
                    committed_read_only ->
                      {ok, {TxId, Transaction#transaction.vec_snapshot_time}};
                    committed ->
                      %% Execute post_commit_hooks
                      _Result = execute_post_commit_hooks(ClientOps),
                      %% TODO: What happens if commit hook fails?
                      DcId = dc_utilities:get_my_dc_id(),
                      CausalClock = vectorclock:set(DcId, CommitTime, Transaction#transaction.vec_snapshot_time),
                      {ok, {TxId, CausalClock}};
                    aborted ->
                      ?STATS(transaction_aborted),
                      case ReturnAcc of
                        {error, Reason} ->
                          {error, Reason};
                        _ ->
                          {error, aborted}
                      end
                  end,
          gen_statem:reply(From, Reply)
      end,
  % transaction is finished, decrement count
  ?STATS(transaction_finished),
  {stop, normal, State}.



execute_post_commit_hooks(Ops) ->
  lists:foreach(fun({Key, Type, Update}) ->
    case antidote_hooks:execute_post_commit_hook(Key, Type, Update) of
      {error, Reason} ->
        ?LOG_INFO("Post commit hook failed. Reason ~p", [Reason]);
      _ -> ok
    end
  end, lists:reverse(Ops)).




%% Replaces the first occurrence of an entry;
%% yields error if there the element to be replaced is not in the list
replace_key_with_snapshot([], _, _) ->
    error;
replace_key_with_snapshot([Key|Rest], Key, Snapshot) ->
    [Snapshot|Rest];
replace_key_with_snapshot([NotMyKey|Rest], Key, Snapshot) ->
    [NotMyKey|replace_key_with_snapshot(Rest, Key, Snapshot)].
