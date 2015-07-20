-module(new_inter_dc_sub_buf_fsm).
-behaviour(gen_fsm).
-include("antidote.hrl").

%% Subscriber buffer FSM - handles transactions received via the interDC protocol.
%% The objective of this FSM is to track operation log IDs, and to detect if any message was lost.
%% If so, this FSM buffers incoming transactions and sends the query to remote DC's log_reader, fetching missed txns.

-export([up_to_date/2, buffering/2, handle_txn/2]).
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4, start_link/2]).

-record(state, {
  pdcid :: partition_dcid(),
  last_observed_opid :: non_neg_integer(),
  buffer :: [interdc_txn()],
  socket :: zmq_socket()
}).

-spec start_link(partition_dcid(), socket_address()) -> any().
start_link(PDCID, LogReaderAddress) -> gen_fsm:start_link(?MODULE, [PDCID, LogReaderAddress], []).

init([PDCID, Address]) -> {ok, up_to_date, #state{
  pdcid = PDCID,
  last_observed_opid = 0, %% TODO: fetch the last observed opid from log
  buffer = [],
  socket = zmq_utils:create_connect_socket(req, true, Address) %% TODO: Maybe connect only when necessary?
}}.

%% API: pass the transaction so the FSM will handle it, possibly buffering.
handle_txn(FsmRef, Txn) ->
  gen_fsm:send_event(FsmRef, {msg_from_publisher, Txn}).

%% In the up_to_date state, the buffer is empty. Subsequent transactions are delivered directly to the log.
%% If the OpId gap is detected, the transactions are buffered and a request to log_reader is sent.
-spec up_to_date({msg_from_publisher, interdc_txn()}, #state{}) -> any().
up_to_date({msg_from_publisher, Txn}, State) ->
  MinOpId = txn_get_min_op_id(Txn),
  case State#state.last_observed_opid + 1 == MinOpId of
    true ->
      %% No message was lost, deliver transaction normally.
      deliver(Txn),
      {next_state, up_to_date, State#state{last_observed_opid = txn_get_max_op_id(Txn)}};
    false ->
      %% Gap in subsequent opid's. Buffer the transaction and ask the remote log reader (via socket) for the missed txns.
      {_, Partition} = State#state.pdcid,
      Request = {read_log, Partition, State#state.last_observed_opid, MinOpId},
      ok = erlzmq:send(State#state.socket, term_to_binary(Request)),
      {next_state, buffering, State#state{buffer = [Txn] ++ State#state.buffer}}
  end.

%% In the buffering state, all new transactions are stored until the log_reader response arrives.
buffering({msg_from_publisher, Txn}, State) ->
  {next_state, buffering, State#state{buffer = [Txn] ++ State#state.buffer}};

%% When the log_reader response arrives, all transactions are delivered in order.
buffering({log_reader_rsp, Txns} ,State) ->
  %% TODO: maybe recheck log ids again, something may have been lost during the buffering
  ToDeliver = Txns ++ lists:reverse(State#state.buffer),
  lists:foreach(fun deliver/1, ToDeliver),
  LastTxn = lists:last(ToDeliver),
  {next_state, up_to_date, State#state{buffer = [], last_observed_opid = txn_get_max_op_id(LastTxn)}}.

%% The socket is marked as active, therefore messages are delivered to the fsm through the handle_info method.
handle_info({zmq, _Socket, BinaryMsg, _Flags}, StateName, State) ->
  ok = gen_fsm:send_event(self(), {log_reader_rsp, binary_to_term(BinaryMsg)}),
  {next_state, StateName, State}.

%% Delivered transaction is stored in the log.
deliver(Txn) -> lager:info("Delivered TXN ~p", [Txn]).

handle_event(_Event, _StateName, StateData) -> {stop, badmsg, StateData}.
handle_sync_event(_Event, _From, _StateName, StateData) -> {stop, badmsg, StateData}.
terminate(_Reason, _StateName, State) -> erlzmq:close(State#state.socket).
code_change(_OldVsn, _StateName, _StateData, _Extra) -> erlang:error(not_implemented).

txn_get_min_op_id({_PDCID, Ops}) ->
  Op = hd(Ops),
  {OpId, _} = Op#operation.op_number,
  OpId.

txn_get_max_op_id({_PDCID, Ops}) ->
  Op = lists:last(Ops),
  {OpId, _} = Op#operation.op_number,
  OpId.