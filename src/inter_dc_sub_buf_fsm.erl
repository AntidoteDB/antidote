-module(inter_dc_sub_buf_fsm).
-behaviour(gen_fsm).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% Subscriber buffer FSM - handles transactions received via the interDC protocol.
%% The objective of this FSM is to track operation log IDs, and to detect if any message was lost.
%% If so, this FSM buffers incoming transactions and sends the query to remote DC's log_reader, fetching missed txns.

-export([up_to_date/2, buffering/2, handle_txn/2]).
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4, start_link/2]).

-record(state, {
  pdcid :: pdcid(),
  last_observed_opid :: non_neg_integer(),
  queue :: queue(),
  address :: socket_address(),
  socket :: zmq_socket()
}).

%% API: pass the transaction so the FSM will handle it, possibly buffering.
handle_txn(FsmRef, Txn) -> gen_fsm:send_event(FsmRef, {txn, Txn}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link(pdcid(), socket_address()) -> any().
start_link(PDCID, LogReaderAddress) -> gen_fsm:start_link(?MODULE, [PDCID, LogReaderAddress], []).

init([PDCID, Address]) -> {ok, up_to_date, #state{
  pdcid = PDCID,
  last_observed_opid = 0, %% TODO: fetch the last observed opid from log
  queue = queue:new(),
  address = Address,
  socket = none
}}.

up_to_date({txn, Txn}, State) -> process_queue(State#state{queue = queue:in(Txn, State#state.queue)}).

buffering({txn, Txn}, State) -> {next_state, buffering, State#state{queue = queue:in(Txn, State#state.queue)}};

buffering({log_reader_rsp, Txns} ,State = #state{queue = Queue}) ->
  ok = lists:foreach(fun deliver/1, Txns),
  NewLast = case Txns of
    [] ->
      case queue:peek(Queue) of
        empty -> State#state.last_observed_opid;
        {value, Txn} ->
          {Min, _} = Txn#interdc_txn.logid_range,
          Min - 1
      end;
    _ ->
      {_, Max} = (lists:last(Txns))#interdc_txn.logid_range,
      Max
  end,
  NewState = State#state{last_observed_opid = NewLast},
  process_queue(NewState).

process_queue(State = #state{queue = Queue, last_observed_opid = Last}) ->
  case queue:peek(Queue) of
    empty -> {next_state, up_to_date, close_socket(State)};
    {value, Txn} ->
      {Min, Max} = Txn#interdc_txn.logid_range,
      %% assert Max >= Min
      case Last + 1 >= Min of
        true ->
          deliver(Txn),
          process_queue(State#state{queue = queue:drop(Queue), last_observed_opid = Max});
        false ->
          {_, Partition} = State#state.pdcid,
          Request = {read_log, Partition, State#state.last_observed_opid, Min},
          {next_state, buffering, ask_log_reader(State, Request)}
      end
  end.

ask_log_reader(State, Request) ->
  Socket = case State#state.socket of
    none -> zmq_utils:create_connect_socket(req, true, State#state.address);
    S -> S
  end,
  ok = erlzmq:send(Socket, term_to_binary(Request)),
  State#state{socket = Socket}.

close_socket(State) ->
  case State#state.socket of
    none -> ok;
    Socket -> erlzmq:close(Socket)
  end,
  State#state{socket = none}.

%% The socket is marked as active, therefore messages are delivered to the fsm through the handle_info method.
handle_info({zmq, _Socket, BinaryMsg, _Flags}, StateName, State) ->
  Msg = binary_to_term(BinaryMsg),
  ok = gen_fsm:send_event(self(), {log_reader_rsp, Msg}),
  {next_state, StateName, State}.

deliver(Txn) -> inter_dc_dep_vnode:handle_transaction(Txn).
handle_event(_Event, _StateName, StateData) -> {stop, badmsg, StateData}.
handle_sync_event(_Event, _From, _StateName, StateData) -> {stop, badmsg, StateData}.
terminate(_Reason, _StateName, State) -> close_socket(State), ok.
code_change(_OldVsn, _StateName, _StateData, _Extra) -> erlang:error(not_implemented).