-module(new_inter_dc_sub_buf_fsm).
-behaviour(gen_fsm).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% Subscriber buffer FSM - handles transactions received via the interDC protocol.
%% The objective of this FSM is to track operation log IDs, and to detect if any message was lost.
%% If so, this FSM buffers incoming transactions and sends the query to remote DC's log_reader, fetching missed txns.

-export([up_to_date/2, buffering/2, handle_txn/2, handle_ping/2]).
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4, start_link/2]).

-record(state, {
  pdcid :: pdcid(),
  last_observed_opid :: non_neg_integer(),
  queue :: queue(),
  socket :: zmq_socket()
}).

%% API: pass the transaction so the FSM will handle it, possibly buffering.
handle_txn(FsmRef, Txn) -> gen_fsm:send_event(FsmRef, {txn, Txn}).
handle_ping(FsmRef, Ping) -> gen_fsm:send_event(FsmRef, {ping, Ping}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link(pdcid(), socket_address()) -> any().
start_link(PDCID, LogReaderAddress) -> gen_fsm:start_link(?MODULE, [PDCID, LogReaderAddress], []).

%% TODO: fetch the last observed opid from log
init([PDCID, Address]) -> {ok, up_to_date, #state{pdcid=PDCID, last_observed_opid=0, queue=queue:new(), socket=zmq_utils:create_connect_socket(req, true, Address)}}.

up_to_date({txn, Txn}, State) -> process_queue(State#state{queue = queue:in(Txn, State#state.queue)});

up_to_date({ping, _Ping}, _State) -> lager:info("qweqweqwe").

buffering({txn, Txn}, State) -> {next_state, buffering, State#state{queue = queue:in(Txn, State#state.queue)}};

buffering({log_reader_rsp, Txns} ,State = #state{queue = Queue}) ->
  ok = lists:foreach(fun deliver/1, Txns),
  NewLast = case Txns of
    [] ->
      case queue:peek(Queue) of
        empty -> State#state.last_observed_opid;
        {value, Txn} -> txn_min_op_id(Txn) - 1
      end;
    _ -> txn_max_op_id(lists:last(Txns))
  end,
  process_queue(State#state{last_observed_opid = NewLast}).

process_queue(State = #state{queue = Queue, last_observed_opid = Last}) ->
  case queue:peek(Queue) of
    empty -> {next_state, up_to_date, State};
    {value, Txn} ->
      Min = txn_min_op_id(Txn),
      case Last + 1 == Min of
        true ->
          deliver(Txn),
          process_queue(State#state{queue = queue:drop(Queue), last_observed_opid = txn_max_op_id(Txn)});
        false ->
          {_, Partition} = State#state.pdcid,
          Request = {read_log, Partition, State#state.last_observed_opid, Min},
          ok = erlzmq:send(State#state.socket, term_to_binary(Request)),
          {next_state, buffering, State}
      end
  end.

%% The socket is marked as active, therefore messages are delivered to the fsm through the handle_info method.
handle_info({zmq, _Socket, BinaryMsg, _Flags}, StateName, State) ->
  ok = gen_fsm:send_event(self(), {log_reader_rsp, binary_to_term(BinaryMsg)}),
  {next_state, StateName, State}.

deliver(Txn) -> new_inter_dc_dep_vnode:handle_transaction(Txn).
handle_event(_Event, _StateName, StateData) -> {stop, badmsg, StateData}.
handle_sync_event(_Event, _From, _StateName, StateData) -> {stop, badmsg, StateData}.
terminate(_Reason, _StateName, State) -> erlzmq:close(State#state.socket).
code_change(_OldVsn, _StateName, _StateData, _Extra) -> erlang:error(not_implemented).
txn_min_op_id(#interdc_txn{operations = Ops}) -> {Min, _} = (hd(Ops))#operation.op_number, Min.
txn_max_op_id(#interdc_txn{operations = Ops}) -> {Max, _} = (lists:last(Ops))#operation.op_number, Max.