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

%% Transaction buffer, used to check for message loss through operation log id gaps.

-module(inter_dc_sub_buf).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("kernel/include/logger.hrl").

%% Expected time to wait until the logging_vnode is started
-define(LOG_STARTUP_WAIT, 1000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
  new_state/1,
  process/2]).

%%%% API --------------------------------------------------------------------+

%% TODO: Fetch last observed ID from durable storage (maybe log?). This way, in case of a node crash, the queue can be fetched again.
-spec new_state(pdcid()) -> inter_dc_sub_buf().
new_state(PDCID) ->
  {ok, EnableLogging} = application:get_env(antidote, enable_logging),
  #inter_dc_sub_buf{
    state_name = normal,
    pdcid = PDCID,
    last_observed_opid = init,
    queue = queue:new(),
    logging_enabled = EnableLogging,
    log_reader_timeout = 0
  }.

-spec process({txn, interdc_txn()} | {log_reader_resp, [interdc_txn()]}, inter_dc_sub_buf()) -> inter_dc_sub_buf().
process({txn, Txn}, State = #inter_dc_sub_buf{last_observed_opid = init, pdcid = {DCID, Partition}}) ->
    %% If this is the first txn received (i.e. if last_observed_opid = init) then check the log
    %% to see if there was a previous op received (i.e. in the case of fail and restart) so that
    %% you can check for duplicates or lost messages
    Result = try
                 logging_vnode:request_op_id(dc_utilities:partition_to_indexnode(Partition),
                         DCID, Partition)
             catch
                 _:Reason ->
                     ?LOG_DEBUG("Error loading last opid from log: ~w, will retry", [Reason])
             end,
    case Result of
    {ok, OpId} ->
        ?LOG_DEBUG("Loaded opid ~p from log for dc ~p, partition, ~p", [OpId, DCID, Partition]),
        process({txn, Txn}, State#inter_dc_sub_buf{last_observed_opid=OpId});
    _ ->
        riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, {txn, Txn}),
        State
    end;
process({txn, Txn}, State = #inter_dc_sub_buf{state_name = normal}) -> process_queue(push(Txn, State));
process({txn, Txn}, State = #inter_dc_sub_buf{state_name = buffering, log_reader_timeout = Timeout}) ->
    %% Buffering incoming transactions while waiting for log reader response.
    %% Change to normal state, if response timeout exceed, to query for response again.
    ?LOG_INFO("Buffering txn in ~p", [State#inter_dc_sub_buf.pdcid]),
    Time = erlang:system_time(millisecond),
    if
        Timeout < Time ->
            ?LOG_WARNING("Got timeout for log_reader_resp in ~p", [State#inter_dc_sub_buf.pdcid]),
            process_queue(push(Txn, State#inter_dc_sub_buf{state_name = normal}));
        true ->
            push(Txn, State)
    end;

process({log_reader_resp, Txns}, State = #inter_dc_sub_buf{queue = Queue}) ->
    %% Add log response to buffer and process.
    NewQueue = queue:join(queue:from_list(Txns), Queue),
    NewState = State#inter_dc_sub_buf{queue = NewQueue},
    process_queue(NewState).


%%%% Methods ----------------------------------------------------------------+
process_queue(State = #inter_dc_sub_buf{queue = Queue, last_observed_opid = Last, logging_enabled = EnableLogging}) ->
  case queue:peek(Queue) of
    empty -> State#inter_dc_sub_buf{state_name = normal};
    {value, Txn} ->
      TxnLast = Txn#interdc_txn.prev_log_opid#op_number.local,
      case cmp(TxnLast, Last) of

      %% If the received transaction is immediately after the last observed one
        eq ->
          deliver(Txn),
          Max = (inter_dc_txn:last_log_opid(Txn))#op_number.local,
          process_queue(State#inter_dc_sub_buf{queue = queue:drop(Queue), last_observed_opid = Max});

      %% If the transaction seems to come after an unknown transaction, ask the remote origin log
        gt ->
        case EnableLogging of
          true ->
            ?LOG_INFO("Whoops, lost message. New is ~p, last was ~p. Asking the remote DC ~p",
                  [TxnLast, Last, State#inter_dc_sub_buf.pdcid]),
            try
              case query(State#inter_dc_sub_buf.pdcid, State#inter_dc_sub_buf.last_observed_opid + 1, TxnLast) of
                ok ->
                  %% Enter buffering state while waiting for response and set timeout
                  State#inter_dc_sub_buf{state_name = buffering, log_reader_timeout = erlang:system_time(millisecond) + ?LOG_REQUEST_TIMEOUT};
                _  ->
                  ?LOG_WARNING("Failed to send log query to DC, will retry on next ping message"),
                  State#inter_dc_sub_buf{state_name = normal}
              end
            catch
              _:_ ->
                  ?LOG_WARNING("Failed to send log query to DC, will retry on next ping message"),
                  State#inter_dc_sub_buf{state_name = normal}
            end;
          false -> %% we deliver the transaction as we can't ask anything to the remote log
                         %% as logging to disk is disabled.
                    deliver(Txn),
                    Max = (inter_dc_txn:last_log_opid(Txn))#op_number.local,
                    process_queue(State#inter_dc_sub_buf{queue = queue:drop(Queue), last_observed_opid = Max})
        end;

      %% If the transaction has an old value, drop it.
        lt ->
            ?LOG_WARNING("Dropping duplicate message ~w, last time was ~w", [(TxnLast + 1), Last]),
            process_queue(State#inter_dc_sub_buf{queue = queue:drop(Queue)})
      end
  end.

-spec deliver(interdc_txn()) -> ok.
deliver(Txn) -> inter_dc_dep_vnode:handle_transaction(Txn).

%% TODO: consider dropping messages if the queue grows too large.
%% The lost messages would be then fetched again by the log_reader.
-spec push(interdc_txn(), inter_dc_sub_buf()) -> inter_dc_sub_buf().
push(Txn, State) -> State#inter_dc_sub_buf{queue = queue:in(Txn, State#inter_dc_sub_buf.queue)}.

%% Instructs the log reader to ask the remote DC for a given range of operations.
%% Instead of a simple request/response with blocking, the result is delivered
%% asynchronously to inter_dc_sub_vnode.
-spec query(pdcid(), log_opid(), log_opid()) -> ok | unknown_dc.
query({DCID, Partition}, From, To) ->
    BinaryRequest = term_to_binary({read_log, Partition, From, To}),
    inter_dc_query:perform_request(?LOG_READ_MSG, {DCID, Partition}, BinaryRequest, fun inter_dc_sub_vnode:deliver_log_reader_resp/2).

cmp(A, B) when A > B -> gt;
cmp(A, B) when B > A -> lt;
cmp(_, _) -> eq.

-ifdef(TEST).

process_init() ->
    State = new_state({0, 0}),
    Txn = make_txn(0),
    NewState = process({txn, Txn}, State),
    ?assertEqual(normal, NewState#inter_dc_sub_buf.state_name).

process_old() ->
    State = new_state({0, 0}),
    Txn = make_txn(-1),
    NewState = process({txn, Txn}, State#inter_dc_sub_buf{state_name = normal}),
    ?assertEqual(normal, NewState#inter_dc_sub_buf.state_name).

process_missing_txn() ->
    State = new_state({0, 0}),
    Txn = make_txn(1),
    NewState = process({txn, Txn}, State#inter_dc_sub_buf{state_name = normal}),
    ?assertEqual(buffering, NewState#inter_dc_sub_buf.state_name).

process_buffering() ->
    State = new_state({0, 0}),
    Txn = make_txn(1),
    NewState = process({txn, Txn}, State#inter_dc_sub_buf{state_name = buffering, log_reader_timeout = erlang:system_time(millisecond) + 3000}),
    ?assertEqual(buffering, NewState#inter_dc_sub_buf.state_name),
    NewState2 = process({txn, Txn}, State#inter_dc_sub_buf{state_name = buffering, log_reader_timeout = erlang:system_time(millisecond) - 1000}),
    ?assertEqual(buffering, NewState2#inter_dc_sub_buf.state_name).

process_resp() ->
    meck:reset(inter_dc_query),
    State = new_state({0, 0}),
    Txn = make_txn(1),
    BufState = process({txn, Txn}, State#inter_dc_sub_buf{state_name = normal}),
    ?assertEqual(buffering, BufState#inter_dc_sub_buf.state_name),
    ?assertEqual(1, queue:len(BufState#inter_dc_sub_buf.queue)),
    Txn2 = make_txn(0),
    NormalState = process({log_reader_resp, [Txn2]}, State#inter_dc_sub_buf{state_name = normal}),
    ?assertEqual(normal, NormalState#inter_dc_sub_buf.state_name),
    ?assertEqual(0, queue:len(NormalState#inter_dc_sub_buf.queue)).

make_txn(Last) ->
    #interdc_txn{
        dcid = 0,
        partition = 0,
        prev_log_opid = #op_number{node = {node(), 0}, global = 0, local = Last},
        log_records = []
    }.

test_init() ->
    application:set_env(antidote, enable_logging, true),
    meck:new(dc_utilities),
    meck:new(logging_vnode),
    meck:new(inter_dc_dep_vnode),
    meck:new(inter_dc_query),
    meck:expect(logging_vnode, request_op_id, fun(_, _, _) -> {ok, 0} end),
    meck:expect(dc_utilities, partition_to_indexnode, fun(_) -> {0, node()} end),
    meck:expect(inter_dc_query, perform_request, fun(_, _, _, _) -> ok end),
    meck:expect(inter_dc_dep_vnode, handle_transaction, fun(_) -> ok end),
    logger:add_handler_filter(default, ?MODULE, {fun(_, _) -> stop end, nostate}),
    ok.

test_cleanup(_) ->
    meck:unload(dc_utilities),
    meck:unload(logging_vnode),
    meck:unload(inter_dc_dep_vnode),
    meck:unload(inter_dc_query),
    logger:remove_handler_filter(default, ?MODULE).

meck_test_() -> {
    setup,
    fun test_init/0,
    fun test_cleanup/1,
    [
        fun process_init/0,
        fun process_old/0,
        fun process_missing_txn/0,
        fun process_buffering/0,
        fun process_resp/0
    ]}.

-endif.
