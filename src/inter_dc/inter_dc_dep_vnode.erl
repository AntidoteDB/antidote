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

%% This vnode receives all transactions happening at remote DCs,
%% in commit order for each DC and with no missing operations
%% (ensured by interDC). The goal of this module is to ensure
%% that transactions are committed when their causal dependencies
%% are satisfied.

-module(inter_dc_dep_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([
  handle_transaction/1,
  set_dependency_clock/2]).

%% VNode methods
-export([
  init/1,
  start_vnode/1,
  handle_command/3,
  handle_coverage/4,
  handle_exit/3,
  handoff_starting/2,
  handoff_cancelled/1,
  handoff_finished/2,
  handle_handoff_command/3,
  handle_handoff_data/2,
  encode_handoff_item/2,
  is_empty/1,
  terminate/2,
  delete/1,
  handle_overload_command/3,
  handle_overload_info/2]).

%% VNode state
-record(state, {
  partition :: partition_id(),
  queues :: dict:dict(dcid(), queue:queue()), %% DCID -> queue()
  vectorclock :: vectorclock(),
  last_updated :: non_neg_integer(),
  drop_ping :: boolean()
}).
-type state() :: #state{}.
%%%% API --------------------------------------------------------------------+

%% Passes the received transaction to the dependency buffer.
%% At this point no message can be lost (the transport layer must ensure all transactions are delivered reliably).
-spec handle_transaction(interdc_txn()) -> ok.
handle_transaction(Txn=#interdc_txn{partition = P}) -> dc_utilities:call_local_vnode_sync(P, inter_dc_dep_vnode_master, {txn, Txn}).

%% After restarting from failure, load the vectorclock of the max times of all the updates received from other DCs
%% Otherwise new updates from other DCs will be blocked
-spec set_dependency_clock(partition_id(), vectorclock()) -> ok.
set_dependency_clock(Partition, Vector) -> dc_utilities:call_local_vnode_sync(Partition, inter_dc_dep_vnode_master, {set_dependency_clock, Vector}).

%%%% VNode methods ----------------------------------------------------------+

-spec init([partition_id()]) -> {ok, state()}.
init([Partition]) ->
  StableSnapshot = vectorclock:new(),
  {ok, #state{partition = Partition, queues = dict:new(), vectorclock = StableSnapshot, last_updated = 0, drop_ping = false}}.

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% Check the content of each queue, try to apply as many elements as possible.
%% If any element was successfully pushed from any queue, repeat the process.
-spec process_all_queues(state()) -> state().
process_all_queues(State = #state{queues = Queues}) ->
  DCIDs = dict:fetch_keys(Queues),
  {NewState, NumUpdated} = lists:foldl(fun process_queue/2, {State, 0}, DCIDs),
  case NumUpdated of
    0 -> NewState;
    _ -> process_all_queues(NewState)
  end.

%% Tries to process as many elements in the queue as possible.
%% Returns the new state and the number of processed elements
process_queue(DCID, {State, Acc}) ->
  Queue = dict:fetch(DCID, State#state.queues),
  case queue:peek(Queue) of
    empty -> {State, Acc};
    {value, Txn} ->
      {NewState, Success} = try_store(State, Txn),
      case Success of
        false -> {NewState, Acc};
        true -> process_queue(DCID, {pop_txn(NewState, DCID), Acc + 1}) %% remove the just-applied txn and retry
    end
  end.

%% Store the heartbeat message.
%% This is not a true transaction, so its dependencies are always satisfied.
-spec try_store(state(), interdc_txn()) -> {state(), boolean()}.
try_store(State=#state{drop_ping = true}, #interdc_txn{log_records = []}) ->
    {State, true};
try_store(State, #interdc_txn{dcid = DCID, timestamp = Timestamp, log_records = []}) ->
    {update_clock(State, DCID, Timestamp), true};

%% Store the normal transaction
try_store(State, Txn=#interdc_txn{dcid = DCID, partition = Partition, timestamp = Timestamp, log_records = Ops}) ->
  %% The transactions are delivered reliably and in order, so the entry for originating DC is irrelevant.
  %% Therefore, we remove it prior to further checks.
  Dependencies = vectorclock:set(DCID, 0, Txn#interdc_txn.snapshot),
  CurrentClock = vectorclock:set(DCID, 0, get_partition_clock(State)),

  %% Check if the current clock is greater than or equal to the dependency vector
  case vectorclock:ge(CurrentClock, Dependencies) of

    %% If not, the transaction will not be stored right now.
    %% Still need to update the timestamp for that DC, up to 1 less than the
    %% value of the commit time, because updates from other DCs might depend
    %% on a time up to this
    false -> {update_clock(State, DCID, Timestamp-1), false};

    %% If so, store the transaction
    true ->
      %% Put the operations in the log
      {ok, _} = logging_vnode:append_group({Partition, node()},
                                           [Partition], Ops, false),

      ClockSiOps = updates_to_clocksi_payloads(Txn),

      ?STATS({dc_ops_received, length(ClockSiOps)}),
      ?STATS({dc_ops_received_size, byte_size(term_to_binary(ClockSiOps))}),

      %% Update the materializer (send only the update operations)
      ok = lists:foreach(fun(Op) -> materializer_vnode:update(Op#clocksi_payload.key, Op) end, ClockSiOps),
      {update_clock(State, DCID, Timestamp), true}
  end.

handle_command({set_dependency_clock, Vector}, _Sender, State) ->
    {reply, ok, State#state{vectorclock = Vector}};

handle_command({txn, Txn}, _Sender, State) ->

    NewState = process_all_queues(push_txn(State, Txn)),
    {reply, ok, NewState};

%% Tells the vnode to drop ping messages or not
%% Used for debugging
handle_command({drop_ping, DropPing}, _Sender, State) ->
    {reply, ok, State#state{drop_ping = DropPing}}.

handle_coverage(_Req, _KeySpaces, _Sender, State) -> {stop, not_implemented, State}.
handle_exit(_Pid, _Reason, State) -> {noreply, State}.
handoff_starting(_TargetNode, State) -> {true, State}.
handoff_cancelled(State) -> {ok, State}.
handoff_finished(_TargetNode, State) -> {ok, State}.
handle_handoff_command(_Message, _Sender, State) -> {noreply, State}.
handle_handoff_data(_Data, State) -> {reply, ok, State}.
encode_handoff_item(_ObjectName, _ObjectValue) -> <<>>.
is_empty(State) -> {true, State}.
terminate(_Reason, _ModState) -> ok.
delete(State) -> {ok, State}.
handle_overload_command(_, _, _) ->
    ok.
handle_overload_info(_, _) ->
    ok.


%%%% Utilities --------------------------------------------------------------+

%% Push the transaction to an appropriate queue inside the state.
-spec push_txn(state(), interdc_txn()) -> state().
push_txn(State = #state{queues = Queues}, Txn = #interdc_txn{dcid = DCID}) ->
  DCID = Txn#interdc_txn.dcid,
  Queue = case dict:find(DCID, Queues) of
    {ok, Q} -> Q;
    error -> queue:new()
  end,
  NewQueue = queue:in(Txn, Queue),
  State#state{queues = dict:store(DCID, NewQueue, Queues)}.

%% Remove one transaction from the chosen queue in the state.
pop_txn(State = #state{queues = Queues}, DCID) ->
  Queue = dict:fetch(DCID, Queues),
  NewQueue = queue:drop(Queue),
  State#state{queues = dict:store(DCID, NewQueue, Queues)}.

%% Update the clock value associated with the given DCID from the perspective of this partition.
-spec update_clock(state(), dcid(), non_neg_integer()) -> state().
update_clock(State = #state{last_updated = LastUpdated}, DCID, Timestamp) ->
  %% Should we decrement the timestamp value by 1?
  NewClock = vectorclock:set(DCID, Timestamp, State#state.vectorclock),

  %% Check if the stable snapshot should be refreshed.
  %% It's an optimization that reduces communication overhead during intensive updates at remote DCs.
  %% This assumes that heartbeats/updates arrive on a regular basis,
  %% and that there is always the next one arriving shortly.
  %% This causes the stable_snapshot to tick more slowly, which is an expected behaviour.
  Now = dc_utilities:now_millisec(),
  NewLastUpdated = case Now > LastUpdated + ?VECTORCLOCK_UPDATE_PERIOD of
    %% Stable snapshot was not updated for the defined period of time.
    %% Push the changes and update the last_updated parameter to the current timestamp.
    %% WARNING: this update must push the whole contents of the partition vectorclock,
    %% not just the current DCID/Timestamp pair in the arguments.
    %% Failure to do so may lead to a deadlock during the connection phase.
    true ->

      %% Update the stable snapshot NEW way (as in Tyler's weak_meta_data branch)
      ok = meta_data_sender:put_meta(stable_time_functions, State#state.partition, NewClock),

      Now;
    %% Stable snapshot was recently updated, no need to do so.
    false -> LastUpdated
  end,

  State#state{vectorclock = NewClock, last_updated = NewLastUpdated}.

%% Get the current vectorclock from the perspective of this partition, with the updated entry for current DC.
-spec get_partition_clock(state()) -> vectorclock().
get_partition_clock(State) ->
  %% Return the vectorclock associated with the current state, but update the local entry with the current timestamp
  vectorclock:set(dc_utilities:get_my_dc_id(), dc_utilities:now_microsec(), State#state.vectorclock).

%% Utility function: converts the transaction to a list of clocksi_payload ops.
-spec updates_to_clocksi_payloads(interdc_txn()) -> list(clocksi_payload()).
updates_to_clocksi_payloads(Txn = #interdc_txn{dcid = DCID, timestamp = CommitTime, snapshot = SnapshotTime}) ->
  lists:map(fun(#log_record{log_operation = LogRecord}) ->
    #update_log_payload{key = Key, type = Type, op = Op} = LogRecord#log_operation.log_payload,
    #clocksi_payload{
      key = Key,
      type = Type,
      op_param = Op,
      snapshot_time = SnapshotTime,
      commit_time = {DCID, CommitTime},
      txid =  LogRecord#log_operation.tx_id
    }
  end, inter_dc_txn:ops_by_type(Txn, update)).
