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
  start_vnode/1]).

%% VNode methods
-export([
  init/1,
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
  delete/1]).

%% VNode state
-record(state, {
  partition :: partition_id(),
  queue :: queue(),
  vectorclock :: vectorclock()
}).

%%%% API --------------------------------------------------------------------+

-spec init([partition_id()]) -> {ok, #state{}}.
init([Partition]) ->
  {ok, StableSnapshot} = vectorclock:get_stable_snapshot(),
  {ok, #state{partition = Partition, queue = queue:new(), vectorclock = StableSnapshot}}.

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

handle_transaction(Txn=#interdc_txn{partition = P}) -> dc_utilities:call_vnode_sync(P, inter_dc_dep_vnode_master, {txn, Txn}).

%%%% VNode methods ----------------------------------------------------------+

-spec process_queue(#state{}) -> #state{}.
process_queue(State=#state{queue=Queue}) ->
  case queue:peek(Queue) of
    empty -> State;
    {value, Txn} ->
      {NewState, Success} = try_store(State, Txn),
      case Success of
        false -> NewState;
        true -> process_queue(NewState#state{queue = queue:drop(Queue)})
      end
  end.

%% Store the heartbeat message.
%% This is not a true transaction, so its dependencies are always satisfied.
-spec try_store(#state{}, #interdc_txn{}) -> {#state{}, boolean}.
try_store(State, #interdc_txn{dcid = DCID, timestamp = Timestamp, operations = []}) ->
  {update_the_clock(State, DCID, Timestamp), true};

%% Store the normal transaction
try_store(State, Txn=#interdc_txn{dcid = DCID, partition = Partition, timestamp = Timestamp, operations = Ops}) ->
  %% The transactions are delivered reliably and in order, so the entry for originating DC is irrelevant.
  %% Therefore, we remove it prior to further checks.
  Dependencies = vectorclock:set_clock_of_dc(DCID, 0, Txn#interdc_txn.snapshot),
  CurrentClock = vectorclock:set_clock_of_dc(DCID, 0, get_partition_clock(State)),

  %% Check if the current clock is greather than or equal to the dependency vector
  case vectorclock:ge(CurrentClock, Dependencies) of

    %% If not, the transaction will not be stored right now.
    false -> {State, false};

    %% If so, store the transaction
    true ->
      %% Put the operations in the log
      ok = lists:foreach(fun(#operation{payload=Payload}) ->
        logging_vnode:append(dc_utilities:partition_to_indexnode(Partition), [Partition], Payload)
      end, Ops),

      %% Update the materializer (send only the update operations)
      ClockSiOps = updates_to_clocksi_payloads(Txn),

      ok = lists:foreach(fun(Op) -> materializer_vnode:update(Op#clocksi_payload.key, Op) end, ClockSiOps),
      {update_the_clock(State, DCID, Timestamp), true}
  end.

handle_command({txn, Txn}, _Sender, State=#state{queue=Queue}) ->
  NewState = process_queue(State#state{queue = queue:in(Txn, Queue)}),
  {reply, ok, NewState}.

handle_coverage(_Req, _KeySpaces, _Sender, State) -> {stop, not_implemented, State}.
handle_exit(_Pid, _Reason, State) -> {noreply, State}.
handoff_starting(_TargetNode, State) -> {true, State}.
handoff_cancelled(State) -> {ok, State}.
handoff_finished(_TargetNode, State) -> {ok, State}.
handle_handoff_command(_Message, _Sender, State) -> {noreply, State}.
handle_handoff_data(_Data, State) -> {reply, ok, State}.
encode_handoff_item(Key,Operation) -> term_to_binary({Key, Operation}).
is_empty(State) -> {true, State}.
terminate(_Reason, _ModState) -> ok.
delete(State) -> {ok, State}.

%%%% Utilities --------------------------------------------------------------+

update_the_clock(State, DCID, Timestamp) ->
  %% Should we decrement the timestamp value by 1?
  NewClock = vectorclock:set_clock_of_dc(DCID, Timestamp - 1, State#state.vectorclock),
  ok = meta_data_sender:put_meta_dict(State#state.partition, NewClock),
  State#state{vectorclock = NewClock}.

get_partition_clock(State) ->
  %% Return the vectorclock associated with the current state, but update the local entry with the current timestamp
  vectorclock:set_clock_of_dc(dc_utilities:get_my_dc_id(), inter_dc_utils:now_millisec(), State#state.vectorclock).

updates_to_clocksi_payloads(Txn = #interdc_txn{dcid = DCID, timestamp = CommitTime, snapshot = SnapshotTime}) ->
  lists:map(fun(#operation{payload = Logrecord}) ->
    {Key, Type, Op} = Logrecord#log_record.op_payload,
    #clocksi_payload{
      key = Key,
      type = Type,
      op_param = Op,
      snapshot_time = SnapshotTime,
      commit_time = {DCID, CommitTime},
      txid =  Logrecord#log_record.tx_id
    }
  end, inter_dc_txn:ops_by_type(Txn, update)).




