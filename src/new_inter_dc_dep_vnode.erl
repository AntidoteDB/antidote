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

%% This vnode stores received transactions in the log, making sure that its causal dependencies are preserved.

-module(new_inter_dc_dep_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-record(state, {
  partition :: partition_id(),
  queue :: queue()
}).

-export([handle_transaction/1]).
-export([init/1, handle_command/3, handle_coverage/4, handle_exit/3, handoff_starting/2, handoff_cancelled/1, handoff_finished/2, handle_handoff_command/3, handle_handoff_data/2, encode_handoff_item/2, is_empty/1, terminate/2, delete/1, start_vnode/1]).


init([Partition]) -> {ok, #state{partition = Partition, queue = queue:new()}}.
start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

handle_command({txn, Txn}, _Sender, State=#state{queue=Queue}) ->
  {reply, ok, process_queue(State#state{queue = queue:in(Txn, Queue)})}.

process_queue(State=#state{queue=Queue}) ->
  case queue:peek(Queue) of
    empty -> State;
    {value, Txn} ->
      case try_store(Txn) of
        false -> State;
        true -> process_queue(State#state{queue = queue:drop(Queue)})
      end
  end.

try_store(Txn=#interdc_txn{dcid = DCID, partition = Partition, operations = Ops}) ->
  %% The transactions are delivered reliably and in order, so the entry for originating DC is irrelevant
  Dependencies = vectorclock:set_clock_of_dc(DCID, 0, get_txn_snapshot_time(Txn)),
  CurrentClock = vectorclock:set_clock_of_dc(DCID, 0, get_partition_clock(Partition)),

  case vectorclock:ge(CurrentClock, Dependencies) of
    false -> false;
    true ->

      ok = lists:foreach(fun(#operation{payload=Payload}) ->
        logging_vnode:append(dc_utilities:partition_to_indexnode(Partition), [Partition], Payload)
      end, Ops),

      Transaction = clocksi_transaction_reader:construct_transaction(Ops),
      DownOps = clocksi_transaction_reader:get_update_ops_from_transaction(Transaction),

      ok = lists:foreach(fun(Op) -> materializer_vnode:update(Op#clocksi_payload.key, Op) end, DownOps),

      {_, {_, Ts}, _, _} = Transaction,
      ok = vectorclock:update_clock(Partition, DCID, Ts),
      dc_utilities:call_vnode(Partition, vectorclock_vnode_master, calculate_stable_snapshot),
      %%lists:foreach(fun(P) -> vectorclock:update_clock(P, DCID, Ts) end, dc_utilities:get_partitions()),
      %%dc_utilities:bcast_vnode(vectorclock_vnode_master, calculate_stable_snapshot),

      {ok, NewClock} = vectorclock:get_clock(Partition),
      {ok, Stable} = vectorclock:get_stable_snapshot(),
      lager:info("Done! NC=~p ST=~p", [dict:to_list(NewClock), dict:to_list(Stable)]),
      true
  end.


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

%%%%%%%%%%%%%%%%%%%%%%%%

handle_transaction(Txn=#interdc_txn{partition = P}) -> dc_utilities:call_vnode_sync(P, new_inter_dc_dep_vnode_master, {txn, Txn}).

get_txn_snapshot_time(#interdc_txn{operations = Ops}) ->
  CommitPld = (lists:last(Ops))#operation.payload,
  commit = CommitPld#log_record.op_type, %% sanity check
  {_, SnapshotTime} = CommitPld#log_record.op_payload,
  SnapshotTime.

get_partition_clock(Partition) ->
  {ok, LocalClock} = vectorclock:get_clock(Partition),
  vectorclock:set_clock_of_dc(dc_utilities:get_my_dc_id(), now_millisec(), LocalClock).

now_millisec() ->
  {MegaSecs, Secs, MicroSecs} = erlang:now(),
  (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.


