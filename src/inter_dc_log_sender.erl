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
-module(inter_dc_log_sender).
-behaviour(riak_core_vnode).

%% Each logging_vnode informs this vnode about every new appended operation.
%% This vnode assembles operations into transactions, and sends the transactions to appropriate destinations.
%% If no transaction is sent in 10 seconds, heartbeat messages are sent instead.

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-export([
  start_vnode/1,
  send/2,
  ping/1]).
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

-record(state, {
  partition :: partition_id(),
  buffer, %% log_tx_assembler:state
  last_log_id :: non_neg_integer(),
  ping_timer :: any()
}).

%% API
start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).
send(Partition, Operation) -> dc_utilities:call_vnode_sync(Partition, inter_dc_log_sender_master, {log_event, Operation}).
ping(Partition) -> dc_utilities:call_vnode_sync(Partition, inter_dc_log_sender_master, ping).

init([Partition]) ->
  {ok, set_timer(#state{
    partition = Partition,
    buffer = log_txn_assembler:new_state(),
    last_log_id = 0,
    ping_timer = none
  })}.

handle_command(ping, _Sender, State) ->
  %% TODO: think if the timestamp could cause any problems
  PingTxn = inter_dc_txn:ping(State#state.partition, State#state.last_log_id, inter_dc_utils:now_millisec()),
  {reply, ok, broadcast(State, PingTxn)};

handle_command({log_event, Operation}, _Sender, State) ->
  {Result, NewBufState} = log_txn_assembler:process(Operation, State#state.buffer),
  State1 = State#state{buffer = NewBufState},
  State2 = case Result of
    {ok, Ops} ->
      Txn = inter_dc_txn:from_ops(Ops, State1#state.partition),
      %% sanity check - we only publish locally committed transactions
      case inter_dc_txn:is_local(Txn) of
        true -> broadcast(State1, Txn);
        false -> State1
      end;
    none -> State1
  end,
  {reply, ok, State2}.


handle_coverage(_Req, _KeySpaces, _Sender, State) -> {stop, not_implemented, State}.
handle_exit(_Pid, _Reason, State) -> {noreply, State}.
handoff_starting(_TargetNode, State) -> {true, State}.
handoff_cancelled(State) -> {ok, State}.
handoff_finished(_TargetNode, State) -> {ok, State}.
handle_handoff_command( _Message , _Sender, State) -> {noreply, State}.
handle_handoff_data(_Data, State) -> {reply, ok, State}.
encode_handoff_item(Key, Operation) -> term_to_binary({Key, Operation}).
is_empty(State) -> {true, State}.
terminate(_Reason, _State) -> ok.
delete(State) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%

set_timer(State) ->
  ClearedState = clr_timer(State),
  {ok, Timer} = timer:apply_after(1000, inter_dc_log_sender, ping, [State#state.partition]),
  ClearedState#state{ping_timer = Timer}.

clr_timer(State = #state{ping_timer = none}) -> State;
clr_timer(State = #state{ping_timer = Timer}) ->
  {ok, cancel} = timer:cancel(Timer),
  State#state{ping_timer = none}.

broadcast(State, Txn) ->
  State1 = clr_timer(State),
  inter_dc_pub:broadcast(Txn),
  update_stream_pub:broadcast(Txn),
  {_, Id} = Txn#interdc_txn.logid_range,
  set_timer(State1#state{last_log_id = Id}).
