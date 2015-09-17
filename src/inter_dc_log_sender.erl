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
  send/2]).
-export([
  init/1,
  handle_command/3,
  handle_info/2,
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
  last_log_id :: log_opid(),
  timer :: any()
}).

%% API
start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).
send(Partition, Operation) -> dc_utilities:call_vnode_sync(Partition, inter_dc_log_sender_master, {log_event, Operation}).

init([Partition]) ->
  {ok, set_timer(#state{
    partition = Partition,
    buffer = log_txn_assembler:new_state(),
    last_log_id = 0,
    timer = none
  })}.

handle_command({log_event, Operation}, _Sender, State) ->
  {Result, NewBufState} = log_txn_assembler:process(Operation, State#state.buffer),
  State1 = State#state{buffer = NewBufState},
  State2 = case Result of
    {ok, Ops} ->
      Txn = inter_dc_txn:from_ops(Ops, State1#state.partition, State#state.last_log_id),
      %% sanity check - we only publish locally committed transactions
      case inter_dc_txn:is_local(Txn) of
        true -> broadcast(State1, Txn);
        false -> State1
      end;
    none -> State1
  end,
  {reply, ok, State2}.

handle_info(ping, State) ->
  PingTxn = inter_dc_txn:ping(State#state.partition, State#state.last_log_id, get_stable_time(State#state.partition)),
  {ok, set_timer(broadcast(State, PingTxn))}.

handle_coverage(_Req, _KeySpaces, _Sender, State) -> {stop, not_implemented, State}.
handle_exit(_Pid, _Reason, State) -> {noreply, State}.
handoff_starting(_TargetNode, State) -> {true, del_timer(State)}.
handoff_cancelled(State) -> {ok, set_timer(State)}.
handoff_finished(_TargetNode, State) -> {ok, set_timer(State)}.
handle_handoff_command( _Message , _Sender, State) -> {noreply, State}.
handle_handoff_data(_Data, State) -> {reply, ok, State}.
encode_handoff_item(Key, Operation) -> term_to_binary({Key, Operation}).
is_empty(State) -> {true, State}.
terminate(_Reason, State) -> del_timer(State), ok.
delete(State) -> {ok, del_timer(State)}.

%%%%%%%%%%%%%%%%%%%%%%%%

del_timer(State = #state{timer = none}) -> State;
del_timer(State = #state{timer = Timer}) -> erlang:cancel_timer(Timer), State#state{timer = none}.

set_timer(State) ->
  State1 = del_timer(State),
  State1#state{timer = erlang:send_after(?HEARTBEAT_PERIOD, self(), ping)}.

broadcast(State, Txn) ->
  inter_dc_pub:broadcast(Txn),
  Id = inter_dc_txn:last_log_opid(Txn),
  State#state{last_log_id = Id}.

%% @doc Return smallest snapshot time of active transactions.
%%      No new updates with smaller timestamp will occur in future.
get_stable_time(_Partition) ->
    Now = inter_dc_utils:now_millisec(),
    Now.
%% case clocksi_vnode:get_active_txns_call(Partition) of
    %%     {ok, Active_txns} ->
    %%         lists:foldl(fun({_TxId, Snapshot_time}, Min_time) ->
    %%                             case Min_time > Snapshot_time of
    %%                                 true ->
    %%                                     Snapshot_time;
    %%                                 false ->
    %%                                     Min_time
    %%                             end
    %%                     end,
    %% 			Now,
    %%                     Active_txns);
    %%     _ -> Now
    %% end.
