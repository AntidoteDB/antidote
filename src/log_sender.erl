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
-module(log_sender).
-behaviour(riak_core_vnode).

%% Each logging_vnode informs this vnode about every new appended operation.
%% This vnode assembles operations into transactions, and sends the transactions to appropriate destinations.

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-export([start_vnode/1, send/2]).
-export([init/1, handle_command/3, handle_coverage/4, handle_exit/3, handoff_starting/2, handoff_cancelled/1, handoff_finished/2, handle_handoff_command/3, handle_handoff_data/2, encode_handoff_item/2, is_empty/1, terminate/2, delete/1]).

-record(state, {
  partition :: partition_id(),
  buffer %% log_tx_assembler:state
}).

%% API
start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).
init([Partition]) -> {ok, #state{partition = Partition, buffer = log_txn_assembler:new_state()}}.
send(Partition, Operation) -> dc_utilities:call_vnode_sync(Partition, log_sender_master, {log_event, Operation}).

handle_command({log_event, Operation}, _Sender, State) ->
  {Result, NewBufState} = log_txn_assembler:process(Operation, State#state.buffer),
  case Result of
    {ok, Txn} -> handle_transaction(State#state.partition, dc_utilities:get_my_dc_id(), Txn);
    none -> none
  end,
  {reply, ok, State#state{buffer = NewBufState}}.

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

handle_transaction(Partition, DCID, Txn) ->
  new_inter_dc_pub:broadcast_transaction({DCID, Partition}, Txn).
