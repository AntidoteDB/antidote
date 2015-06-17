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

%% @doc : This vnode is responsible for sending transaction committed in local
%%  DCs to remote DCs in commit-time order

-module(new_inter_dc_pusher_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").

-export([start_vnode/1, init/1, handle_command/3, handle_coverage/4, handle_exit/3, handoff_starting/2, handoff_cancelled/1, handoff_finished/2, handle_handoff_command/3, handle_handoff_data/2, encode_handoff_item/2, is_empty/1, terminate/2, delete/1]).

-record(state, {reader}).
-define(REPL_PERIOD, 5000).

start_vnode(I) ->
  {ok, Pid} = riak_core_vnode_master:get_vnode_pid(I, ?MODULE),
  riak_core_vnode:send_command(Pid, trigger),
  {ok, Pid}.

init([Partition]) ->
  DcId = dc_utilities:get_my_dc_id(),
  {ok, Reader} = clocksi_transaction_reader:init(Partition, DcId),
  {ok, #state{reader = Reader}}.

handle_command(trigger, _Sender, State=#state{reader=Reader}) ->
  timer:sleep(?REPL_PERIOD),
  {NewReaderState, Transactions} = clocksi_transaction_reader:get_next_transactions(Reader),
  lists:foreach(fun broadcast_transaction/1, Transactions),
  riak_core_vnode:send_command(self(), trigger),
  {noreply, State#state{reader = NewReaderState}}.

broadcast_transaction(Transaction) ->
  new_inter_dc_pub:broadcast({replicate, Transaction}).


handle_handoff_command(_Message, _Sender, State) -> {noreply, State}.
handoff_starting(_TargetNode, State) -> {true, State}.
handoff_cancelled(State) -> {ok, State}.
handoff_finished(_TargetNode, State) -> {ok, State}.
handle_handoff_data(_Data, State) -> {reply, ok, State}.
encode_handoff_item(_ObjectName, _ObjectValue) -> <<>>.
is_empty(State) -> {true, State}.
delete(State) -> {ok, State}.
handle_coverage(_Req, _KeySpaces, _Sender, State) -> {stop, not_implemented, State}.
handle_exit(_Pid, _Reason, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.