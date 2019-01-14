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

%% This vnode is responsible for receiving transactions from remote DCs and
%% passing them on to appropriate buffer FSMs

-module(inter_dc_sub_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([
  deliver_txn/1,
  deliver_log_reader_resp/2]).

%% Vnode methods
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

%% State
-record(state, {
  partition :: non_neg_integer(),
  buffer_fsms :: dict:dict(dcid(), #inter_dc_sub_buf{}) %% dcid -> buffer
}).

%%%% API --------------------------------------------------------------------+

-spec deliver_txn(#interdc_txn{}) -> ok.
deliver_txn(Txn) -> call(Txn#interdc_txn.partition, {txn, Txn}).

%% This function is called with the response from the log request operations request
%% when some messages were lost
-spec deliver_log_reader_resp(binary(), #request_cache_entry{}) -> ok.
deliver_log_reader_resp(BinaryRep, _RequestCacheEntry) ->
    <<Partition:?PARTITION_BYTE_LENGTH/big-unsigned-integer-unit:8, RestBinary/binary>> = BinaryRep,
    call(Partition, {log_reader_resp, RestBinary}).

%%%% VNode methods ----------------------------------------------------------+

init([Partition]) -> {ok, #state{partition = Partition, buffer_fsms = dict:new()}}.
start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

handle_command({txn, Txn = #interdc_txn{dcid = DCID}}, _Sender, State) ->
  Buf0 = get_buf(DCID, State),
  Buf1 = inter_dc_sub_buf:process({txn, Txn}, Buf0),
  {noreply, set_buf(DCID, Buf1, State)};

handle_command({log_reader_resp, BinaryRep}, _Sender, State) ->
  %% The binary reply is type {pdcid(), [#interdc_txn{}]}
  {{DCID, _Partition}, Txns} = binary_to_term(BinaryRep),
  Buf0 = get_buf(DCID, State),
  Buf1 = inter_dc_sub_buf:process({log_reader_resp, Txns}, Buf0),
  {noreply, set_buf(DCID, Buf1, State)}.

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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec call(partition_id(), {txn, #interdc_txn{}} | {log_reader_resp, binary()}) -> ok.
call(Partition, Request) -> dc_utilities:call_local_vnode(Partition, inter_dc_sub_vnode_master, Request).

-spec get_buf(dcid(), #state{}) -> #inter_dc_sub_buf{}.
get_buf(DCID, State) ->
  case dict:find(DCID, State#state.buffer_fsms) of
    {ok, Buf} -> Buf;
    error -> inter_dc_sub_buf:new_state({DCID, State#state.partition})
  end.

-spec set_buf(dcid(), #inter_dc_sub_buf{}, #state{}) -> #state{}.
set_buf(DCID, Buf, State) -> State#state{buffer_fsms = dict:store(DCID, Buf, State#state.buffer_fsms)}.
