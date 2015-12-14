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
  delete/1]).

%% State
-record(state, {
  partition :: non_neg_integer(),
  use_delay :: boolean(),
  dc_delays :: dict(),
  buffer_fsms :: dict() %% dcid -> buffer
}).

%%%% API --------------------------------------------------------------------+

-spec deliver_txn(#interdc_txn{}) -> ok.
deliver_txn(Txn) -> call(Txn#interdc_txn.partition, {txn, Txn}).

-spec deliver_log_reader_resp(pdcid(), [#interdc_txn{}]) -> ok.
deliver_log_reader_resp({DCID, Partition}, Txns) -> call(Partition, {log_reader_resp, DCID, Txns}).

%%%% VNode methods ----------------------------------------------------------+

init([Partition]) -> {ok, #state{partition = Partition, buffer_fsms = dict:new(), use_delay = false, dc_delays = dict:new()}}.
start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

handle_command({add_delay, DCID, Delay}, _Sender, State = #state{dc_delays = DcDelays}) ->
    {reply, ok, State#state{use_delay = true, dc_delays = dict:store(DCID, Delay, DcDelays)}};

handle_command({txn_delayed, Txn = #interdc_txn{dcid = DCID}}, _Sender, State) ->
    process_txn(Txn, DCID, State);

handle_command({txn, Txn = #interdc_txn{dcid = DCID}}, _Sender, State = #state{use_delay = UseDelay, dc_delays = DcDelays}) ->
    DelayTime =
	case UseDelay of
	    true ->
		case dict:find(DCID, DcDelays) of
		    {ok, Time} ->
			Time;
		    error ->
			false
		end;
	    false ->
		false
	end,
    case DelayTime of
	false ->
	    process_txn(Txn, DCID, State);
	_ ->
	    lager:info("Adding delay ~p, for ~p", [DelayTime, DCID]),
	    riak_core_vnode:send_command_after(DelayTime, {txn_delayed, Txn}),
	    {noreply, State}
    end;

handle_command({log_reader_resp, DCID, Txns}, _Sender, State) ->
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

call(Partition, Request) -> dc_utilities:call_vnode(Partition, inter_dc_sub_vnode_master, Request).

process_txn(Txn, DCID, State) ->
    Buf0 = get_buf(DCID, State),
    Buf1 = inter_dc_sub_buf:process({txn, Txn}, Buf0),
    {noreply, set_buf(DCID, Buf1, State)}.

get_buf(DCID, State) ->
  case dict:find(DCID, State#state.buffer_fsms) of
    {ok, Buf} -> Buf;
    error -> inter_dc_sub_buf:new_state({DCID, State#state.partition})
  end.

set_buf(DCID, Buf, State) -> State#state{buffer_fsms = dict:store(DCID, Buf, State#state.buffer_fsms)}.
