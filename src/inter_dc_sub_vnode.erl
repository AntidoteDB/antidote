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
  deliver_txn/2,
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
  partition :: partition_id(),
  buffer_fsms :: dict() %% dcid -> buffer
}).

%%%% API --------------------------------------------------------------------+

-spec deliver_txn(#interdc_txn{}, {dict(),partition_id()}) -> ok.
deliver_txn(Txn = #interdc_txn{prev_log_opid_dc = #partial_ping{}}, {_MyNodePartitions,_DefaultPartition}) ->
    _ = dc_utilities:bcast_my_vnode(inter_dc_sub_vnode_master, {partial_ping, Txn}),
    ok;
deliver_txn(Txn = #interdc_txn{partition=FromPartition}, {MyNodePartitions,DefaultPartition}) ->
    %% TODO: better way to choose partition for transaction from a partition
    %% that doesn't exist at this DC
    Partition = 
	case dict:is_key(FromPartition, MyNodePartitions) of
	    true -> FromPartition;
	    false -> DefaultPartition
	end,
    call(Partition, {txn, Txn}).

%% This function is called with the response from the log request operations request
%% when some messages were lost
-spec deliver_log_reader_resp(binary(),#request_cache_entry{}) -> ok.
deliver_log_reader_resp(BinaryRep,#request_cache_entry{extra_state = LocalPartition}) ->
    <<_ExternalPartition:?PARTITION_BYTE_LENGTH/big-unsigned-integer-unit:8, RestBinary/binary>> = BinaryRep,
    call(LocalPartition, {log_reader_resp, RestBinary}).

%%%% VNode methods ----------------------------------------------------------+

init([Partition]) -> {ok, #state{partition = Partition, buffer_fsms = dict:new()}}.
start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

handle_command({partial_ping, Txn = #interdc_txn{dcid = DCID}}, _Sender, State) ->
    %% TODO, this should only be for partitions that are subbed by this node
    NewState =
	lists:foldl(fun(Partition,AccState) ->
			    Buf0 = get_buf({DCID,Partition}, AccState),
			    Buf1 = inter_dc_sub_buf:process({txn, Txn}, Buf0),
			    set_buf({DCID,Partition},Buf1,AccState)
		    end, State, [State#state.partition]),
    {noreply, NewState};

handle_command({txn, Txn = #interdc_txn{dcid = DCID, partition = Partition}}, _Sender, State) ->
    %% lager:info("got a txn: ~p", [Txn]),
    Buf0 = get_buf({DCID,Partition}, State),
    Buf1 = inter_dc_sub_buf:process({txn, Txn}, Buf0),
    {noreply, set_buf({DCID,Partition}, Buf1, State)};

handle_command({log_reader_resp, BinaryRep}, _Sender, State) ->
  %% The binary reply is type {pdcid(), [#interdc_txn{}]}
  {PDCID, Txns} = binary_to_term(BinaryRep),
  Buf0 = get_buf(PDCID, State),
  Buf1 = inter_dc_sub_buf:process({log_reader_resp, Txns}, Buf0),
  {noreply, set_buf(PDCID, Buf1, State)}.

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
-spec call(partition_id(), {txn, #interdc_txn{}} | {log_reader_resp, binary()}) -> ok.
call(Partition, Request) -> dc_utilities:call_local_vnode(Partition, inter_dc_sub_vnode_master, Request).

-spec get_buf(pdcid(),#state{}) -> #inter_dc_sub_buf{}.
get_buf(PDCID, State) ->
  case dict:find(PDCID, State#state.buffer_fsms) of
    {ok, Buf} -> Buf;
    error -> inter_dc_sub_buf:new_state(PDCID, State#state.partition)
  end.

-spec set_buf(pdcid(), #inter_dc_sub_buf{}, #state{}) -> #state{}.
set_buf(PDCID, Buf, State) -> State#state{buffer_fsms = dict:store(PDCID, Buf, State#state.buffer_fsms)}.
