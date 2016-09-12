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
  deliver_txn/3,
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
  ping_count :: dict(),
  partition :: partition_id(),
  buffer_fsms :: dict() %% dcid -> buffer
}).

%%%% API --------------------------------------------------------------------+

-spec deliver_txn(#interdc_txn{}, dict(), dict()) -> ok.
deliver_txn(Txn = #interdc_txn{dcid = DCID, prev_log_opid_dc = #partial_ping{}}, _MyNodePartitions, DictPartitionMatch) ->
    {_PartitionMatch,PartitionMatchReverse} = dict:fetch(DCID, DictPartitionMatch),
    ok = dict:fold(fun(LocalP,ExternalPList,ok) ->
			 call(LocalP, {send_partial_ping, Txn, ExternalPList})
		 end, ok, PartitionMatchReverse),
    %% TODO, this should also be broadcast to all nodes (to the inter_dc_sub process) in the local DC
    ok;
deliver_txn(Txn = #interdc_txn{dcid = DCID, partition=FromPartition}, MyNodePartitions, DictPartitionMatch) ->
    {PartitionMatch,PartitionMatchReverse} = dict:fetch(DCID, DictPartitionMatch),
    Partition = 
	case dict:is_key(FromPartition, MyNodePartitions) of
	    true -> FromPartition;
	    false ->
		%% For knowing what partition to send to if it comes from a partition that doesn't exist at this DC
		dict:fetch(FromPartition,dict:fetch(DCID, PartitionMatch))
	end,
    call(Partition, {txn, Txn, dict:fetch(Partition,PartitionMatchReverse)}).

%% This function is called with the response from the log request operations request
%% when some messages were lost
-spec deliver_log_reader_resp(binary(),#request_cache_entry{}) -> ok.
deliver_log_reader_resp(BinaryRep,#request_cache_entry{extra_state = LocalPartition}) ->
    <<_ExternalPartition:?PARTITION_BYTE_LENGTH/big-unsigned-integer-unit:8, RestBinary/binary>> = BinaryRep,
    call(LocalPartition, {log_reader_resp, RestBinary}).

%%%% VNode methods ----------------------------------------------------------+

init([Partition]) -> {ok, #state{partition = Partition, buffer_fsms = dict:new(), ping_count = dict:new()}}.
start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

handle_command({send_partial_ping, Txn = #interdc_txn{dcid = DCID}, ExternalPList}, _Sender, State) ->
    %% TODO, this should only be for partitions that are subbed by this node
    NewState =
	lists:foldl(fun(Partition,AccState) ->
			    Buf0 = get_buf({DCID,Partition}, ExternalPList, AccState),
			    {Buf1,PingList} = inter_dc_sub_buf:process({txn, Txn}, Buf0),
			    set_buf({DCID,Partition},Buf1,PingList,AccState)
		    end, State, [State#state.partition|ExternalPList]),
    {noreply, NewState};

handle_command({txn, Txn = #interdc_txn{dcid = DCID, partition = Partition}, ExternalPList}, _Sender, State) ->
    %% lager:info("got a txn: ~p", [Txn]),
    Buf0 = get_buf({DCID,Partition}, ExternalPList, State),
    {Buf1,PingList} = inter_dc_sub_buf:process({txn, Txn}, Buf0),
    {noreply, set_buf({DCID,Partition}, Buf1, PingList, State)};

handle_command({log_reader_resp, BinaryRep}, _Sender, State) ->
  %% The binary reply is type {pdcid(), [#interdc_txn{}]}
  {PDCID, Txns} = binary_to_term(BinaryRep),
  Buf0 = get_buf(PDCID, State),
  {Buf1,PingList} = inter_dc_sub_buf:process({log_reader_resp, Txns}, Buf0),
  {noreply, set_buf(PDCID, Buf1, PingList, State)}.

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
-spec call(partition_id(), {send_partial_ping, #interdc_txn{}, [partition_id()]} | {txn, #interdc_txn{}, [partition_id()]} | {log_reader_resp, binary()}) -> ok.
call(Partition, Request) -> dc_utilities:call_local_vnode(Partition, inter_dc_sub_vnode_master, Request).

-spec get_buf(pdcid(),#state{}) -> #inter_dc_sub_buf{}.
get_buf(PDCID, State) ->
    {ok, Buf} = dict:find(PDCID, State#state.buffer_fsms),
    Buf.

-spec get_buf(pdcid(),[partition_id()],#state{}) -> #inter_dc_sub_buf{}.
get_buf(PDCID, ExternalPList, State) ->
  case dict:find(PDCID, State#state.buffer_fsms) of
    {ok, Buf} -> Buf;
    error -> inter_dc_sub_buf:new_state(PDCID, State#state.partition, length(ExternalPList))
  end.

-spec set_buf(pdcid(), #inter_dc_sub_buf{}, [#interdc_txn{}], #state{}) -> #state{}.
set_buf({DCID,Partition}, Buf, PingList, State) ->
    PingDict = case dict:find(DCID, State#state.ping_count) of
		   {ok, Value} -> Value;
		   error -> dict:new()
	       end,
    %% All partitions count is the number of partitions from the external DC that
    %% this local node is responsible for replicating.
    %% When each of them has confirmed a ping has received all dependencies, then
    %% that ping can be used to update the stable time
    AllPartitionsCount = Buf#inter_dc_sub_buf.external_partition_count,
    NewPingDict = 
	lists:foldl(fun(Ping,Acc) ->
			    case dict:find(Ping, Acc) of
				{ok, AllPartitionsCount} ->
				    %% send ping
				    inter_dc_dep_vnode:handle_transaction(Ping,State#state.partition,State#state.partition),
				    dict:erase(Ping,Acc);
				{ok, _Val} ->
				    dict:update_counter(Ping, 1, Acc);
				error ->
				    case AllPartitionsCount of
					0 ->
					    %% send ping
					    inter_dc_dep_vnode:handle_transaction(Ping,State#state.partition,State#state.partition),
					    dict:erase(Ping,Acc);
					_ ->
					    dict:update_counter(Ping, 1, Acc)
				    end
			    end
		    end, PingDict, PingList),
    State#state{buffer_fsms = dict:store({DCID,Partition}, Buf, State#state.buffer_fsms), ping_count = dict:store(DCID, NewPingDict, State#state.ping_count)}.
