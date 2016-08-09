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
-module(partial_repli_utils).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-export([check_wait_time/2,
	 replace_external_ops/2,
	 perform_external_read/5,
	 deliver_external_read_resp/2,
	 trim_ops_from_dc/5,
	 wait_for_external_read_resp/0]).

-spec deliver_external_read_resp(binary(),#request_cache_entry{}) -> ok.
deliver_external_read_resp(BinaryRep,RequestCacheEntry) ->
    Coordinator = RequestCacheEntry#request_cache_entry.req_pid,
    case Coordinator of
	{fsm, Sender} -> %% Return Type and Value directly here.
	    gen_fsm:send_event(Sender, {external_read_resp, BinaryRep});
	_ ->
	    Coordinator ! {external_read_resp, BinaryRep}
    end.

-spec wait_for_external_read_resp() -> {ok, snapshot()}.
wait_for_external_read_resp() ->
    receive
	{external_read_resp, BinaryRep} ->
	    {external_read_rep, _Key, _Type, Snapshot} = binary_to_term(BinaryRep),
	    {ok, Snapshot}
    end.

-spec perform_external_read(pdcid(), key(), type(), tx(), pid() | {fsm, pid()}) -> ok | unknown_dc.
perform_external_read({DCID,Partition},Key,Type,Transaction,Coordinator) ->
    %% First check for any ops in this DC
    StartTime = Transaction#transaction.snapshot_time - ?EXTERNAL_READ_BACK_TIME,
    SnapshotTime = Transaction#transaction.vec_snapshot_time,
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    {ok, OpList} = clocksi_readitem_fsm:get_ops(IndexNode,Key,Type,StartTime,SnapshotTime,Transaction),
    Property = #external_read_property{from_dcid=dc_meta_data_utilities:get_my_dc_id(),included_ops=OpList,included_ops_time=StartTime},
    BinaryRequest = term_to_binary({external_read, Key, Type, Transaction, Property}),
    inter_dc_query:perform_request(?EXTERNAL_READ_MSG, {DCID,Partition}, BinaryRequest,fun deliver_external_read_resp/2, Coordinator).

%% This will wait for the dependencies of the external read request if needed
-spec check_wait_time(snapshot_time(), clocksi_readitem_fsm:read_property_list()) ->
			     {ok, snapshot_time()}.
check_wait_time(MinSnapshotTime, PropertyList) ->
    case get_property(external_read_property, PropertyList) of
	ReadProp when is_record(ReadProp, external_read_property) ->
	    FromDC = ReadProp#external_read_property.from_dcid,
	    IncludedOpsTime = ReadProp#external_read_property.included_ops_time,
	    Clock = vectorclock:set_clock_of_dc(FromDC, IncludedOpsTime, MinSnapshotTime),
	    clocksi_interactive_tx_coord_fsm:wait_for_clock(Clock);
	false ->
	    {ok, MinSnapshotTime}
    end.

%% An external read will include a list of ops from the external DC (to avoid blocking)
%% This method will place those in the list of ops to be materialized locally
-spec replace_external_ops(#snapshot_get_response{}, clocksi_readitem_fsm:read_property_list()) ->
				  #snapshot_get_response{}.
replace_external_ops(SnapshotGetResp, PropertyList) ->
    case get_property(external_read_property, PropertyList) of
	ReadProp when is_record(ReadProp, external_read_property) ->
	    %% First remove any possible duplicates
	    OldOps = SnapshotGetResp#snapshot_get_response.ops_list,
	    FromDC = ReadProp#external_read_property.from_dcid,
	    IncludedOpsTime = ReadProp#external_read_property.included_ops_time,
	    OldOpsRem = remove_ops(OldOps,IncludedOpsTime,FromDC,[]),
	    %% Next instert the external ops
	    %% Note this is really expensive, but should expect the list of new ops to be short anyway
	    lager:info("Modifying the op list for external read"),
	    Ops =
		lists:foldl(fun({NewId,NewOp},OldOpsAcc) ->
				    lager:info("There is an op in the external read!!"),
				    NewOpDeps = NewOp#clocksi_payload.snapshot_time,
				    {FromDC,NewOpCT} = NewOp#clocksi_payload.commit_time,
				    insert_op(OldOpsAcc,{NewId,NewOp},NewOpDeps,NewOpCT,FromDC,[])
			    end, OldOpsRem, ReadProp#external_read_property.included_ops),
	    SnapshotGetResp#snapshot_get_response{ops_list = Ops, number_of_ops = length(Ops)};
	false ->
	    SnapshotGetResp
    end.

%% remove any ops that would be doubles from the list of ops from the external DC
%% Note: The list of ops have the most recent ones on the left
%% TODO: this needs to take a tuple now
-spec remove_ops([{integer(),clocksi_payload()}],clock_time(),dcid(),[{integer(),clocksi_payload()}]) ->
			[{integer(),clocksi_payload()}].		     
remove_ops([],_IncludedOpsTime,_DCID,Acc) ->
    lists:reverse(Acc);
remove_ops([{_Id, #clocksi_payload{commit_time = {DCID,Time}}}|RestOld],IncludedOpsTime,DCID,Acc)
  when DCID == DCID, (Time > IncludedOpsTime) ->
    %% Remove duplicate
    remove_ops(RestOld,IncludedOpsTime,DCID,Acc);
remove_ops([Op|RestOld],IncludedOpsTime,DCID,Acc) ->
    %% Don't remove
    remove_ops(RestOld,IncludedOpsTime,DCID,[Op|Acc]).

%% This will insert a new op in a list of ops in causal order
%% Note: The list of ops have the most recent ones on the left
-spec insert_op([{integer(),clocksi_payload()}],{integer(),clocksi_payload()},snapshot_time(),clock_time(),dcid(),[{integer(),clocksi_payload()}]) ->
		       [{integer(),clocksi_payload()}].
insert_op([],NewOp,_NewOpDeps,_NewOpCT,_NewOpDCID,Acc) ->
    lists:reverse([NewOp|Acc]);
insert_op([{OldId, OldOp = #clocksi_payload{commit_time = {DCID,Time}}}|RestOld],{_NewOpId,NewOp},_NewOpDeps,NewOpCT,NewOpDCID,Acc) when DCID == NewOpDCID, (Time < NewOpCT) ->
    %% Op must be ordered after the ops from its own DC with smaller CT
    %% Use the op id of the op before it in the list because need to use the op ids generated by the local materializer
    lists:reverse(Acc) ++ [{OldId,NewOp}|[{OldId,OldOp}|RestOld]];    
insert_op([{OldId,OldOp}|RestOld],NewOp,NewOpDeps,NewOpCT,NewOpDCID,Acc) ->
    {OldDCID,OldCT} = OldOp#clocksi_payload.commit_time,
    Dep = vectorclock:get_clock_of_dc(OldDCID,NewOpDeps),
    case Dep >= OldCT of
	true ->
	    %% Op must be put here, because it depends on OldOp
	    lists:reverse(Acc) ++ [NewOp|[{OldId,OldOp}|RestOld]];
	false ->
	    %% Keep going to find the place to insert the op
	    insert_op(RestOld,NewOp,NewOpDeps,NewOpCT,NewOpDCID,[{OldId,OldOp}|Acc])
    end.

%% Only keep the ops in the list that are from DCID and have committed in between the 
%% given min and max times
-spec trim_ops_from_dc([{op_num(),clocksi_payload()}],dcid(),clock_time(),clock_time(),[clocksi_payload()])
		      -> [{op_num(),clocksi_payload()}].
trim_ops_from_dc([],_DCID,_MinTime,_MaxTime,Acc) ->
    lists:reverse(Acc);
trim_ops_from_dc([{OpNum,Op = #clocksi_payload{commit_time = {DCID,Time}}}|Rest],DCID,MinTime,MaxTime,Acc)
  when DCID == DCID, (Time > MinTime), (Time =< MaxTime) ->
    %% Keep this op
    trim_ops_from_dc(Rest,DCID,MinTime,MaxTime,[{OpNum,Op}|Acc]);
trim_ops_from_dc([_Op|Rest],DCID,MinTime,MaxTime,Acc) ->
    trim_ops_from_dc(Rest,DCID,MinTime,MaxTime,Acc).

-spec get_property(external_read_property, clocksi_readitem_fsm:read_property_list()) -> clocksi_readitem_fsm:external_read_property() | false.
get_property(external_read_property, PropertyList) ->
    lists:keyfind(external_read_property,1,PropertyList).
