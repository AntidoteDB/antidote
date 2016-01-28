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
-module(inter_dc_txn).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-define(PARTITION_BYTE_LENGTH, 40).

%% API
-export([
	 ops_to_dc_transactions/4,
	 ping/3,
	 is_local/1,
	 ops_by_type/2,
	 to_bin/1,
	 from_bin/1,
	 partition_to_bin/1,
	 last_log_opid/1,
	 is_ping/1]).

%% Functions
-spec ops_to_dc_transactions([#operation{}]) -> {[#interdc_txn{}], dcid(), dict()}.
ops_to_dc_transactions([CommitOp | NotCommitOps], Partition, MyDCID, PrevLogIdDict) ->
    CommitId = Operation#operation.op_number,
    %% Go through the ops checking where they are replicated
    %% For each DC where it is replicated there will be an entry in the dict
    %% The key is the DCID and the value is the list of ops replicated there
    Dict = lists:foldl(fun(Op, Acc) ->
			       DCs = replication_check:get_dc_ids((Op#operation.payload)#payload.key),			       
			       lists:foldl(fun(DCID, Acc2) ->
						   dict:update(DCID, fun(Trans) ->
									     [Op|Trans]
								     end, [Op | [CommitOp]], Acc2)
					   end, Acc, DCs)
		       end, dict:new(), NotCommitOps),
    PrevTotalId = case dict:find(total_count, PrevLogIdDict) of
		      {ok, OpId} -> OpId;
		      error -> 0
		  end,
    %% The new max Id for all txns is the id of the commit op
    NewLogIdDict = dict:store(total_count, CommitId, PrevLogIdDict),
    %% Now go through the dict, and create a inter-dc txn for each record
    %% For each DC that will receive the txn, update the locally stored dict that keeps
    %% track of the last id sent for that DC this is stored, by key containing the id
    %% of this partition and the destination partition
    dict:fold(fun(DCID1, OpList, {Acc1, NewLogIdDict1}) ->
		      DestPart = get_dc_partition(Partition, DCID1),
		      {PrevDCId, DCDict} = case dict:find(DCID1, NewLogIdDict1) of
					       {ok, DCDictOps} ->
						   case dict:find({Partition, DestPart}, DCDictOps) of
						       {ok, OpId1} -> {OpId1, DcDictOps};
						       error -> {0, DcDictOps}
						   end;
					       error ->
						   {0, dict:new()}
					   end,
		      NewLogIdDict2 = dict:store(DCID1, dict:store({Partition, DestPart}, CommitId, DCDict), NewLogIdDict1),
		      CommitPld = CommitOp#operation.payload,
		      commit = CommitPld#log_record.op_type, %% sanity check
		      {{DCID, CommitTime}, SnapshotTime} = CommitPld#log_record.op_payload,
		      Txn = #interdc_txn{
			       dest = DCID1,
			       dcid = MyDCID,
			       dest_partition = DestPart,
			       partition = Partition,
			       prev_log_opid = PrevDCId,
			       prev_log_total_opid = PrevTotalId,
			       operations = Ops,
			       snapshot = SnapshotTime,
			       timestamp = CommitTime
			      },
		      {[Txn | Acc1], NewLogIdDict2}
	      end, {[], NewLogIdDict}, Dict).

%% Check if the destination DC contains the same partition id as the local partition that this txn was created on
%% If no, just pick a random partition to send it to on the destination DC
-spec get_dc_partition(partition_id(), dcid()) -> partition_id().
get_dc_partition(Partition, DCID) ->
    {DestPartDict, DestPartTuple, DestPartSize} = replication_check:get_dc_partitions(DCID),
    case dict:find(Partition, DestPartDict) of
	{ok, Par} ->
	    Par;
	error ->
	    element(random:uniform(DestPartSize), DestPartTuple)
    end.

-spec ping(partition_id(), dict(), dict()) -> [#interdc_txn{}].
ping(Partition, PrevLogIdDict, TimestampDict) ->
    %% PrevLogIdDict needs to be the size of number of connections
    %% so when it is received the receiver can know if there are
    %% any msgs missing on any connection
    DCs = replication_check:get_all_dc_ids(),
    MyDCID = dc_utilities:get_my_dc_id(),
    PrevTotalId = case dict:find(total_count, PrevLogIdDict) of
		      {ok, OpId} -> OpId;
		      error -> 0
		  end,
    lists:map(fun(DCID) ->
		      DestPart = get_dc_partition(Partition, DCID),
		      DCDictOpId = case dict:find(DCID, PrevLogIdDict) of
				       {ok, DCDict} -> DCDict;
				       error -> dict:new()
				   end,
		      Timestamp = case dict:find(DCID, TimestampDict) of
				      {ok, Timestamp} -> Timestamp;
				      error -> 0
				  end,
		      #interdc_txn{
			 dest = DCID,
			 dcid = MyDCID,
			 dest_partition = DestPart,
			 partition = Partition,
			 op_id_dict = DCDictOpId,
			 %% prev_log_opid = ,
			 pre_log_total_opid = PreTotalId,
			 operations = [],
			 snapshot = dict:new(),
			 timestamp = Timestamp
			}
	      end, [], DCs).

-spec last_log_opid(#interdc_txn{}) -> log_opid().
last_log_opid(Txn = #interdc_txn{operations = Ops, prev_log_opid = LogOpId}) ->
  case is_ping(Txn) of
    true -> LogOpId;
    false ->
      LastOp = lists:last(Ops),
      CommitPld = LastOp#operation.payload,
      commit = CommitPld#log_record.op_type, %% sanity check
      {Max, _} = LastOp#operation.op_number,
      Max
  end.

-spec is_local(#interdc_txn{}) -> boolean().
is_local(#interdc_txn{dcid = DCID}) -> DCID == dc_utilities:get_my_dc_id().

-spec is_ping(#interdc_txn{}) -> boolean().
is_ping(#interdc_txn{operations = Ops}) -> Ops == [].

-spec ops_by_type(#interdc_txn{}, any()) -> [#operation{}].
ops_by_type(#interdc_txn{operations = Ops}, Type) ->
  F = fun(Op) -> Type == Op#operation.payload#log_record.op_type end,
  lists:filter(F, Ops).

-spec to_bin(#interdc_txn{}) -> binary().
to_bin(Txn = #interdc_txn{dest = DestDC, dest_partition = {_, P}}) ->
    Prefix = dcid_to_bin(DestDC, P),
    Msg = term_to_binary(Txn),
    <<Prefix/binary, Msg/binary>>.

-spec from_bin(binary()) -> #interdc_txn{}.
from_bin(Bin) ->
    L = byte_size(Bin),
    Msg = binary_part(Bin, {?PARTITION_BYTE_LENGTH, L - ?PARTITION_BYTE_LENGTH}),
    binary_to_term(Msg).

-spec pad(non_neg_integer(), binary()) -> binary().
pad(Width, Binary) when byte_size(Binary) <= Width ->
    case Width - byte_size(Binary) of
	N when N =< 0 -> Binary;
	N -> <<0:(N*8), Binary/binary>>
    end.

-spec partition_to_bin(partition_id()) -> binary().
partition_to_bin(Partition) -> pad(?PARTITION_BYTE_LENGTH, binary:encode_unsigned(Partition)).

-spec dcid_to_bin(dcid(), partition_id()) -> binary().
dcid_to_bin(DCID, Partition) ->
    pad(?PARTITION_BYTE_LENGTH, <<atom_to_binary(DCID)/binary, binary:encode_unsigned(Partition)/binary>>).
