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
	 ops_to_dc_transactions/1,
	 from_ops/3,
	 ping/3,
	 is_local/1,
	 ops_by_type/2,
	 to_bin/1,
	 from_bin/1,
	 partition_to_bin/1,
	 last_log_opid/1,
	 is_ping/1]).

%% Functions
-spec ops_to_dc_transactions([#operation{}]) -> {[#interdc_txn{}], dict()}.
ops_to_dc_transactions(Ops, Partition, PrevLogIdDict) ->
    Dict = lists:foldl(fun(Op, Acc) ->
			       DCs = replication_check:get_dc_ids((Op#operation.payload)#payload.key),			       
			       lists:foldl(fun(DCID, Acc2) ->
						   dict:update(DCID, fun(Trans) ->
									     [Op|Trans]
								     end, [Trans], Acc2)
					   end, Acc, DCs)
		       end, dict:new(), Ops),
    dict:fold(fun(DCID1, OpList, {Acc1, NewLogIdDict}) ->
		      PrevDCId = case dict:find(DCID1, NewLogIdDict) of
				 {ok, OpId} -> OpId;
				 error -> 0
			     end,
		      PrevTotalId = case dict:find(total_count, NewLogIdDict) of
					{ok, OpId} -> OpId;
					error -> 0
				    end,
		      NewLogIdDict1 = dict:update_counter(total_count, 1, NewLogIdDict),
		      NewLogIdDict2 = dict:update_counter(DCID1, 1, NewLogIdDict),
		      {[from_ops(OpList, Partition, PrevDCId, PrevTotalId, DCID1, replication_check:get_dc_partitions(DCID1))
			| Acc1], NewLogIdDict2}
	      end, {[], PrevLogIdDict}, Dict).

-spec from_ops([#operation{}], partition_id(), log_opid() | none, log_opid() | none, dcid(), tuple()) -> #interdc_txn{}.
from_ops(Ops, Partition, PrevDCId, PrevTotalId, DestDC, {DestPartDict, DestPartTuple, DestPartSize}) ->
    DestPart = case dict:find(Partition, DestPartDict) of
		   {ok, Par} ->
		       Par;
		   error ->
		       element(random:uniform(DestPartSize), DestPartTuple)
	       end,
    LastOp = lists:last(Ops),
    CommitPld = LastOp#operation.payload,
    commit = CommitPld#log_record.op_type, %% sanity check
    {{DCID, CommitTime}, SnapshotTime} = CommitPld#log_record.op_payload,
    #interdc_txn{
       dest = DestDC,
       dcid = DCID,
       partition = Partition,
       prev_log_opid = PrevDCId,
       prev_log_total_opid = PrevTotalId,
       operations = Ops,
       snapshot = SnapshotTime,
       timestamp = CommitTime
      }.

-spec ping(partition_id(), log_opid(), non_neg_integer()) -> #interdc_txn{}.
ping(Partition, PrevLogOpId, Timestamp) -> #interdc_txn{
  dcid = dc_utilities:get_my_dc_id(),
  partition = Partition,
  prev_log_opid = PrevLogOpId,
  operations = [],
  snapshot = dict:new(),
  timestamp = Timestamp
}.

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
to_bin(Txn = #interdc_txn{dest = DestDC, partition = P}) ->
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
