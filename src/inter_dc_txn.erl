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

%% API
-export([
 from_ops/4,
  ping/4,
  is_local/1,
  req_id_to_bin/1,
  to_bin/1,
  from_bin/1,
  partition_to_bin/1,
  last_log_opid/1,
  is_ping/1,
  get_bucket_sub/2,
  get_partition_sub/1,
  to_per_bucket_bin/1,
  to_local_txn_partition_list/2,
  get_bucket_sub_for_partition/1,
  get_all_local_partitions/1,
  ops_by_type/2]).

%% Functions

-type bucket_bin() :: undefined | {bucket,bucket()}.

-spec from_ops([#log_record{}], partition_id(), none | #op_number{},  none | [{dcid(),#op_number{}}]) -> #interdc_txn{}.
from_ops(Ops, Partition, PrevLogOpId, PrevLogOpIdDC) ->
    LastOp = lists:last(Ops),
    CommitPld = LastOp#log_record.log_operation,
    commit = CommitPld#log_operation.op_type, %% sanity check
    #commit_log_payload{commit_time = {DCID, CommitTime}, snapshot_time = SnapshotTime} = CommitPld#log_operation.log_payload,
    #interdc_txn{
       dcid = DCID,
       partition = Partition,
       prev_log_opid = PrevLogOpId,
       prev_log_opid_dc = PrevLogOpIdDC,
       log_records = Ops,
       snapshot = SnapshotTime,
       timestamp = CommitTime
      }.

-spec ping(partition_id(), #op_number{} | none, [{dcid(),#op_number{}}] | none, non_neg_integer()) -> #interdc_txn{}.
ping(Partition, PrevLogOpId, PrevOpIdDC, Timestamp) -> #interdc_txn{
  dcid = dc_meta_data_utilities:get_my_dc_id(),
  partition = Partition,
  prev_log_opid = PrevLogOpId,
  prev_log_opid_dc = PrevOpIdDC,
  log_records = [],
  snapshot = dict:new(),
  timestamp = Timestamp
}.

%% In case the partitioning scheme is different on the DC compared to
%% where the transaction can from, might need to seperate the transaction
-spec to_local_txn_partition_list(partition_id(),#interdc_txn{}) -> [{partition_id(),#interdc_txn{}}].
to_local_txn_partition_list(LocalPartition,Txn = #interdc_txn{log_records = Ops}) ->
    case (?IS_PARTIAL() and (Ops /= [])) of
	false -> [{LocalPartition,Txn}];
	true ->
	    PartDict = 
		lists:foldl(fun(LogOp = #log_record{log_operation = Payload}, Acc) ->
				    Type = Payload#log_operation.op_type,
				    case Type of
					update ->
					    UpdatePayload = Payload#log_operation.log_payload,
					    Key = UpdatePayload#update_log_payload.key,
					    %% TODO what about bucket??
					    %% Bucket = UpdatePayload#update_log_payload.bucket,
					    Preflist = log_utilities:get_preflist_from_key(Key),
					    {Partition,_Node} = hd(Preflist),
					    case dict:find(Partition,Acc) of
						{ok,List} ->
						    dict:store(Partition,[LogOp|List],Acc);
						error ->
						    dict:store(Partition,[LogOp],Acc)
					    end;
					_ ->
					    Acc
				    end
			    end, dict:new(), Ops),
	    CommitOp = lists:last(Ops),
	    dict:fold(fun(Partition,List,Acc) ->
			      NewTxn = Txn#interdc_txn{log_records = lists:reverse([CommitOp|List])},
			      [{Partition,NewTxn}|Acc]
		      end, [], PartDict)
    end.

%% Get all the partitions that this transaction updated at the local DC
-spec get_all_local_partitions(#interdc_txn{}) -> [partition_id()].
get_all_local_partitions(#interdc_txn{log_records = Ops}) ->
    PartitionSet = 
	lists:foldl(fun(#log_record{log_operation = LogOp},Acc) ->
			    case LogOp#log_operation.op_type of
				update ->
				    UpdatePayload = LogOp#log_operation.log_payload,
				    Key = UpdatePayload#update_log_payload.key,
				    %% TODO what about bucket??
				    %% Bucket = UpdatePayload#update_log_payload.bucket,
				    Preflist = log_utilities:get_preflist_from_key(Key),
				    {Partition,_Node} = hd(Preflist),
				    sets:add_element(Partition,Acc);
				_ ->
				    Acc
			    end
		    end, sets:new(), Ops),
    sets:to_list(PartitionSet).
				
-spec last_log_opid(#interdc_txn{}) -> {#op_number{}, [{dcid(),#op_number{}}]}.
last_log_opid(Txn = #interdc_txn{log_records = Ops, prev_log_opid = LogOpId, prev_log_opid_dc = DCLogOpId}) ->
    case is_ping(Txn) of
	true ->
	    {LogOpId, DCLogOpId};
	false ->
	    case ?IS_PARTIAL() of
		false ->
		    LastOp = lists:last(Ops),
		    CommitPld = LastOp#log_record.log_operation,
		    commit = CommitPld#log_operation.op_type, %% sanity check
		    {LastOp#log_record.op_number, []};
		true ->
		    LastOp = lists:last(Ops),
		    CommitPld = LastOp#log_record.log_operation,
		    commit = CommitPld#log_operation.op_type, %% sanity check
		    %% In partial the commit records dont store the opids (because 
		    %% ordering is based on bucket ids)
		    Op = lists:nth(length(Ops)-1,Ops),
		    {LastOp#log_record.op_number,Op#log_record.op_number_dcid}
	    end
    end.

-spec is_local(#interdc_txn{}) -> boolean().
is_local(#interdc_txn{dcid = DCID}) -> DCID == dc_meta_data_utilities:get_my_dc_id().

-spec is_ping(#interdc_txn{}) -> boolean().
is_ping(#interdc_txn{log_records = Ops}) -> Ops == [].

-spec ops_by_type(#interdc_txn{}, any()) -> [#log_record{}].
ops_by_type(#interdc_txn{log_records = Ops}, Type) ->
  F = fun(Op) -> Type == Op#log_record.log_operation#log_operation.op_type end,
  lists:filter(F, Ops).

-spec to_bin(#interdc_txn{}) -> binary().
to_bin(Txn = #interdc_txn{partition = P}) ->
  Prefix = partition_to_bin(P),
  Msg = term_to_binary(Txn),
  Type = get_type_binary(fulltxn),
  <<Type/binary, Prefix/binary, Msg/binary>>.

%% Go through a full transaction, and seperate it into a list
%% of per bucket transactions
-spec to_per_bucket_bin(#interdc_txn{}) -> [binary()].
to_per_bucket_bin(Txn = #interdc_txn{log_records = [], partition = Partition}) ->
    %% This is a ping txn
    PrefixPartition = partition_to_bin(Partition),
    %% No bucket, so just use 0's
    PrefixBucket = bucket_to_bin(undefined),
    Msg = term_to_binary(Txn),
    BinaryType = get_type_binary(singlebucket),
    [<<BinaryType/binary, PrefixPartition/binary, PrefixBucket/binary, Msg/binary>>];
to_per_bucket_bin(Txn = #interdc_txn{log_records = Ops, partition = Partition}) ->
    DictBucket = 
	lists:foldl(fun(Op = #log_record{log_operation = Payload}, Acc) ->
			    Type = Payload#log_operation.op_type,
			    case Type of
				update ->
				    Bucket = 
					case Payload#log_operation.log_payload#update_log_payload.key of
					    {_Key, Buck} ->
						{bucket, Buck};
					    _ ->
						%% No bucket, so just use 0's
						undefined
					end,
				    case dict:find(Bucket,Acc) of
					{ok,List} ->
					    dict:store(Bucket,[Op|List],Acc);
					error ->
					    dict:store(Bucket,[Op],Acc)
				    end;
				_ ->
				    Acc
			    end
		    end, dict:new(), Ops),
    CommitOp = lists:last(Ops),
    dict:fold(fun(Bucket,List,Acc) ->
		      NewTxn = Txn#interdc_txn{log_records = lists:reverse([CommitOp|List])},
		      PrefixPartition = partition_to_bin(Partition),
		      PrefixBucket = bucket_to_bin(Bucket),
		      Msg = term_to_binary(NewTxn),
		      BinaryType = get_type_binary(singlebucket),
		      [<<BinaryType/binary, PrefixPartition/binary, PrefixBucket/binary, Msg/binary>> | Acc]
	      end, [], DictBucket).

-spec get_type_binary(fulltxn | singlebucket) -> <<_:?TYPE_BIT_LENGTH>>.
get_type_binary(fulltxn) ->
    <<?FULL_TXN_SUB>>;
get_type_binary(singlebucket) ->
    <<?SINGLE_BUCKET_SUB>>.

-spec from_bin(binary()) -> #interdc_txn{}.
from_bin(<<?FULL_TXN_SUB,_:(?PARTITION_BYTE_LENGTH*8),Rest/binary>>) ->
    binary_to_term(Rest);
from_bin(<<?SINGLE_BUCKET_SUB,_:(?PARTITION_BYTE_LENGTH*8),_:(?BUCKET_BYTE_LENGTH*8),Rest/binary>>) ->
    binary_to_term(Rest).

%% Pad the binary to the given width, crash if the binary is bigger than
%% the width
-spec pad(non_neg_integer(), binary()) -> binary().
pad(Width, Binary) ->
  case Width - byte_size(Binary) of
    N when N == 0 -> Binary;
    N when N > 0 -> <<0:(N*8), Binary/binary>>
  end.

%% Takes a binary and makes it size width
%% if it is too small than it adds 0s
%% otherwise it trims bits from the left size
-spec pad_or_trim(non_neg_integer(), binary()) -> binary().
pad_or_trim(Width, Binary) ->
    case Width - byte_size(Binary) of
	N when N == 0 -> Binary;
	N when N < 0 ->
	    Pos = trunc(abs(N)),
	    <<_:Pos/binary, Rest:Width/binary>> = Binary,
	    Rest;
	N -> <<0:(N*8), Binary/binary>>
  end.

-spec partition_to_bin(partition_id()) -> binary().
partition_to_bin(Partition) -> pad(?PARTITION_BYTE_LENGTH, binary:encode_unsigned(Partition)).

%% These are interdc message ids, as non-neg-integers, encoded as unsigned
%% They are of a fixed binary size, looping back to zero
%% once the max size is reached (by triming the bits on the left)
-spec req_id_to_bin(non_neg_integer()) -> binary().
req_id_to_bin(ReqId) ->
    pad_or_trim(?REQUEST_ID_BYTE_LENGTH, binary:encode_unsigned(ReqId)).
			   
-spec get_partition_sub(partition_id()) -> [binary(),...].
get_partition_sub(Partition) ->
    case ?IS_PARTIAL() of
	false ->
	    Type = get_type_binary(fulltxn),
	    PartitionBin = partition_to_bin(Partition),
	    [<<Type/binary, PartitionBin/binary>>];
	true ->
	    get_bucket_sub_for_partition(Partition)
    end.

-spec bucket_to_bin(bucket_bin()) -> binary().
bucket_to_bin(undefined) ->
    %% No bucket, so just use 0's
    pad(?BUCKET_BYTE_LENGTH, <<0>>);
bucket_to_bin({bucket, Bucket}) ->
    %% lager:info("bucket is ~p", [Bucket]),
    pad(?BUCKET_BYTE_LENGTH, term_to_binary(Bucket)).

%% Returns the zmq subscription prefix for the
%% given bucket/parition
-spec get_bucket_sub(partition_id(),bucket_bin()) -> binary().
get_bucket_sub(Partition,Bucket) ->
    Type = get_type_binary(singlebucket),
    PartitionBin = partition_to_bin(Partition),
    BucketBin = bucket_to_bin(Bucket),
    <<Type/binary, PartitionBin/binary, BucketBin/binary>>.

%% Returns the zmq subscription prefix for all buckets
%% replicated by this DC for a given parition
-spec get_bucket_sub_for_partition(partition_id()) -> [binary(),...].
get_bucket_sub_for_partition(Partition) ->
    BucketList = replication_check:get_my_buckets(),
    SubList = 
	lists:map(fun(Bucket) ->
			  get_bucket_sub(Partition,{bucket,Bucket})
		  end, BucketList),
    %% Always sub to all updates that dont have a bucket
    [get_bucket_sub(Partition,undefined)|SubList].

