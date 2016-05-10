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
-define(TYPE_BYTE_LENGTH, 1).

%% API
-export([
  from_ops/3,
  ping/3,
  is_local/1,
  get_key_sub/1,
  get_partition_sub/1,
  to_per_key_bin/1,
  ops_by_type/2, to_bin/1, from_bin/1, partition_to_bin/1, last_log_opid/1, is_ping/1]).

%% Functions

-spec from_ops([#operation{}], partition_id(), #op_number{} | none) -> #interdc_txn{}.
from_ops(Ops, Partition, PrevLogOpId) ->
  LastOp = lists:last(Ops),
  CommitPld = LastOp#operation.payload,
  commit = CommitPld#log_record.op_type, %% sanity check
  {{DCID, CommitTime}, SnapshotTime} = CommitPld#log_record.op_payload,
  #interdc_txn{
    dcid = DCID,
    partition = Partition,
    prev_log_opid = PrevLogOpId,
    operations = Ops,
    snapshot = SnapshotTime,
    timestamp = CommitTime
  }.

-spec ping(partition_id(), #op_number{} | none, non_neg_integer()) -> #interdc_txn{}.
ping(Partition, PrevLogOpId, Timestamp) -> #interdc_txn{
  dcid = dc_meta_data_utilities:get_my_dc_id(),
  partition = Partition,
  prev_log_opid = PrevLogOpId,
  operations = [],
  snapshot = dict:new(),
  timestamp = Timestamp
}.

-spec last_log_opid(#interdc_txn{}) -> #op_number{}.
last_log_opid(Txn = #interdc_txn{operations = Ops, prev_log_opid = LogOpId}) ->
  case is_ping(Txn) of
    true -> LogOpId;
    false ->
      LastOp = lists:last(Ops),
      CommitPld = LastOp#operation.payload,
      commit = CommitPld#log_record.op_type, %% sanity check
      Max = LastOp#operation.op_number,
      Max
  end.

-spec is_local(#interdc_txn{}) -> boolean().
is_local(#interdc_txn{dcid = DCID}) -> DCID == dc_meta_data_utilities:get_my_dc_id().

-spec is_ping(#interdc_txn{}) -> boolean().
is_ping(#interdc_txn{operations = Ops}) -> Ops == [].

-spec ops_by_type(#interdc_txn{}, any()) -> [#operation{}].
ops_by_type(#interdc_txn{operations = Ops}, Type) ->
  F = fun(Op) -> Type == Op#operation.payload#log_record.op_type end,
  lists:filter(F, Ops).

-spec to_bin(#interdc_txn{}) -> binary().
to_bin(Txn = #interdc_txn{partition = P}) ->
  Prefix = partition_to_bin(P),
  Msg = term_to_binary(Txn),
  Type = get_type_binary(fulltxn),
  <<Type/binary, Prefix/binary, Msg/binary>>.

-spec to_per_key_bin(#interdc_txn{}) -> [binary()].
to_per_key_bin(Txn = #interdc_txn{operations = Ops}) ->
    lists:foldl(fun(Op = #operation{payload = Payload}, Acc) ->
			Type = Payload#log_record.op_type,
			case Type of
			    update ->
				CommitOp = lists:last(Ops),
				NewTxn = Txn#interdc_txn{operations = [Op,CommitOp]},
				{Key, _, _} = Payload#log_record.op_payload,
				Prefix = key_to_bin(Key),
				Msg = term_to_binary(NewTxn),
				BinaryType = get_type_binary(singlekey),
				[<<BinaryType/binary, Prefix/binary, Msg/binary>> | Acc];
			    _ ->
				Acc
			end
		end, [], Ops).

-spec get_type_binary(atom()) -> binary().
get_type_binary(fulltxn) ->
    <<0:(?TYPE_BYTE_LENGTH*8)>>;
get_type_binary(singlekey) ->
    <<1:(?TYPE_BYTE_LENGTH*8)>>.

-spec from_bin(binary()) -> #interdc_txn{}.
from_bin(Bin) ->
  L = byte_size(Bin),
  HeaderSize = ?TYPE_BYTE_LENGTH + ?PARTITION_BYTE_LENGTH,
  Msg = binary_part(Bin, {HeaderSize, L - HeaderSize}),
  binary_to_term(Msg).

%% Pad the binary to the given width, crash if the binary is bigger than
%% the width
-spec pad(non_neg_integer(), binary()) -> binary().
pad(Width, Binary) ->
  case Width - byte_size(Binary) of
    N when N == 0 -> Binary;
    N when N > 0 -> <<0:(N*8), Binary/binary>>
  end.

-spec partition_to_bin(partition_id()) -> binary().
partition_to_bin(Partition) -> pad(?PARTITION_BYTE_LENGTH, binary:encode_unsigned(Partition)).

-spec get_partition_sub(partition_id()) -> binary().
get_partition_sub(Partition) ->
    Type = get_type_binary(fulltxn),
    PartitionBin = partition_to_bin(Partition),
    <<Type/binary, PartitionBin/binary>>.

-spec key_to_bin(term()) -> binary().
key_to_bin(Key) ->
    pad(?PARTITION_BYTE_LENGTH, term_to_binary(Key)).

-spec get_key_sub(term()) -> binary().
get_key_sub(Key) ->
    Type = get_type_binary(singlekey),
    KeyBin = key_to_bin(Key),
    <<Type/binary, KeyBin/binary>>.
