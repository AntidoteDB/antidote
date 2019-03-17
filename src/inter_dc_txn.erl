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

-module(inter_dc_txn).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([
  from_ops/3,
  ping/3,
  is_local/1,
  req_id_to_bin/1,
  ops_by_type/2,
  to_bin/1,
  from_bin/1,
  partition_to_bin/1,
  last_log_opid/1,
  is_ping/1]).

%% Functions

-spec from_ops([#log_record{}], partition_id(), #op_number{} | none) -> #interdc_txn{}.
from_ops(Ops, Partition, PrevLogOpId) ->
  LastOp = lists:last(Ops),
  CommitPld = LastOp#log_record.log_operation,
  commit = CommitPld#log_operation.op_type, %% sanity check
  #commit_log_payload{commit_time = {DCID, CommitTime}, snapshot_time = SnapshotTime} = CommitPld#log_operation.log_payload,
  #interdc_txn{
    dcid = DCID,
    partition = Partition,
    prev_log_opid = PrevLogOpId,
    log_records = Ops,
    snapshot = SnapshotTime,
    timestamp = CommitTime
  }.

-spec ping(partition_id(), #op_number{} | none, non_neg_integer()) -> #interdc_txn{}.
ping(Partition, PrevLogOpId, Timestamp) -> #interdc_txn{
  dcid = dc_meta_data_utilities:get_my_dc_id(),
  partition = Partition,
  prev_log_opid = PrevLogOpId,
  log_records = [],
  snapshot = dict:new(),
  timestamp = Timestamp
}.

-spec last_log_opid(#interdc_txn{}) -> #op_number{}.
last_log_opid(Txn = #interdc_txn{log_records = Ops, prev_log_opid = LogOpId}) ->
    case is_ping(Txn) of
        true -> LogOpId;
        false ->
            LastOp = lists:last(Ops),
            CommitPld = LastOp#log_record.log_operation,
            commit = CommitPld#log_operation.op_type, %% sanity check
            LastOp#log_record.op_number
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
  <<Prefix/binary, Msg/binary>>.

-spec from_bin(binary()) -> #interdc_txn{}.
from_bin(Bin) ->
  L = byte_size(Bin),
  Msg = binary_part(Bin, {?PARTITION_BYTE_LENGTH, L - ?PARTITION_BYTE_LENGTH}),
  binary_to_term(Msg).

-spec pad(non_neg_integer(), binary()) -> binary().
pad(Width, Binary) ->
  case Width - byte_size(Binary) of
    N when N =< 0 -> Binary;
    N -> <<0:(N*8), Binary/binary>>
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
%% once the max size is reached (by trimming the bits on the left)
-spec req_id_to_bin(non_neg_integer()) -> binary().
req_id_to_bin(ReqId) ->
    pad_or_trim(?REQUEST_ID_BYTE_LENGTH, binary:encode_unsigned(ReqId)).
