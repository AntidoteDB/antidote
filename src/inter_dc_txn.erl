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

-define(PARTITION_BYTE_LENGTH, 20).

%% API
-export([
  from_ops/2,
  ping/3,
  is_local/1,
  ops_by_type/2, to_bin/1, from_bin/1, partition_to_bin/1]).

%% Functions

-spec from_ops([#operation{}], partition_id()) -> #interdc_txn{}.
from_ops(Ops, Partition) ->
  FirstOp = hd(Ops),
  LastOp = lists:last(Ops),
  CommitPld = LastOp#operation.payload,
  commit = CommitPld#log_record.op_type, %% sanity check
  {{DCID, CommitTime}, SnapshotTime} = CommitPld#log_record.op_payload,
  {Min, _} = FirstOp#operation.op_number,
  {Max, _} = LastOp#operation.op_number,
  #interdc_txn{
    dcid = DCID,
    partition = Partition,
    logid_range = {Min, Max},
    operations = Ops,
    snapshot = SnapshotTime,
    timestamp = CommitTime
  }.

%-spec ping(partition_id(), log_id(), non_neg_integer()) -> #interdc_txn{}.
ping(Partition, LogNum, Timestamp) -> #interdc_txn{
  dcid = dc_utilities:get_my_dc_id(),
  partition = Partition,
  logid_range = {LogNum, LogNum},
  operations = [],
  snapshot = dict:new(),
  timestamp = Timestamp
}.

-spec is_local(#interdc_txn{}) -> boolean().
is_local(#interdc_txn{dcid = DCID}) -> DCID == dc_utilities:get_my_dc_id().

-spec ops_by_type(#interdc_txn{}, any()) -> [#operation{}].
ops_by_type(#interdc_txn{operations = Ops}, Type) ->
  F = fun(Op) -> Type == Op#operation.payload#log_record.op_type end,
  lists:filter(F, Ops).

to_bin(Txn = #interdc_txn{partition = P}) ->
  Prefix = partition_to_bin(P),
  Msg = term_to_binary(Txn),
  <<Prefix/binary, Msg/binary>>.

from_bin(Bin) ->
  L = byte_size(Bin),
  Msg = binary_part(Bin, {?PARTITION_BYTE_LENGTH, L - ?PARTITION_BYTE_LENGTH}),
  binary_to_term(Msg).

pad(Width, Binary) ->
  case Width - byte_size(Binary) of
    N when N =< 0 -> Binary;
    N -> <<0:(N*8), Binary/binary>>
  end.

partition_to_bin(Partition) -> pad(?PARTITION_BYTE_LENGTH, binary:encode_unsigned(Partition)).