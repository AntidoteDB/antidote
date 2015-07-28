-module(new_inter_dc_utils).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-define(PARTITION_BYTE_LENGTH, 20).

-export([snapshot/1, now_millisec/0, logid_range/1, commit_time/1, bin_to_txn/1, txn_to_bin/1, partition_to_bin/1]).

commit_payload(Ops) ->
  CommitPld = (lists:last(Ops))#operation.payload,
  commit = CommitPld#log_record.op_type, %% sanity check
  CommitPld.

snapshot(Ops) ->
  {_, SnapshotTime} = (commit_payload(Ops))#log_record.op_payload,
  SnapshotTime.

commit_time(Ops) ->
  {{_, CommitTime}, _} = (commit_payload(Ops))#log_record.op_payload,
  CommitTime.

logid_range(Ops) ->
  {Min, _} = (hd(Ops))#operation.op_number,
  {Max, _} = (lists:last(Ops))#operation.op_number,
  {Min, Max}.

now_millisec() ->
  {MegaSecs, Secs, MicroSecs} = erlang:now(),
  (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

pad(Width, Binary) ->
  case Width - byte_size(Binary) of
    N when N =< 0 -> Binary;
    N -> <<0:(N*8), Binary/binary>>
  end.

partition_to_bin(Partition) -> pad(?PARTITION_BYTE_LENGTH, binary:encode_unsigned(Partition)).

txn_to_bin(Txn = #interdc_txn{partition = P}) ->
  Prefix = partition_to_bin(P),
  Msg = term_to_binary(Txn),
  <<Prefix/binary, Msg/binary>>.

bin_to_txn(Bin) ->
  L = byte_size(Bin),
  Msg = binary_part(Bin, {?PARTITION_BYTE_LENGTH, L - ?PARTITION_BYTE_LENGTH}),
  binary_to_term(Msg).

