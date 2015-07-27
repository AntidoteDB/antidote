-module(new_inter_dc_utils).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-export([snapshot/1, now_millisec/0, logid_range/1, commit_time/1]).

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