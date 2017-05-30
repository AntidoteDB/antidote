%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 SyncFree Consortium.  All Rights Reserved.
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

%% This module exports functions used for the compression of
%% operations contained inside lists of transactions.

-module(inter_dc_compression_buffer).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
    compress/1,
    compress_and_broadcast/1]).

%% Compresses a list of transactions and then broadcasts the final transaction.
-spec compress_and_broadcast([#interdc_txn{}]) -> ok.
compress_and_broadcast(Buffer) ->
    inter_dc_pub:broadcast(compress(Buffer)).

%% Compresses a list of transactions and returns a single compressed transaction.
-spec compress([#interdc_txn{}]) -> #interdc_txn{} | none.
compress([]) -> none;
compress(Buffer) ->
    % get the transaction record we will reuse to build the new one (most recent one)
    Txn = lists:last(Buffer),
    % TODO: perhaps we should generate a new txid instead of reusing the most recent in the list?
    %       if this change is applied the log_records in `Commits` must also have their txid changed
    TxnId = get_txid(Txn),
    {CompressableOps, ReversedOtherOps} =
        lists:foldl(fun(#interdc_txn{log_records = Logs}, Acc) ->
            split_transaction_records(Logs, Acc, TxnId)
        end, {#{}, []}, Buffer),
    % compress the operation logs
    CompressedMapping = maps:map(fun(_, LogRecords) -> compress_log_records(lists:reverse(LogRecords)) end, CompressableOps),
    % get the prepare and commit log_records from the transaction we are reusing
    Commits = lists:filter(fun(Record) -> get_log_op_type(Record) =/= update end, Txn#interdc_txn.log_records),
    % get the prev_log_opid from the earliest transaction in the buffer
    FstTxn = hd(Buffer),
    PrevLogOpId = FstTxn#interdc_txn.prev_log_opid,
    % build a list of all the already compressed operations.
    Ops = lists:flatten(maps:values(CompressedMapping)),
    % reverse to get the correct ordering.
    OtherOps = lists:reverse(ReversedOtherOps),
    Txn#interdc_txn{log_records = OtherOps ++ Ops ++ Commits, prev_log_opid = PrevLogOpId}.

%%%% Private

%% Grabs the transaction id from an interdc_txn record.
get_txid(Txn) ->
    Record = hd(Txn#interdc_txn.log_records),
    Record#log_record.log_operation#log_operation.tx_id.

%% Splits a collection of #log_record{} into a tuple containing:
%% - a map of `{Key, Bucket} => [#log_record{}]` for the compressable data types;
%% - a list of `#log_record{}` for the remaining data types;
-spec split_transaction_records([#log_record{}], {#{}, [#log_record{}]}, txid()) -> {#{}, [#log_record{}]}.
split_transaction_records(Logs, {Compressable, Other}, TxId) ->
  lists:foldl(fun(Log, Acc) -> place_txn_record(Log, Acc, TxId) end, {Compressable, Other}, Logs).

%% Updates the #log_record{} txid and places the #log_record{} into the correct tuple slot.
-spec place_txn_record(#log_record{}, {#{}, [#log_record{}]}, txid()) -> {#{}, [#log_record{}]}.
place_txn_record(
    LogRecordArg = #log_record{
        log_operation = #log_operation{
            op_type = OpType,
            log_payload = LogPayload
        }
    },
    {Compressable, Other},
    TxId) ->
    case OpType of
        update ->
            % update #log_record txid
            LogOp = LogRecordArg#log_record.log_operation,
            LogRecord = LogRecordArg#log_record{
                log_operation = LogOp#log_operation{
                    tx_id = TxId
                }
            },
            {Key, Bucket, Type, _Op} = destructure_update_payload(LogPayload),
            case antidote_crdt:is_compressable(Type) of
                true ->
                    K = {Key, Bucket},
                    Compressable1 = case maps:is_key(K, Compressable) of
                        true ->
                            Current = maps:get(K, Compressable),
                            maps:put(K, [LogRecord | Current], Compressable);
                        false -> maps:put(K, [LogRecord], Compressable)
                    end,
                    {Compressable1, Other};
                false -> {Compressable, [LogRecord | Other]}
            end;
        _ -> {Compressable, Other}
    end.

%% Gets the log op type from a #log_record{}.
-spec get_log_op_type(#log_record{}) -> update | prepare | commit.
get_log_op_type(#log_record{log_operation = #log_operation{op_type = Type}}) ->
    Type.

%% Gets the CRDT op() from a #log_record{}.
-spec get_op(#log_record{}) -> op().
get_op(#log_record{log_operation = #log_operation{log_payload = #update_log_payload{op = Op}}}) ->
    Op.

%% Gets the CRDT type() from a #log_record{}.
-spec get_type(#log_record{}) -> type().
get_type(#log_record{log_operation = #log_operation{log_payload = #update_log_payload{type = Type}}}) ->
    Type.

%% Replaces the CRDT op() in the nested #log_record{} with the given op().
-spec replace_op(#log_record{}, op()) -> #log_record{}.
replace_op(LogRecord, Op) ->
    LogOp = LogRecord#log_record.log_operation,
    LogPayload = LogOp#log_operation.log_payload,
    LogRecord#log_record{
        log_operation = LogOp#log_operation{
            log_payload = LogPayload#update_log_payload{op = Op}
        }
    }.

%% Destructures an #update_log_payload{} record to the tuple {key(), bucket(), type(), op()}.
-spec destructure_update_payload(#update_log_payload{}) -> {key(), bucket(), type(), op()}.
destructure_update_payload(#update_log_payload{key = Key, bucket = Bucket, type = Type, op = Op}) ->
    {Key, Bucket, Type, Op}.

%% Compresses the given list of #log_record{} records.
-spec compress_log_records([#log_record{}]) -> [#log_record{}].
compress_log_records(LogRecords) ->
    % This builds a propagation log starting from scratch, adding each #log_record{} one-by-one.
    % Every time a new record is added using log/2 it attempts to compress the newly added record.
    lists:reverse(lists:foldl(fun(LogRecord, LogAcc) ->
        log(LogAcc, LogRecord)
    end, [], LogRecords)).

%% Adds a #log_record{} to the log and runs the compression.
-spec log([#log_record{}], #log_record{}) -> [#log_record{}].
log(LogAcc, LogRecord) ->
    case log_(LogAcc, LogRecord) of
        {ok, Logs} -> Logs;
        {append, Logs, NewLogRecord} -> [NewLogRecord | Logs]
    end.

%% Helper function for log/2.
-spec log_([#log_record{}], #log_record{}) -> {ok, [#log_record{}]} | {append, [#log_record{}], #log_record{}}.
log_([], LogRecord) -> {append, [], LogRecord};
log_([LogRecord2 | Rest], LogRecord1) ->
    Type = get_type(LogRecord1),
    Op1 = get_op(LogRecord1),
    Op2 = get_op(LogRecord2),
    case Type:can_compress(Op2, Op1) of
        true ->
            case Type:compress(Op2, Op1) of
                {noop, noop} -> {ok, Rest};
                {noop, NewOp1} ->
                    NewRecordOp1 = replace_op(LogRecord1, NewOp1),
                    log_(Rest, NewRecordOp1);
                {NewOp2, noop} ->
                    NewRecordOp2 = replace_op(LogRecord2, NewOp2),
                    {ok, [NewRecordOp2 | Rest]};
                {NewOp2, NewOp1} ->
                    NewRecordOp1 = replace_op(LogRecord1, NewOp1),
                    NewRecordOp2 = replace_op(LogRecord2, NewOp2),
                    case log_(Rest, NewRecordOp1) of
                        {ok, List} -> {ok, [NewRecordOp2 | List]};
                        {append, List, AppendOp} -> {append, [NewRecordOp2 | List], AppendOp}
                    end
            end;
        false ->
            % Could not compress the two operations, but we can still commmute them.
            case log_(Rest, LogRecord1) of
                {ok, List} -> {ok, [LogRecord2 | List]};
                {append, List, AppendOp} -> {append, [LogRecord2 | List], AppendOp}
            end
    end.

%%% Tests

-ifdef(TEST).
inter_dc_txn_from_ops(Ops, PrevLogOpId, N, TxId, CommitTime, SnapshotTime) ->
    {Records, Number} = lists:foldl(fun({Key, Bucket, Type, Op}, {List, Number}) ->
        Record = #log_record{
            version = 0,
            op_number = Number,
            bucket_op_number = Number,
            log_operation = #log_operation{
                tx_id = TxId,
                op_type = update,
                log_payload = #update_log_payload{
                    key = Key,
                    bucket = Bucket,
                    type = Type,
                    op = Op
                }
            }
        },
        {[Record | List], Number + 1}
    end, {[], N}, Ops),
    {RecordsCCRDT, RecordsOther} = split_transaction_records(lists:reverse(Records), {#{}, []}, TxId),
    Prepare = #log_record{version = 0, op_number = Number, bucket_op_number = Number, log_operation = #log_operation{tx_id = TxId, op_type = prepare, log_payload = #prepare_log_payload{prepare_time = CommitTime - 1}}},
    Commit = #log_record{version = 0, op_number = Number + 1, bucket_op_number = Number + 1, log_operation = #log_operation{tx_id = TxId, op_type = commit, log_payload = #commit_log_payload{commit_time = CommitTime, snapshot_time = SnapshotTime}}},
    LogRecords = lists:reverse(RecordsOther) ++ lists:flatten(lists:map(fun lists:reverse/1, maps:values(RecordsCCRDT))) ++ [Prepare, Commit],
    #interdc_txn{
    dcid = replica1,
    partition = 1,
    prev_log_opid = PrevLogOpId,
    snapshot = SnapshotTime,
    timestamp = CommitTime,
    log_records = LogRecords
    }.

empty_txns_test() ->
    ?assertEqual(compress([]), none).

orset_test() ->
    Buffer1 = [
        inter_dc_txn_from_ops([{key, bucket, antidote_crdt_orset, [{5, [<<"a">>], []}]}],
                              0,
                              1,
                              2,
                              300,
                              250)
    ],
    ?assertEqual(compress(Buffer1), hd(Buffer1)),
    Buffer2 = [
        inter_dc_txn_from_ops([{key, bucket, antidote_crdt_orset, [{5, [<<"a">>], []}]},
                               {key, bucket, antidote_crdt_orset, [{5, [<<"b">>], [<<"a">>]}]},
                               {key, bucket, antidote_crdt_orset, [{5, [<<"c">>], [<<"b">>]}]}],
                              0,
                              1,
                              2,
                              300,
                              250)
    ],
    Expected2 = inter_dc_txn_from_ops(
        [{key, bucket, antidote_crdt_orset, [{5, [<<"c">>], []}]}],
        0,
        3,
        2,
        300,
        250
    ),
    ?assertEqual(compress(Buffer2), Expected2),
    Buffer3 = [
        inter_dc_txn_from_ops([{key, bucket, antidote_crdt_orset, [{5, [<<"a">>], []}]},
                               {key, bucket, antidote_crdt_orset, [{5, [<<"b">>], [<<"a">>]}]},
                               {key, bucket, antidote_crdt_orset, [{6, [<<"z">>], []}]}],
                              0,
                              1,
                              2,
                              300,
                              250),
        inter_dc_txn_from_ops([{key, bucket, antidote_crdt_orset, [{5, [<<"c">>], [<<"b">>]}]}],
                              3,
                              4,
                              3,
                              400,
                              350)
    ],
    Expected3 = inter_dc_txn_from_ops(
        [{key, bucket, antidote_crdt_orset, [{5, [<<"c">>], []}, {6, [<<"z">>], []}]}],
        0,
        4,
        3,
        400,
        350
    ),
    ?debugFmt("~p~n", [compress(Buffer3)]),
    ?assertEqual(compress(Buffer3), Expected3).
-endif.
