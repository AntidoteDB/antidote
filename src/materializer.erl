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
-module(materializer).
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    create_snapshot/1,
    update_snapshot/3,
    materialize_eager/3,
    check_operations/1,
    check_operation/1,
    belongs_to_snapshot_op/3]).

%% @doc Creates an empty CRDT
-spec create_snapshot(type()) -> snapshot().
create_snapshot(Type) ->
    Type:new().

%% @doc Applies an downstream effect to a snapshot of a crdt.
%%      This function yields an error if the crdt does not have a corresponding update operation.
-spec update_snapshot(type(), snapshot(), effect()) -> {ok, snapshot()} | {error, {unexpected_operation, effect(), type()}}.
update_snapshot(Type, Snapshot, Op) ->
    try
        Type:update(Op, Snapshot)
    catch
        _:_ ->
            {error, {unexpected_operation, Op, Type}}
    end.

%% @doc Applies updates in given order without any checks, errors are simply propagated.
-spec materialize_eager(type(), snapshot(), [effect()]) -> snapshot() | {error, {unexpected_operation, effect(), type()}}.
materialize_eager(_Type, Snapshot, []) ->
    Snapshot;
materialize_eager(Type, Snapshot, [Effect | Rest]) ->
    case update_snapshot(Type, Snapshot, Effect) of
        {error, Reason} ->
            {error, Reason};
        {ok, Result} ->
            materialize_eager(Type, Result, Rest)
    end.


%% @doc Check that in a list of client operations, all of them are correctly typed.
-spec check_operations([client_op()]) -> ok | {error, {type_check_failed, client_op()}}.
check_operations([]) ->
    ok;
check_operations([Op | Rest]) ->
    case check_operation(Op) of
        true ->
            check_operations(Rest);
        false ->
            {error, {type_check, Op}}
    end.

%% @doc Check that an operation is correctly typed.
-spec check_operation(client_op()) -> boolean().
check_operation(Op) ->
    case Op of
        {update, {_, Type, Update}} ->
            antidote_crdt:is_type(Type) andalso
                Type:is_operation(Update);
        {read, {_, Type}} ->
            antidote_crdt:is_type(Type);
        _ ->
            false
    end.

%% Should be called doesn't belong in SS
%% returns true if op is more recent than SS (i.e. is not in the ss)
%% returns false otw
-spec belongs_to_snapshot_op(snapshot_time() | ignore, dc_and_commit_time(), snapshot_time()) -> boolean().
belongs_to_snapshot_op(ignore, {_OpDc, _OpCommitTime}, _OpSs) ->
    true;
belongs_to_snapshot_op(SSTime, {OpDc, OpCommitTime}, OpSs) ->
    OpSs1 = dict:store(OpDc, OpCommitTime, OpSs),
    not vectorclock:le(OpSs1, SSTime).


-ifdef(TEST).

%% Testing update with pn_counter.
update_pncounter_test() ->
    Type = antidote_crdt_counter_pn,
    Counter = create_snapshot(Type),
    ?assertEqual(0, Type:value(Counter)),
    Op = 1,
    {ok, Counter2} = update_snapshot(Type, Counter, Op),
    ?assertEqual(1, Type:value(Counter2)).

%% Testing pn_counter with update log
materializer_counter_withlog_test() ->
    Type = antidote_crdt_counter_pn,
    Counter = create_snapshot(Type),
    ?assertEqual(0, Type:value(Counter)),
    Ops = [1,
           1,
           2,
           3
          ],
    Counter2 = materialize_eager(Type, Counter, Ops),
    ?assertEqual(7, Type:value(Counter2)).

%% Testing counter with empty update log
materializer_counter_emptylog_test() ->
    Type = antidote_crdt_counter_pn,
    Counter = create_snapshot(Type),
    ?assertEqual(0, Type:value(Counter)),
    Ops = [],
    Counter2 = materialize_eager(Type, Counter, Ops),
    ?assertEqual(0, Type:value(Counter2)).

%% Testing non-existing crdt
materializer_error_nocreate_test() ->
    ?assertException(error, undef, create_snapshot(bla)).

%% Testing crdt with invalid update operation
materializer_error_invalidupdate_test() ->
    Type = antidote_crdt_counter_pn,
    Counter = create_snapshot(Type),
    ?assertEqual(0, Type:value(Counter)),
    Ops = [{non_existing_op_type, {non_existing_op, actor1}}],
    ?assertEqual({error, {unexpected_operation,
                    {non_existing_op_type, {non_existing_op, actor1}},
                    antidote_crdt_counter_pn}},
                 materialize_eager(Type, Counter, Ops)).

%% Testing that the function check_operations works properly
check_operations_test() ->
    Operations =
        [{read, {key1, antidote_crdt_counter_pn}},
         {update, {key1, antidote_crdt_counter_pn, increment}}
        ],
    ?assertEqual(ok, check_operations(Operations)),

    Operations2 = [{read, {key1, antidote_crdt_counter_pn}},
        {update, {key1, antidote_crdt_counter_pn, {{add, elem}, a}}},
        {update, {key2, antidote_crdt_counter_pn, {increment, a}}},
        {read, {key1, antidote_crdt_counter_pn}}],
    ?assertMatch({error, _}, check_operations(Operations2)).

%% Testing belongs_to_snapshot returns true when a commit time
%% is smaller than a snapshot time
belongs_to_snapshot_test() ->
    CommitTime1a = 1,
    CommitTime2a = 1,
    CommitTime1b = 1,
    CommitTime2b = 7,
    SnapshotClockDC1 = 5,
    SnapshotClockDC2 = 5,
    CommitTime3a = 5,
    CommitTime4a = 5,
    CommitTime3b = 10,
    CommitTime4b = 10,

    SnapshotVC=vectorclock:from_list([{1, SnapshotClockDC1}, {2, SnapshotClockDC2}]),
    ?assertEqual(true, belongs_to_snapshot_op(
                 vectorclock:from_list([{1, CommitTime1a}, {2, CommitTime1b}]), {1, SnapshotClockDC1}, SnapshotVC)),
    ?assertEqual(true, belongs_to_snapshot_op(
                 vectorclock:from_list([{1, CommitTime2a}, {2, CommitTime2b}]), {2, SnapshotClockDC2}, SnapshotVC)),
    ?assertEqual(false, belongs_to_snapshot_op(
                  vectorclock:from_list([{1, CommitTime3a}, {2, CommitTime3b}]), {1, SnapshotClockDC1}, SnapshotVC)),
    ?assertEqual(false, belongs_to_snapshot_op(
                  vectorclock:from_list([{1, CommitTime4a}, {2, CommitTime4b}]), {2, SnapshotClockDC2}, SnapshotVC)).
-endif.
