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

%% @doc Responsible for generating the object versions requested by clients.

-module(materializer).
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/1,
    update_snapshot/3,
    apply_effects/3,
    belongs_to_snapshot_op/3]).

%% @doc Creates an empty CRDT
-spec new(type()) -> snapshot().
new(Type) ->
    antidote_crdt:new(Type).

%% @doc Applies an downstream effect to a snapshot of a crdt.
%%      This function yields an error if the crdt does not have a corresponding update operation.
-spec update_snapshot(type(), snapshot(), effect()) -> snapshot().
update_snapshot(Type, Snapshot, Op) ->
    {ok, Result} = antidote_crdt:update(Type, Op, Snapshot),
    Result.

%% @doc Applies updates in given order without any checks, errors are simply propagated.
-spec apply_effects(type(), snapshot(), [effect()]) -> snapshot().
apply_effects(Type, InitialSnapshot, Effects) ->
    lists:foldl(fun (Effect, Snapshot) -> update_snapshot(Type, Snapshot, Effect) end, InitialSnapshot, Effects).

%% Should be called doesn't belong in SS
%% returns true if op is more recent than SS (i.e. is not in the ss)
%% returns false otw
-spec belongs_to_snapshot_op(snapshot_time() | ignore, dc_and_commit_time(), snapshot_time()) -> boolean().
belongs_to_snapshot_op(ignore, _, _) ->
    true;
belongs_to_snapshot_op(SSTime, {OpDc, OpCommitTime}, OpSs) ->
    OpSs1 = vectorclock:set(OpDc, OpCommitTime, OpSs),
    not vectorclock:le(OpSs1, SSTime).


-ifdef(TEST).

%% Testing update with pn_counter.
update_pncounter_test() ->
    Type = antidote_crdt_counter_pn,
    Counter = new(Type),
    ?assertEqual(0, Type:value(Counter)),
    Op = 1,
    Counter2 = update_snapshot(Type, Counter, Op),
    ?assertEqual(1, Type:value(Counter2)).

%% Testing pn_counter with update log
materializer_counter_withlog_test() ->
    Type = antidote_crdt_counter_pn,
    Counter = new(Type),
    ?assertEqual(0, Type:value(Counter)),
    Ops = [1,
           1,
           2,
           3
          ],
    Counter2 = apply_effects(Type, Counter, Ops),
    ?assertEqual(7, Type:value(Counter2)).

%% Testing counter with empty update log
materializer_counter_emptylog_test() ->
    Type = antidote_crdt_counter_pn,
    Counter = new(Type),
    ?assertEqual(0, Type:value(Counter)),
    Ops = [],
    Counter2 = apply_effects(Type, Counter, Ops),
    ?assertEqual(0, Type:value(Counter2)).

%% Testing non-existing crdt
materializer_error_nocreate_test() ->
    ?assertException(error, {badmatch, false}, new(bla)).

materializer_test()->
    Type = antidote_crdt_counter_pn,
    PNCounter = new(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    % test - no ops
    PNCounter2 = apply_effects(Type, PNCounter, []),
    ?assertEqual(0, Type:value(PNCounter2)),
    % test - several ops
    Ops = [1, 2, 3, 4],
    PNCounter3 = apply_effects(Type, PNCounter, Ops),
    ?assertEqual(10, Type:value(PNCounter3)).


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
