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

-module(clocksi_materializer).
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([materialize/3]).

%% The materializer is given of tuple containing ordered update operations.
%% Each update operation has an id number that is one larger than
%% the previous. This function takes as input that tuple and returns the id number of the first update
%% operation (i.e. the one with the largest id).
-spec get_first_id([{non_neg_integer(), clocksi_payload()}] | tuple()) ->
                          non_neg_integer().
get_first_id([]) ->
    0;
get_first_id([{Id, _Op}|_]) ->
    Id;
get_first_id(Tuple) when is_tuple(Tuple) ->
    Length = get_current_length_oplist(Tuple),
    case Length of
        0 -> 0;
        _ ->
            {Id, _Op} = element(?FIRST_OP+(Length-1), Tuple),
            Id
    end.

get_current_length_oplist(TupleOps) ->
    {Length, _ListLen} = element(2, TupleOps),
    Length.


%% @doc Applies the operation of a list to a previously created CRDT snapshot. Only the
%%      operations that are not already in the previous snapshot and
%%      with smaller timestamp than the specified
%%      are considered. Newer operations are discarded.
%%      Input:
%%      Type: The type of CRDT to create
%%      Snapshot: Current state of the CRDT
%%      SnapshotCommitTime: The time used to describe the state of the CRDT given in Snapshot
%%      MinSnapshotTime: The threshold time given by the reading transaction
%%      Ops: The list of operations to apply in causal order
%%
%%      Output: A tuple. The first element is ok, the second is the CRDT after applying the operations,
%%      the third element 1 minus the number of the operation with the smallest id not included in the snapshot,
%%      the fourth element is the smallest vectorclock that describes this snapshot,
%%      the fifth element is a boolean, if it is true it means that the returned snapshot contains
%%      more operations than the one given as input, false otherwise.
%%      The sixth element is an integer representing the number of operations applied to make the snapshot
-spec materialize(type(),
                  snapshot_time() | ignore,
                  snapshot_get_response()
                 ) ->
                         {ok, snapshot(), integer(), snapshot_time() | ignore,
                          boolean(), non_neg_integer()} | {error, reason()}.
materialize(Type, MinSnapshotTime,
            #snapshot_get_response{snapshot_time = BaseTime, ops_list = Ops,
                                   materialized_snapshot = #materialized_snapshot{last_op_id = LastOp, value = BaseSnapshot}}) ->
    FirstId = get_first_id(Ops),
    {FilteredOps, NewLastOp, LastOpCt, IsNewSnapshotTime} =
        filter_oplist([], LastOp, FirstId, BaseTime, MinSnapshotTime,
                           Ops, BaseTime, false, 0),
    try
        NewSnapshot = apply_operations(Type, BaseSnapshot, FilteredOps),
        {ok, NewSnapshot, NewLastOp, LastOpCt, IsNewSnapshotTime, length(FilteredOps)}
    catch
        _:_ ->
        {error, {unexpected_operations, FilteredOps, Type}}
    end.


%% @doc Applies a list of operations to a snapshot
%%      Input:
%%          Type: The type of CRDT of the snapshot
%%          Snapshot: The initial snapshot to apply the operations to
%%          OpList: The list of operations to apply
%%      Output: Snapshot with the operations applied to
-spec apply_operations(type(), snapshot(), [clocksi_payload()]) -> snapshot().

apply_operations(Type, BaseSnapshot, Ops) ->
    lists:foldl(fun(Op, Snapshot) -> materializer:update_snapshot(Type, Snapshot, Op#clocksi_payload.op_param) end, BaseSnapshot, Ops).

%% @doc Internal function that goes through a list of operations and a snapshot
%%      time and returns which operations from the list should be applied for
%%      the given snapshot time.
%%      Input:
%%      Type: The type of the CRDT
%%      OpList: Should be given initially as an empty list, this will accumulate
%%      the operations to apply.
%%      LastOp: 1 minus the number of the operation with the smallest id not included in the initial snapshot
%%      FirstHole: The variable keeps track of 1 minus the number of the operation with the smallest id
%%      not included in the new snapshot that is currently being generated, it should be initialised to the
%%      id of the first op in OpList
%%      SnapshotCommitTime: The time used to describe the initial state of the CRDT given in Snapshot
%%      MinSnapshotTime: The threshold time given by the reading transaction
%%      Ops: The list of operations to apply in causal order, the most recent op is on the left
%%      TxId: The Id of the transaction requesting the snapshot
%%      LastOpCommitTime: The snapshot time of the last operation in the list of operations to apply
%%      NewSS: Boolean that is true if any operations should be applied, fail otherwise.  Should start as false.
%%
%%      Output: A tuple with 4 elements or an error.  The first element of the tuple is the atom ok.
%%      The second element is the list of operations that should be applied to the snapshot.
%%      The third element 1 minus the number of the operation with the smallest id not included in the snapshot.
%%      The fourth element is the snapshot time of the last operation in the list.
%%      The fifth element is a boolean, true if a new snapshot should be generated, false otherwise.
-spec filter_oplist(
                         [clocksi_payload()],
                         integer(),
                         integer(),
                         snapshot_time(),
                         snapshot_time(),
                         [{integer(), clocksi_payload()}] | tuple(),
                         snapshot_time() | ignore,
                         boolean(),
                         non_neg_integer()) ->
                                {[clocksi_payload()], integer(), snapshot_time()|ignore, boolean()}.
filter_oplist(OpList, _LastOp, FirstHole, _BaseSnapshotTime, _MinSnapshotTime, [], LastOpCt, NewSS, _Location) ->
    {OpList, FirstHole, LastOpCt, NewSS};

%% Here: Operations to be applied are given as list
filter_oplist(OpList, LastOp, FirstHole, BaseSnapshotTime, MinSnapshotTime, [{OpId, Op}|Rest], LastOpCt, NewSS, Location) ->
    Result = materialize_intern_perform(OpList, FirstHole, BaseSnapshotTime, MinSnapshotTime, {OpId, Op}, LastOpCt, NewSS),
    case Result of
        {NewOpList, NewLastOpCt, NewSS1, NewHole} ->
            filter_oplist(NewOpList, LastOp, NewHole, BaseSnapshotTime,
                               MinSnapshotTime, Rest, NewLastOpCt, NewSS1, Location+1)
    end;

%% Here: Operations to be applied are given as tuple
filter_oplist(OpList, LastOp, FirstHole, BaseSnapshotTime, MinSnapshotTime, TupleOps, LastOpCt, NewSS, Location) ->
    Length = get_current_length_oplist(TupleOps),
    case Length == Location of
        true ->
            {OpList, FirstHole, LastOpCt, NewSS};
        false ->
            Result = materialize_intern_perform(OpList, FirstHole, BaseSnapshotTime, MinSnapshotTime,
                                       element((?FIRST_OP+Length-1) - Location, TupleOps), LastOpCt, NewSS),
            case Result of
                {NewOpList, NewLastOpCt, NewSS1, NewHole} ->
                    filter_oplist(NewOpList, LastOp, NewHole, BaseSnapshotTime, MinSnapshotTime, TupleOps, NewLastOpCt, NewSS1, Location+1)
                end
    end.

materialize_intern_perform(OpList, FirstHole, BaseSnapshotTime, MinSnapshotTime, {OpId, Op}, LastOpCt, IsUpdated) ->
    OpCom=Op#clocksi_payload.commit_time,
    OpSS=Op#clocksi_payload.snapshot_time,
    case (is_op_in_snapshot(OpCom, OpSS, MinSnapshotTime, BaseSnapshotTime, LastOpCt)) of
                         {true, NewOpCt} ->
                             %% Include the new op because it has a timestamp bigger than the snapshot being generated
                             {[Op | OpList], NewOpCt, true, FirstHole};
                         {false, _} ->
                             %% Dont include the op
                             {OpList, LastOpCt, IsUpdated, OpId-1}; % no update
                         already_in_snapshot ->
                             %% Dont Include the op, because it was already in the SS
                             {OpList, LastOpCt, IsUpdated, FirstHole}
    end.

%% @doc Check whether an udpate is included in a snapshot and also
%%      if that update is newer than a snapshot's commit time
%%      (i.e. the operation happened after the snapshot)
%%      Input:
%%      {OpDc, OpCommitTime}: The DC and commit time of the operation
%%      OperationSnapshotTime: The snapshot time of the operation
%%      SnapshotTime: The snapshot time to check if the operation is included in
%%      LastSnapshot: The previous snapshot that is being used to generate the new snapshot
%%      PrevTime: The snapshot time of the previous operation that was checked
%%
%%      Output: A tuple of 3 elements.  The first element is a boolean that is true
%%      if the operation should be included in the snapshot false otherwise, the second element
%%      the snapshot time of the last operation to
%%      be applied to the snapshot
-spec is_op_in_snapshot(dc_and_commit_time(), snapshot_time(), snapshot_time(),
                        snapshot_time(), snapshot_time()) -> {boolean(), snapshot_time() | ignore} | already_in_snapshot.
is_op_in_snapshot({OpDc, OpCommitTime}, OperationSnapshotTime, SnapshotTime, LastSnapshot, PrevTime) ->
    %% First check if the op was already included in the previous snapshot
    case materializer:belongs_to_snapshot_op(LastSnapshot, {OpDc, OpCommitTime}, OperationSnapshotTime) of
        false ->
            %% If not, check if it should be included in the new snapshot
            %% Replace the snapshot time of the dc where the transaction committed with the commit time
            OpSSCommit = vectorclock:set(OpDc, OpCommitTime, OperationSnapshotTime),
            case vectorclock:le(OpSSCommit, SnapshotTime) of
                true ->
                    PrevTime2 = case PrevTime of
                                        ignore ->
                                            OpSSCommit;
                                        _ ->
                                            PrevTime
                                    end,
                    NewTime = vectorclock:max([PrevTime2, OpSSCommit]),
                    {true, NewTime};
                false ->
                    {false, PrevTime}
                end;
        true ->
            %% was already in the prev ss, done searching ops
            already_in_snapshot
    end.

-ifdef(TEST).

materializer_clocksi_test()->
    Type = antidote_crdt_counter_pn,
    PNCounter = materializer:new(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    %%  need to add the snapshot time for these for the test to pass
    Op1 = #clocksi_payload{key = abc, type = Type,
                           op_param = 2,
                           commit_time = {1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{1, 1}])},
    Op2 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {1, 2}, txid = 2, snapshot_time=vectorclock:from_list([{1, 2}])},
    Op3 = #clocksi_payload{key = abc, type =Type,
                           op_param = 1,
                           commit_time = {1, 3}, txid = 3, snapshot_time=vectorclock:from_list([{1, 3}])},
    Op4 = #clocksi_payload{key = abc, type = Type,
                           op_param = 2,
                           commit_time = {1, 4}, txid = 4, snapshot_time=vectorclock:from_list([{1, 4}])},

    Ops = [{4, Op4}, {3, Op3}, {2, Op2}, {1, Op1}],

    SS = #snapshot_get_response{snapshot_time = ignore, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = 0, value = PNCounter}},
    {ok, PNCounter2, 3, CommitTime2, _SsSave, _} = materialize(Type,
                                                             vectorclock:from_list([{1, 3}]),
                                                            SS),
    ?assertEqual({4, vectorclock:from_list([{1, 3}])}, {Type:value(PNCounter2), CommitTime2}),
    {ok, PNcounter3, 4, CommitTime3, _SsSave1, _} = materialize(Type,
                                                             vectorclock:from_list([{1, 4}]),
                                                             SS),
    ?assertEqual({6, vectorclock:from_list([{1, 4}])}, {Type:value(PNcounter3), CommitTime3}),

    {ok, PNcounter4, 4, CommitTime4, _SsSave2, _} = materialize(Type,
                                                            vectorclock:from_list([{1, 7}]),
                                                            SS),
    ?assertEqual({6, vectorclock:from_list([{1, 4}])}, {Type:value(PNcounter4), CommitTime4}).

%% This test tests when a a snapshot is generated that does not include all of the updates in the
%% list of operations, precisely in the case where an operation is not taken, but the operations to
%% the left and right of it in the list are taken.  When this snapshot is then used for a future
%% read with a different timestamp, this missing value must be checked.
materializer_missing_op_test() ->
    Type = antidote_crdt_counter_pn,
    PNCounter = materializer:new(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    Op1 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{1, 1}, {2, 1}])},
    Op2 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {1, 2}, txid = 2, snapshot_time=vectorclock:from_list([{1, 2}, {2, 1}])},
    Op3 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {2, 2}, txid = 3, snapshot_time=vectorclock:from_list([{1, 1}, {2, 1}])},
    Op4 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {1, 3}, txid = 2, snapshot_time=vectorclock:from_list([{1, 2}, {2, 1}])},
    Ops = [{4, Op4}, {3, Op3}, {2, Op2}, {1, Op1}],

    SS = #snapshot_get_response{snapshot_time = ignore, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = 0, value = PNCounter}},
    {ok, PNCounter2, LastOp, CommitTime2, _SsSave, _} = materialize(Type,
                                                                 vectorclock:from_list([{1, 3}, {2, 1}]),
                                                                 SS),
    ?assertEqual({3, vectorclock:from_list([{1, 3}, {2, 1}])}, {Type:value(PNCounter2), CommitTime2}),

    SS2 = #snapshot_get_response{snapshot_time = CommitTime2, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = LastOp, value = PNCounter2}},
    {ok, PNCounter3, 4, CommitTime3, _SsSave2, _} = materialize(Type,
                                                            vectorclock:from_list([{1, 3}, {2, 2}]),
                                                            SS2),
    ?assertEqual({4, vectorclock:from_list([{1, 3}, {2, 2}])}, {Type:value(PNCounter3), CommitTime3}).

%% This test tests the case when there are updates that only snapshots that contain entries from one of the DCs.
%% This can happen for example if an update is commited before the DCs have been connected.
%% It ensures that when we read using a snapshot with and without all the DCs we still include the correct updates.
materializer_missing_dc_test() ->
    Type = antidote_crdt_counter_pn,
    PNCounter = materializer:new(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    Op1 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{1, 1}])},
    Op2 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {1, 2}, txid = 2, snapshot_time=vectorclock:from_list([{1, 2}])},
    Op3 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {2, 2}, txid = 3, snapshot_time=vectorclock:from_list([{2, 1}])},
    Op4 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {1, 3}, txid = 2, snapshot_time=vectorclock:from_list([{1, 2}])},
    Ops = [{4, Op4}, {3, Op3}, {2, Op2}, {1, Op1}],

    SS = #snapshot_get_response{snapshot_time = ignore, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = 0, value = PNCounter}},
    {ok, PNCounterA, LastOpA, CommitTimeA, _SsSave1, _} = materialize(Type,
                                                                  vectorclock:from_list([{1, 3}]),
                                                                  SS),
    ?assertEqual({3, vectorclock:from_list([{1, 3}])}, {Type:value(PNCounterA), CommitTimeA}),

    SS2 = #snapshot_get_response{snapshot_time = CommitTimeA, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = LastOpA, value = PNCounterA}},
    {ok, PNCounterB, 4, CommitTimeB, _SsSave2, _} = materialize(Type,
                                                            vectorclock:from_list([{1, 3}, {2, 2}]),
                                                            SS2),
    ?assertEqual({4, vectorclock:from_list([{1, 3}, {2, 2}])}, {Type:value(PNCounterB), CommitTimeB}),

    {ok, PNCounter2, LastOp, CommitTime2, _SsSave3, _} = materialize(Type,
                                                                 vectorclock:from_list([{1, 3}, {2, 1}]),
                                                                 SS),
    ?assertEqual({3, vectorclock:from_list([{1, 3}])}, {Type:value(PNCounter2), CommitTime2}),

    SS3 = #snapshot_get_response{snapshot_time = CommitTime2, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = LastOp, value = PNCounter2}},
    {ok, PNCounter3, 4, CommitTime3, _SsSave4, _} = materialize(Type,
                                                            vectorclock:from_list([{1, 3}, {2, 2}]),
                                                            SS3),
    ?assertEqual({4, vectorclock:from_list([{1, 3}, {2, 2}])}, {Type:value(PNCounter3), CommitTime3}).

materializer_clocksi_concurrent_test() ->
    Type = antidote_crdt_counter_pn,
    PNCounter = materializer:new(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    Op1 = #clocksi_payload{key = abc, type = Type,
                           op_param = 2,
                           commit_time = {1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{1, 1}, {2, 1}])},
    Op2 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {1, 2}, txid = 2, snapshot_time=vectorclock:from_list([{1, 2}, {2, 1}])},
    Op3 = #clocksi_payload{key = abc, type = Type,
                           op_param = 1,
                           commit_time = {2, 2}, txid = 3, snapshot_time=vectorclock:from_list([{1, 1}, {2, 1}])},

    Ops = [{3, Op2}, {2, Op3}, {1, Op1}],
    {PNCounter2, 3, CommitTime2, _Keep} = filter_oplist(
                                      [], 0, 3, ignore,
                                      vectorclock:from_list([{2, 2}, {1, 2}]),
                                      Ops, ignore, false, 0),
    PNCounter3 = apply_operations(Type, PNCounter, PNCounter2),
    ?assertEqual({4, vectorclock:from_list([{1, 2}, {2, 2}])}, {Type:value(PNCounter3), CommitTime2}),
    Snapshot = materializer:new(Type),
    SS = #snapshot_get_response{snapshot_time = ignore, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = 0, value = Snapshot}},
    {ok, PNcounter3, 1, CommitTime3, _SsSave1, _} = materialize(Type,
                                   vectorclock:from_list([{1, 2}, {2, 1}]), SS),
    ?assertEqual({3, vectorclock:from_list([{1, 2}, {2, 1}])}, {Type:value(PNcounter3), CommitTime3}),
    {ok, PNcounter4, 2, CommitTime4, _SsSave2, _} = materialize(Type,
                                   vectorclock:from_list([{1, 1}, {2, 2}]), SS),
    ?assertEqual({3, vectorclock:from_list([{1, 1}, {2, 2}])}, {Type:value(PNcounter4), CommitTime4}),
    {ok, PNcounter5, 1, CommitTime5, _SsSave3, _} = materialize(Type,
                                   vectorclock:from_list([{1, 1}, {2, 1}]), SS),
    ?assertEqual({2, vectorclock:from_list([{1, 1}, {2, 1}])}, {Type:value(PNcounter5), CommitTime5}).

%% Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    Type = antidote_crdt_counter_pn,
    PNCounter = materializer:new(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    Ops = [],
    {PNCounter2, 0, ignore, _SsSave} = filter_oplist([], 0, 0, ignore,
                                                    vectorclock:from_list([{1, 1}]),
                                                    Ops, ignore, false, 0),
    PNCounter3 = apply_operations(Type, PNCounter, PNCounter2),
    ?assertEqual(0, Type:value(PNCounter3)).


is_op_in_snapshot_test() ->
    OpCT1 = {dc1, 1},
    OpCT1SS = vectorclock:from_list([OpCT1]),
    ST1 = vectorclock:from_list([{dc1, 2}]),
    ST2 = vectorclock:from_list([{dc1, 0}]),
    ?assertEqual({true, OpCT1SS}, is_op_in_snapshot(OpCT1, OpCT1SS, ST1, ignore, ignore)),
    ?assertEqual({false, ignore}, is_op_in_snapshot(OpCT1, OpCT1SS, ST2, ignore, ignore)),
    ?assertEqual(already_in_snapshot, is_op_in_snapshot(OpCT1, OpCT1SS, ST2, ST1, ignore)).
-endif.
