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
-module(clocksi_materializer).
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
         materialize/4,
         materialize_eager/3]).

%% @doc Creates an empty CRDT for a given type.
-spec new(type()) -> snapshot().
new(Type) ->
    materializer:create_snapshot(Type).

%% The materializer is given of tuple containing ordered update operations.
%% Each update operation has an id number that is one larger than
%% the previous.  This function takes as input that tuple and returns the id number of the first update
%% operation (i.e. the one with the largest id)
-spec get_first_id([{non_neg_integer(), #clocksi_payload{}}] | tuple()) ->
                          non_neg_integer().
get_first_id([]) ->
    0;
get_first_id([{Id, _Op}|_]) ->
    Id;
get_first_id(Tuple) when is_tuple(Tuple) ->
    {Length, _ListLen} = element(2, Tuple),
    case Length of
        0 ->
            0;
        Length ->
            {Id, _Op} = element(?FIRST_OP+(Length-1), Tuple),
            Id
    end.

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
%%      TxId: The Id of the transaction requesting the snapshot
%%      Output: A tuple. The first element is ok, the seond is the CRDT after appliying the operations,
%%      the third element 1 minus the number of the operation with the smallest id not included in the snapshot,
%%      the fourth element is the smallest vectorclock that describes this snapshot,
%%      the fifth element is a boolean, it it is true it means that the returned snapshot contains
%%      more operations than the one given as input, false otherwise.
%%      the sixth element is an integer the counts the number of operations applied to make the snapshot
-spec materialize(type(),
                  txid() | ignore,
                  snapshot_time() | ignore,
                  #snapshot_get_response{}
                 ) ->
                         {ok, snapshot(), integer(), snapshot_time() | ignore,
                          boolean(), non_neg_integer()} | {error, reason()}.
materialize(Type, TxId, MinSnapshotTime,
            #snapshot_get_response{snapshot_time = SnapshotCommitTime, ops_list = Ops,
                                   materialized_snapshot = #materialized_snapshot{last_op_id = LastOp, value = Snapshot}}) ->
    FirstId = get_first_id(Ops),
    {ok, OpList, NewLastOp, LastOpCt, IsNewSS} =
        materialize_intern(Type, [], LastOp, FirstId, SnapshotCommitTime, MinSnapshotTime,
                           Ops, TxId, SnapshotCommitTime, false, 0),
    case apply_operations(Type, Snapshot, 0, OpList) of
        {ok, NewSS, Count} ->
            {ok, NewSS, NewLastOp, LastOpCt, IsNewSS, Count};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Applies a list of operations to a snapshot
%%      Input:
%%      Type: The type of CRDT of the snapshot
%%      Snapshot: The initial snapshot to apply the operations to
%%      Count: Should be input as 0, this will count the number of ops applied
%%      OpList: The list of operations to apply
%%      Output: Either the snapshot with the operations applied to
%%      it, or an error.
-spec apply_operations(type(), snapshot(), non_neg_integer(), [clocksi_payload()]) ->
                              {ok, snapshot(), non_neg_integer()} | {error, reason()}.
apply_operations(_Type, Snapshot, Count, []) ->
    {ok, Snapshot, Count};
apply_operations(Type, Snapshot, Count, [Op | Rest]) ->
    case materializer:update_snapshot(Type, Snapshot, Op#clocksi_payload.op_param) of
        {ok, NewSnapshot} ->
            apply_operations(Type, NewSnapshot, Count+1, Rest);
        {error, Reason} ->
            {error, Reason}
    end.

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
%%      SnapshotCommitTime: The time used to describe the intitial state of the CRDT given in Snapshot
%%      MinSnapshotTime: The threshold time given by the reading transaction
%%      Ops: The list of operations to apply in causal order, the most recent op is on the left
%%      TxId: The Id of the transaction requesting the snapshot
%%      LastOpCommitTime: The snapshot time of the last operation in the list of operations to apply
%%      NewSS: Boolean that is true if any operations should be applied, fale otherwise.  Should start as false.
%%      Output: A tuple with 4 elements or an error.  The first element of the tuple is the atom ok.
%%      The second element is the list of operations that should be applied to the snapshot.
%%      The third element 1 minus the number of the operation with the smallest id not included in the snapshot.
%%      The fourth element is the snapshot time of the last operation in the list.
%%      The fifth element is a boolean, true if a new snapshot should be generated, false otherwise.
-spec materialize_intern(type(),
                         [clocksi_payload()],
                         integer(),
                         integer(),
                         snapshot_time() | ignore,
                         snapshot_time(),
                         [{integer(), clocksi_payload()}] | tuple(),
                         txid() | ignore,
                         snapshot_time() | ignore,
                         boolean(),
                         non_neg_integer()) ->
                                {ok, [clocksi_payload()], integer(), snapshot_time()|ignore, boolean()}.
materialize_intern(_Type, OpList, _LastOp, FirstHole, _SnapshotCommitTime, _MinSnapshotTime, [], _TxId, LastOpCt, NewSS, _Location) ->
    {ok, OpList, FirstHole, LastOpCt, NewSS};

materialize_intern(Type, OpList, LastOp, FirstHole, SnapshotCommitTime, MinSnapshotTime, [{OpId, Op}|Rest], TxId, LastOpCt, NewSS, Location) ->
    materialize_intern_perform(Type, OpList, LastOp, FirstHole, SnapshotCommitTime, MinSnapshotTime, {OpId, Op}, Rest, TxId, LastOpCt, NewSS, Location + 1);

materialize_intern(Type, OpList, LastOp, FirstHole, SnapshotCommitTime, MinSnapshotTime, TupleOps, TxId, LastOpCt, NewSS, Location) ->
    {Length, _ListLen} = element(2, TupleOps),
    case Length == Location of
        true ->
            {ok, OpList, FirstHole, LastOpCt, NewSS};
        false ->
            materialize_intern_perform(Type, OpList, LastOp, FirstHole, SnapshotCommitTime, MinSnapshotTime,
                                       element((?FIRST_OP+Length-1) - Location, TupleOps), TupleOps, TxId, LastOpCt, NewSS, Location + 1)
    end.

materialize_intern_perform(Type, OpList, LastOp, FirstHole, SnapshotCommitTime, MinSnapshotTime, {OpId, Op}, Rest, TxId, LastOpCt, NewSS, Location) ->
    Result = case Type == Op#clocksi_payload.type of
                 true ->
                     OpCom=Op#clocksi_payload.commit_time,
                     OpSS=Op#clocksi_payload.snapshot_time,
                     %% Check if the op is not in the previous snapshot and should be included in the new one
                     case (is_op_in_snapshot(TxId, Op, OpCom, OpSS, MinSnapshotTime, SnapshotCommitTime, LastOpCt)) of
                         {true, _, NewOpCt} ->
                             %% Include the new op because it has a timestamp bigger than the snapshot being generated
                             {ok, [Op | OpList], NewOpCt, false, true, FirstHole};
                         {false, false, _} ->
                             %% Dont include the op
                             {ok, OpList, LastOpCt, false, NewSS, OpId-1}; % no update
                         {false, true, _} ->
                             %% Dont Include the op, because it was already in the SS
                             {ok, OpList, LastOpCt, true, NewSS, FirstHole}
                     end;
                 false -> %% Op is not for this {Key, Type}
                     %% @todo THIS CASE PROBABLY SHOULD NOT HAPPEN?!
                     {ok, OpList, LastOpCt, false, NewSS, FirstHole} %% no update
             end,
    case Result of
        {ok, NewOpList1, NewLastOpCt, false, NewSS1, NewHole} ->
            materialize_intern(Type, NewOpList1, LastOp, NewHole, SnapshotCommitTime,
                               MinSnapshotTime, Rest, TxId, NewLastOpCt, NewSS1, Location);
        {ok, NewOpList1, NewLastOpCt, true, NewSS1, NewHole} ->
            case OpId - 1 =< LastOp of
                true ->
                    %% can skip the rest of the ops because they are already included in the SS
                    materialize_intern(Type, NewOpList1, LastOp, NewHole, SnapshotCommitTime,
                                       MinSnapshotTime, [], TxId, NewLastOpCt, NewSS1, Location);
                false ->
                    materialize_intern(Type, NewOpList1, LastOp, NewHole, SnapshotCommitTime,
                                       MinSnapshotTime, Rest, TxId, NewLastOpCt, NewSS1, Location)
            end
    end.

%% @doc Check whether an udpate is included in a snapshot and also
%%      if that update is newer than a snapshot's commit time
%%      Input:
%%      TxId: Descriptor of the transaction requesting the snapshot
%%      Op: The operation to check
%%      {OpDc, OpCommitTime}: The DC and commit time of the operation
%%      OperationSnapshotTime: The snapshot time of the operation
%%      SnapshotTime: The snapshot time to check if the operation is included in
%%      LastSnapshot: The previous snapshot that is being used to generate the new snapshot
%%      PrevTime: The snapshot time of the previous operation that was checked
%%      Outptut: A tuple of 3 elements.  The first element is a boolean that is true
%%      if the operation should be included in the snapshot false otherwise, the second element
%%      is a boolean that is true if the operation was already included in the previous snapshot,
%%      false otherwise.  The thrid element is the snapshot time of the last operation to
%%      be applied to the snapshot
-spec is_op_in_snapshot(txid(), clocksi_payload(), dc_and_commit_time(), snapshot_time(), snapshot_time(),
                        snapshot_time() | ignore, snapshot_time()) -> {boolean(), boolean(), snapshot_time()}.
is_op_in_snapshot(TxId, Op, {OpDc, OpCommitTime}, OperationSnapshotTime, SnapshotTime, LastSnapshot, PrevTime) ->
    %% First check if the op was already included in the previous snapshot
    %% Is the "or TxId ==" part necessary and correct????
    case materializer_vnode:belongs_to_snapshot_op(
           LastSnapshot, {OpDc, OpCommitTime}, OperationSnapshotTime) or (TxId == Op#clocksi_payload.txid) of
        true ->
            %% If not, check if it should be included in the new snapshot
            %% Replace the snapshot time of the dc where the transaction committed with the commit time
            OpSSCommit = dict:store(OpDc, OpCommitTime, OperationSnapshotTime),
            %% PrevTime2 is the time of the previous snapshot, if there was none, it usues the snapshot time
            %% of the new operation
            PrevTime2 = case PrevTime of
                            ignore ->
                                OpSSCommit;
                            _ ->
                                PrevTime
                        end,
            %% Result is true if the op should be included in the snapshot
            %% NewTime is the vectorclock of the snapshot with the time of Op included
                {Result, NewTime} =
                dict:fold(fun(DcIdOp, TimeOp, {Acc, PrevTime3}) ->
                                  Res1 = case dict:find(DcIdOp, SnapshotTime) of
                                             {ok, TimeSS} ->
                                                 case TimeSS < TimeOp of
                                                     true ->
                                                         false;
                                                     false ->
                                                         Acc
                                                 end;
                                             error ->
                                                 lager:error("Could not find DC in SS ~p", [SnapshotTime]),
                                                 false
                                         end,
                                  Res2 = dict:update(DcIdOp, fun(Val) ->
                                                                    case TimeOp > Val of
                                                                        true ->
                                                                            TimeOp;
                                                                        false ->
                                                                            Val
                                                                    end
                                                             end, TimeOp, PrevTime3),
                                  {Res1, Res2}
                          end, {true, PrevTime2}, OpSSCommit),
            case Result of
                true ->
                    {true, false, NewTime};
                false ->
                    {false, false, PrevTime}
            end;
        false->
            %% was already in the prev ss, done searching ops
            {false, true, PrevTime}
    end.

%% @doc Apply updates in given order without any checks.
%%    Careful: In contrast to materialize/6, it takes just operations, not clocksi_payloads!
-spec materialize_eager(type(), snapshot(), [op()]) -> snapshot().
materialize_eager(Type, Snapshot, Ops) ->
    materializer:materialize_eager(Type, Snapshot, Ops).


-ifdef(TEST).

materializer_clocksi_test()->
    Type = antidote_crdt_counter_pn,
    PNCounter = new(Type),
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
                                                            ignore, vectorclock:from_list([{1, 3}]),
                                                            SS),
    ?assertEqual({4, vectorclock:from_list([{1, 3}])}, {Type:value(PNCounter2), CommitTime2}),
    {ok, PNcounter3, 4, CommitTime3, _SsSave1, _} = materialize(Type,
                                                             ignore, vectorclock:from_list([{1, 4}]),
                                                             SS),
    ?assertEqual({6, vectorclock:from_list([{1, 4}])}, {Type:value(PNcounter3), CommitTime3}),

    {ok, PNcounter4, 4, CommitTime4, _SsSave2, _} = materialize(Type,
                                                            ignore, vectorclock:from_list([{1, 7}]),
                                                            SS),
    ?assertEqual({6, vectorclock:from_list([{1, 4}])}, {Type:value(PNcounter4), CommitTime4}).

%% This test tests when a a snapshot is generated that does not include all of the updates in the
%% list of operations, precisely in the case where an operation is not taken, but the operations to
%% the left and right of it in the list are taken.  When this snapshot is then used for a future
%% read with a different timestamp, this missing value must be checked.
materializer_missing_op_test() ->
    Type = antidote_crdt_counter_pn,
    PNCounter = new(Type),
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
                                                                 ignore, vectorclock:from_list([{1, 3}, {2, 1}]),
                                                                 SS),
    ?assertEqual({3, vectorclock:from_list([{1, 3}, {2, 1}])}, {Type:value(PNCounter2), CommitTime2}),

    SS2 = #snapshot_get_response{snapshot_time = CommitTime2, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = LastOp, value = PNCounter2}},
    {ok, PNCounter3, 4, CommitTime3, _SsSave, _} = materialize(Type,
                                                            ignore, vectorclock:from_list([{1, 3}, {2, 2}]),
                                                            SS2),
    ?assertEqual({4, vectorclock:from_list([{1, 3}, {2, 2}])}, {Type:value(PNCounter3), CommitTime3}).

%% This test tests the case when there are updates that only snapshots that contain entries from one of the DCs.
%% This can happen for example if an update is commited before the DCs have been connected.
%% It ensures that when we read using a snapshot with and without all the DCs we still include the correct updates.
materializer_missing_dc_test() ->
    Type = antidote_crdt_counter_pn,
    PNCounter = new(Type),
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
    {ok, PNCounterA, LastOpA, CommitTimeA, _SsSave, _} = materialize(Type,
                                                                  ignore, vectorclock:from_list([{1, 3}]),
                                                                  SS),
    ?assertEqual({3, vectorclock:from_list([{1, 3}])}, {Type:value(PNCounterA), CommitTimeA}),

    SS2 = #snapshot_get_response{snapshot_time = CommitTimeA, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = LastOpA, value = PNCounterA}},
    {ok, PNCounterB, 4, CommitTimeB, _SsSave, _} = materialize(Type,
                                                            ignore, vectorclock:from_list([{1, 3}, {2, 2}]),
                                                            SS2),
    ?assertEqual({4, vectorclock:from_list([{1, 3}, {2, 2}])}, {Type:value(PNCounterB), CommitTimeB}),

    {ok, PNCounter2, LastOp, CommitTime2, _SsSave, _} = materialize(Type,
                                                                 ignore, vectorclock:from_list([{1, 3}, {2, 1}]),
                                                                 SS),
    ?assertEqual({3, vectorclock:from_list([{1, 3}])}, {Type:value(PNCounter2), CommitTime2}),

    SS3 = #snapshot_get_response{snapshot_time = CommitTime2, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = LastOp, value = PNCounter2}},
    {ok, PNCounter3, 4, CommitTime3, _SsSave, _} = materialize(Type,
                                                            ignore, vectorclock:from_list([{1, 3}, {2, 2}]),
                                                            SS3),
    ?assertEqual({4, vectorclock:from_list([{1, 3}, {2, 2}])}, {Type:value(PNCounter3), CommitTime3}).

materializer_clocksi_concurrent_test() ->
    Type = antidote_crdt_counter_pn,
    PNCounter = new(Type),
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
    {ok, PNCounter2, 3, CommitTime2, _Keep} = materialize_intern(Type,
                                      [], 0, 3, ignore,
                                      vectorclock:from_list([{2, 2}, {1, 2}]),
                                      Ops, ignore, ignore, false, 0),
    {ok, PNCounter3, _} = apply_operations(Type, PNCounter, 0, PNCounter2),
    ?assertEqual({4, vectorclock:from_list([{1, 2}, {2, 2}])}, {Type:value(PNCounter3), CommitTime2}),
    Snapshot=new(Type),
    SS = #snapshot_get_response{snapshot_time = ignore, ops_list = Ops,
                                materialized_snapshot = #materialized_snapshot{last_op_id = 0, value = Snapshot}},
    {ok, PNcounter3, 1, CommitTime3, _SsSave1, _} = materialize(Type, ignore,
                                   vectorclock:from_list([{1, 2}, {2, 1}]), SS),
    ?assertEqual({3, vectorclock:from_list([{1, 2}, {2, 1}])}, {Type:value(PNcounter3), CommitTime3}),
    {ok, PNcounter4, 2, CommitTime4, _SsSave2, _} = materialize(Type, ignore,
                                   vectorclock:from_list([{1, 1}, {2, 2}]), SS),
    ?assertEqual({3, vectorclock:from_list([{1, 1}, {2, 2}])}, {Type:value(PNcounter4), CommitTime4}),
    {ok, PNcounter5, 1, CommitTime5, _SsSave3, _} = materialize(Type, ignore,
                                   vectorclock:from_list([{1, 1}, {2, 1}]), SS),
    ?assertEqual({2, vectorclock:from_list([{1, 1}, {2, 1}])}, {Type:value(PNcounter5), CommitTime5}).

%% Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    Type = antidote_crdt_counter_pn,
    PNCounter = new(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    Ops = [],
    {ok, PNCounter2, 0, ignore, _SsSave} = materialize_intern(Type, [], 0, 0, ignore,
                                                    vectorclock:from_list([{1, 1}]),
                                                    Ops, ignore, ignore, false, 0),
    {ok, PNCounter3, _} = apply_operations(Type, PNCounter, 0, PNCounter2),
    ?assertEqual(0, Type:value(PNCounter3)).

materializer_eager_clocksi_test()->
    Type = antidote_crdt_counter_pn,
    PNCounter = new(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    % test - no ops
    PNCounter2 = materialize_eager(Type, PNCounter, []),
    ?assertEqual(0, Type:value(PNCounter2)),
    % test - several ops
    Op1 = 1,
    Op2 = 2,
    Op3 = 3,
    Op4 = 4,
    Ops = [Op1, Op2, Op3, Op4],
    PNCounter3 = materialize_eager(Type, PNCounter, Ops),
    ?assertEqual(10, Type:value(PNCounter3)).

is_op_in_snapshot_test() ->
    Type = antidote_crdt_counter_pn,
    Op1 = #clocksi_payload{key = abc, type = Type,
                           op_param = {increment, 2},
                           commit_time = {dc1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{dc1, 1}])},
    OpCT1 = {dc1, 1},
    OpCT1SS = vectorclock:from_list([OpCT1]),
    ST1 = vectorclock:from_list([{dc1, 2}]),
    ST2 = vectorclock:from_list([{dc1, 0}]),
    ?assertEqual({true, false, OpCT1SS}, is_op_in_snapshot(2, Op1, OpCT1, OpCT1SS, ST1, ignore, ignore)),
    ?assertEqual({false, false, ignore}, is_op_in_snapshot(2, Op1, OpCT1, OpCT1SS, ST2, ignore, ignore)).


-endif.
