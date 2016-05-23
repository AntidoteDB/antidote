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
         materialize/6,
         materialize_eager/3]).

%% @doc Creates an empty CRDT for a given type.
-spec new(type()) -> snapshot().
new(Type) ->
    materializer:create_snapshot(Type).


%% @doc Applies the operation of a list to a previously created CRDT snapshot. Only the
%%      operations that are not already in the previous snapshot and
%%      with smaller timestamp than the specified
%%      are considered. Newer operations are discarded.
%%      Input:
%%      Type: The type of CRDT to create
%%      Snapshot: Current state of the CRDT, where ops should be applied
%%      SnapshotCommitParams: The commit parameters (as defined by the trasactional protocol) of the Snapshot
%%      SnapshotCommitTime: The time used to describe the state of the CRDT given in Snapshot
%%      Transaction: the metadata of the transaction, dependent on the trasactional protocol
%%      Ops: The list of operations to apply in causal order
%%      Output: A tuple. The first element is ok, the seond is the CRDT after appliying the operations,
%%      the third element 1 minus the number of the operation with the smallest id not included in the snapshot,
%%      the fourth element is the smallest vectorclock that describes this snapshot,
%%      the fifth element is a boolean, it it is true it means that the returned snapshot contains
%%      more operations than the one given as input, false otherwise.
-spec materialize(type(),
  snapshot(),
  integer(),
  snapshot_time() | ignore | {snapshot_time(),snapshot_time()},
  transaction(),
  [{integer(), clocksi_payload()}]) ->
    {ok, snapshot(), integer(), snapshot_time() | ignore, boolean()} | {error, reason()}.
materialize(Type, Snapshot, IncludeFromTime, SnapshotCommitParams, Transaction, Ops) ->
    FirstId = case Ops of
                  [] ->
                      0;
                  [{Id, _Op} | _] ->
                      Id
              end,
    {ok, OpList, NewLastOp, LastOpCt, IsNewSS} =
        materialize_intern(Type, [], IncludeFromTime, FirstId, SnapshotCommitParams, Transaction,
            Ops, SnapshotCommitParams, false),
    lager:info("going to apply operations: ~p~n",[OpList]),
    lager:info("to the snapshot: ~p~n",[Snapshot]),
    case apply_operations(Type, Snapshot, OpList) of
        {ok, NewSS} ->
            {ok, NewSS, NewLastOp, LastOpCt, IsNewSS};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Applies a list of operations to a snapshot
%%      Input:
%%      Type: The type of CRDT of the snapshot
%%      Snapshot: The initial snapshot to apply the operations to
%%      OpList: The list of operations to apply
%%      Output: Either the snapshot with the operations applied to
%%      it, or an error.
-spec apply_operations(type(), snapshot(), [clocksi_payload()]) -> {ok, snapshot()} | {error, reason()}.
apply_operations(_Type,Snapshot,[]) ->
    {ok, Snapshot};
apply_operations(Type,Snapshot,[Op | Rest]) ->
    case materializer:update_snapshot(Type, Snapshot, Op#operation_payload.op_param) of
	{ok, NewSnapshot} -> 
	    apply_operations(Type, NewSnapshot, Rest);
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
%%      Ops: The list of operations to apply in causal order
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
             snapshot_time() | {snapshot_time(), snapshot_time()} | ignore,
             transaction(),
			 [{integer(),clocksi_payload()}],
			 snapshot_time() | ignore,
			 boolean()) ->
				{ok,[clocksi_payload()],integer(),snapshot_time()|ignore,boolean()}.
materialize_intern(_Type, OpList, _IncludeFromTime, FirstHole, _SnapshotCommitParams, _Transaction, [], LastOpCommitParams, NewSS) ->
    {ok, OpList, FirstHole, LastOpCommitParams, NewSS};

materialize_intern(Type, OpList, IncludeFromTime, FirstHole, SnapshotCommitParams,
  Transaction, [{OpId, Op} | Rest], LastOpCommitParams, NewSS) ->
%%    This first phase obtains, in OpsToApply, from the list of operations, the sublist of the ones
%%    not already included in the Snapshot
    OpsToApply = case Type == Op#operation_payload.type of
                 true ->
                     OpCT = Op#operation_payload.dc_and_commit_time,
                     {OpBaseSnapshot, LatestSnapshotCommitParams} = case Transaction#transaction.transactional_protocol of
                                          Protocol when ((Protocol == gr) or (Protocol == clocksi)) ->
                                              {Op#operation_payload.snapshot_vc, SnapshotCommitParams};
                                          nmsi ->
                                              {CommitVC, _DepVC, _ReadTime} = SnapshotCommitParams,
                                              {Op#operation_payload.dependency_vc, CommitVC}
                                      end,
                     %% Check if the op is not in the previous snapshot and should be included in the new one
                     %% {should_be_included, is already_in_snapshot}
                     case (is_op_in_snapshot(Op, OpCT, OpBaseSnapshot, Transaction, LatestSnapshotCommitParams, LastOpCommitParams)) of
                         {true, _, NewOpCt} ->
                             %% Include the new op because it has a timestamp bigger than the snapshot being generated
                             {ok, [Op | OpList], NewOpCt, false, true, FirstHole};
                         {false, false, _} ->
                             %% Dont include the op
                             {ok, OpList, LastOpCommitParams, false, NewSS, OpId - 1}; % no update
                         {false, true, _} ->
                             %% Dont Include the op, because it was already in the SS
                             {ok, OpList, LastOpCommitParams, true, NewSS, FirstHole}
                     end;
                 false -> %% Op is not for this {Key, Type}
                     %% @todo THIS CASE PROBABLY SHOULD NOT HAPPEN?!
                     {ok, OpList, LastOpCommitParams, false, NewSS, FirstHole} %% no update
             end,
%%    Now we have the ops to apply, call the materializer to do so.
    case OpsToApply of
        {ok, NewOpList1, NewLastOpCt, false, NewSS1, NewHole} ->
            materialize_intern(Type, NewOpList1, IncludeFromTime, NewHole, SnapshotCommitParams,
                Transaction, Rest, NewLastOpCt, NewSS1);
        {ok, NewOpList1, NewLastOpCt, true, NewSS1, NewHole} ->
            case OpId - 1 =< IncludeFromTime of
                true ->
                    %% can skip the rest of the ops because they are already included in the SS
                    materialize_intern(Type, NewOpList1, IncludeFromTime, NewHole, SnapshotCommitParams,
                        Transaction, [], NewLastOpCt, NewSS1);
                false ->
                    materialize_intern(Type, NewOpList1, IncludeFromTime, NewHole, SnapshotCommitParams,
                        Transaction, Rest, NewLastOpCt, NewSS1)
            end
    end.

%% @doc Check whether an udpate is included in a snapshot and also
%%		if that update is newer than a snapshot's commit time
%%      Input:
%%      TxId: Descriptor of the transaction requesting the snapshot
%%      Op: The operation to check
%%      {OpDc, OpCommitTime}: The DC and commit time of the operation
%%      OperationSnapshotTime: The snapshot time of the operation
%%      SnapshotTime: The snapshot time to check if the operation is included in
%%	LastSnapshot: The previous snapshot that is being used to generate the new snapshot
%%      PrevTime: The snapshot time of the previous operation that was checked
%%      Outptut: A tuple of 3 elements.  The first element is a boolean that is true
%%      if the operation should be included in the snapshot false otherwise, the second element
%%      is a boolean that is true if the operation was already included in the previous snapshot,
%%      false otherwise.  The thrid element is the snapshot time of the last operation to
%%      be applied to the snapshot
-spec is_op_in_snapshot(clocksi_payload(), commit_time(), snapshot_time(), transaction(),
  snapshot_time() | ignore, snapshot_time()) -> {boolean(), boolean(), snapshot_time()}.
is_op_in_snapshot(Op, OpCT, OpBaseSnapshot, Transaction, LastSnapshotCommitParams, PrevTime) ->
    %% First check if the op was already included in the previous snapshot
    %% Is the "or TxId ==" part necessary and correct????
    {OpDc, OpCommitTime} = OpCT,
    OpCommitVC = vectorclock:create_commit_vector_clock(OpDc, OpCommitTime, OpBaseSnapshot),
    case materializer_vnode:op_not_already_in_snapshot(LastSnapshotCommitParams, OpCommitVC) or
        (Transaction#transaction.txn_id == Op#operation_payload.txid) of
        true ->
            %% If not, check if it should be included in the new snapshot
            %% Replace the snapshot time of the dc where the transaction committed with the commit time
            %% PrevTime2 is the time of the previous snapshot, if there was none, it usues the snapshot time
            %% of the new operation
            PrevTime2 = case PrevTime of
                            ignore ->
                                OpCommitVC;
                            empty ->
                                OpCommitVC;
                            _ ->
                                PrevTime
                        end,
            %% IncludeInSnapshot is true if the op should be included in the snapshot
            %% NewTime is the updated vectorclock of the snapshot that includes Op
            {IncludeInSnapshot, NewTime} =
                dict:fold(fun(DcIdOp, TimeOp, {Acc, PrevTime3}) ->
                    Res1 = case compat(OpCommitVC, OpBaseSnapshot, Transaction) of
                               false ->
                                   false;
                               true ->
                                   Acc
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
                          end, {true, PrevTime2}, OpCommitVC),
            case IncludeInSnapshot of
                true ->
                    {true, false, NewTime};
                false ->
                    {false, false, PrevTime}
            end;
        false ->
            %% was already in the prev ss, done searching ops
            {false, true, PrevTime}
    end.

%% @doc Returns true if an operation defined by its commit vectorclock and
%%      its base snapshot is compatible with a transaction's snapshot, as
%%      defined by the metadata encoded in the Transaction record.
compat(OpCommitVC, _OpBaseSnapshot, Transaction) ->
%%    case Transaction#transaction.transactional_protocol of
%%        nmsi ->
%%            DepUpbound = Transaction#transaction.nmsi_read_metadata#nmsi_read_metadata.dep_upbound,
%%            CommitTimeLowbound = Transaction#transaction.nmsi_read_metadata#nmsi_read_metadata.commit_time_lowbound,
%%            vector_orddict:is_causally_compatible(
%%                OpCommitVC, CommitTimeLowbound, OpBaseSnapshot, DepUpbound);
%%        Protocol  when ((Protocol == clocksi) or (Protocol == gr)) ->
            SnapshotTime = Transaction#transaction.snapshot_vc,
            vectorclock:le(OpCommitVC,SnapshotTime).
%%        Other ->
%%            {error, {unknown_transactional_protocol, Other}}
%%    end.

%% @doc Apply updates in given order without any checks.
%%    Careful: In contrast to materialize/6, it takes just operations, not clocksi_payloads!
-spec materialize_eager(type(), snapshot(), [op()]) -> snapshot().
materialize_eager(Type, Snapshot, Ops) ->
    materializer:materialize_eager(Type, Snapshot, Ops).


-ifdef(TEST).

materializer_clocksi_test()->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    %%  need to add the snapshot time for these for the test to pass
    Op1 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,2},1}},
                           dc_and_commit_time = {1, 1}, txid = 1, snapshot_vc =vectorclock:from_list([{1,1}])},
    Op2 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,1},1}},
                           dc_and_commit_time = {1, 2}, txid = 2, snapshot_vc =vectorclock:from_list([{1,2}])},
    Op3 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,1},1}},
                           dc_and_commit_time = {1, 3}, txid = 3, snapshot_vc =vectorclock:from_list([{1,3}])},
    Op4 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,2},1}},
                           dc_and_commit_time = {1, 4}, txid = 4, snapshot_vc =vectorclock:from_list([{1,4}])},

    Ops = [{4,Op4},{3,Op3},{2,Op2},{1,Op1}],
%%    materialize(Type, Snapshot, IncludeFromTime, SnapshotCommitParams, Transaction, Ops)
%%    materialize(Type, Snapshot, LastOp, SnapshotCommitTime, MinSnapshotTime, Ops, TxId) ->
    {ok, PNCounter2, 3, CommitTime2, _SsSave} = materialize(crdt_pncounter,
						PNCounter, 0, ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{1, 3}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({4, vectorclock:from_list([{1,3}])}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    {ok, PNcounter3, 4, CommitTime3, _SsSave1} = materialize(crdt_pncounter, PNCounter, 0, ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{1, 4}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({6, vectorclock:from_list([{1,4}])}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    {ok, PNcounter4, 4,CommitTime4, _SsSave2} = materialize(crdt_pncounter, PNCounter, 0, ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{1, 7}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({6, vectorclock:from_list([{1,4}])}, {crdt_pncounter:value(PNcounter4), CommitTime4}).

%% This test tests when a a snapshot is generated that does not include all of the updates in the
%% list of operations, precisely in the case where an operation is not taken, but the operations to
%% the left and right of it in the list are taken.  When this snapshot is then used for a future
%% read with a different timestamp, this missing value must be checked.
materializer_missing_op_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Op1 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           dc_and_commit_time = {1, 1}, txid = 1, snapshot_vc =vectorclock:from_list([{1,1},{2,1}])},
    Op2 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor2}},
                           dc_and_commit_time = {1, 2}, txid = 2, snapshot_vc =vectorclock:from_list([{1,2},{2,1}])},
    Op3 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor3}},
                           dc_and_commit_time = {2, 2}, txid = 3, snapshot_vc =vectorclock:from_list([{1,1},{2,1}])},
    Op4 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor4}},
                           dc_and_commit_time = {1, 3}, txid = 2, snapshot_vc =vectorclock:from_list([{1,2},{2,1}])},
    Ops = [{4,Op4},{3,Op3},{2,Op2},{1,Op1}],
    {ok, PNCounter2, LastOp, CommitTime2, _SsSave} = materialize(crdt_pncounter,
							    PNCounter, 0, ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{1,3},{2,1}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({3, vectorclock:from_list([{1,3},{2,1}])}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    {ok, PNCounter3, 4, CommitTime3, _SsSave} = materialize(crdt_pncounter,
							    PNCounter2, LastOp, CommitTime2,
        #transaction{snapshot_vc = vectorclock:from_list([{1,3},{2,2}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({4, vectorclock:from_list([{1,3},{2,2}])}, {crdt_pncounter:value(PNCounter3), CommitTime3}).

%% This test tests the case when there are updates that only snapshots that contain entries from one of the DCs.
%% This can happen for example if an update is commited before the DCs have been connected.
%% It ensures that when we read using a snapshot with and without all the DCs we still include the correct updates.
materializer_missing_dc_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Op1 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           dc_and_commit_time = {1, 1}, txid = 1, snapshot_vc =vectorclock:from_list([{1,1}])},
    Op2 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor2}},
                           dc_and_commit_time = {1, 2}, txid = 2, snapshot_vc =vectorclock:from_list([{1,2}])},
    Op3 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor3}},
                           dc_and_commit_time = {2, 2}, txid = 3, snapshot_vc =vectorclock:from_list([{2,1}])},
    Op4 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor4}},
                           dc_and_commit_time = {1, 3}, txid = 2, snapshot_vc =vectorclock:from_list([{1,2}])},
    Ops = [{4,Op4},{3,Op3},{2,Op2},{1,Op1}],
    
    {ok, PNCounterA, LastOpA, CommitTimeA, _SsSave} = materialize(crdt_pncounter,
								  PNCounter, 0, ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{1,3}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({3, vectorclock:from_list([{1,3}])}, {crdt_pncounter:value(PNCounterA), CommitTimeA}),
    {ok, PNCounterB, 4, CommitTimeB, _SsSave} = materialize(crdt_pncounter,
							    PNCounterA, LastOpA, CommitTimeA,
        #transaction{snapshot_vc = vectorclock:from_list([{1,3},{2,2}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({4, vectorclock:from_list([{1,3},{2,2}])}, {crdt_pncounter:value(PNCounterB), CommitTimeB}),
    
    {ok, PNCounter2, LastOp, CommitTime2, _SsSave} = materialize(crdt_pncounter,
								 PNCounter, 0, ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{1,3},{2,1}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({3, vectorclock:from_list([{1,3}])}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    {ok, PNCounter3, 4, CommitTime3, _SsSave} = materialize(crdt_pncounter,
							    PNCounter2, LastOp, CommitTime2,
        #transaction{snapshot_vc = vectorclock:from_list([{1,3},{2,2}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({4, vectorclock:from_list([{1,3},{2,2}])}, {crdt_pncounter:value(PNCounter3), CommitTime3}).
    
materializer_clocksi_concurrent_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Op1 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,2}, actor1}},
                           dc_and_commit_time = {1, 1}, txid = 1, snapshot_vc =vectorclock:from_list([{1,1},{2,1}])},
    Op2 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           dc_and_commit_time = {1, 2}, txid = 2, snapshot_vc =vectorclock:from_list([{1,2},{2,1}])},
    Op3 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           dc_and_commit_time = {2, 2}, txid = 3, snapshot_vc =vectorclock:from_list([{1,1},{2,1}])},

    Ops = [{3,Op2},{2,Op3},{1,Op1}],
    {ok, PNCounter2, 3, CommitTime2, _Keep} = materialize_intern(crdt_pncounter,
                                      [], 0, 3, ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{2,2},{1,2}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops, ignore, false),
    {ok, PNCounter3} = apply_operations(crdt_pncounter, PNCounter, PNCounter2),
    ?assertEqual({4, vectorclock:from_list([{1,2},{2,2}])}, {crdt_pncounter:value(PNCounter3), CommitTime2}),
    
    Snapshot=new(crdt_pncounter),
    {ok, PNcounter3, 1, CommitTime3, _SsSave1} = materialize(crdt_pncounter, Snapshot, 0, ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{1,2},{2,1}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({3, vectorclock:from_list([{1,2},{2,1}])}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    
    {ok, PNcounter4, 2, CommitTime4, _SsSave2} = materialize(crdt_pncounter, Snapshot, 0, ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{1,1},{2,2}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({3, vectorclock:from_list([{1,1},{2,2}])}, {crdt_pncounter:value(PNcounter4), CommitTime4}),
    
    {ok, PNcounter5, 1, CommitTime5, _SsSave3} = materialize(crdt_pncounter, Snapshot, 0, ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{1,1},{2,1}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops),
    ?assertEqual({2, vectorclock:from_list([{1,1},{2,1}])}, {crdt_pncounter:value(PNcounter5), CommitTime5}).

%% @doc Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Ops = [],
    {ok, PNCounter2, 0, ignore, _SsSave} = materialize_intern(crdt_pncounter, [], 0, 0,ignore,
        #transaction{snapshot_vc = vectorclock:from_list([{1,1}]),
            txn_id = ignore, transactional_protocol = clocksi}, Ops, ignore, false),
    {ok, PNCounter3} = apply_operations(crdt_pncounter, PNCounter, PNCounter2),
    ?assertEqual(0,crdt_pncounter:value(PNCounter3)).

materializer_eager_clocksi_test()->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    % test - no ops
    PNCounter2 = materialize_eager(crdt_pncounter, PNCounter, []),
    ?assertEqual(0, crdt_pncounter:value(PNCounter2)),
    % test - several ops
    Op1 = {update,{{increment,1},1}},
    Op2 = {update,{{increment,2},1}},
    Op3 = {update,{{increment,3},1}},
    Op4 = {update,{{increment,4},1}},
    Ops = [Op1, Op2, Op3, Op4],  
    PNCounter3 = materialize_eager(crdt_pncounter, PNCounter, Ops),
    ?assertEqual(10, crdt_pncounter:value(PNCounter3)).
   
is_op_in_snapshot_test()->
    Op1 = #operation_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,2}, actor1}},
                           dc_and_commit_time = {dc1, 1}, txid = 1, snapshot_vc =vectorclock:from_list([{dc1,1}])},
    OpCT1 = {dc1, 1},
    OpCT1SS = vectorclock:from_list([OpCT1]),
    ST1 = vectorclock:from_list([{dc1, 2}]),
    ST2 = vectorclock:from_list([{dc1, 0}]),
    Tx1 = #transaction{snapshot_vc = ST1, transactional_protocol = clocksi, txn_id = 2},
    Tx2 = #transaction{snapshot_vc = ST2, transactional_protocol = clocksi, txn_id = 2},
    ?assertEqual({true,false,OpCT1SS}, is_op_in_snapshot(Op1, OpCT1, OpCT1SS, Tx1, ignore,ignore)),
    ?assertEqual({false,false,ignore}, is_op_in_snapshot(Op1,OpCT1, OpCT1SS, Tx2, ignore,ignore)).

-endif.
