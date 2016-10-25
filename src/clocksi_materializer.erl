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
         materialize/3,
		 compat/2,
         materialize_eager/3]).

%% @doc Creates an empty CRDT for a given type.
-spec new(type()) -> snapshot().
new(Type) ->
    materializer:create_snapshot(Type).

%% The materializer is given of tuple containing ordered update operations.
%% Each update operation has an id number that is one larger than
%% the previous.  This function takes as input that tuple and returns the id number of the first update
%% operation (i.e. the one with the largest id)
-spec get_first_id([{non_neg_integer(),#operation_payload{}}] | tuple()) ->
			  non_neg_integer().
get_first_id([]) ->
    0;
get_first_id([{Id,_Op}|_]) ->
    Id;
get_first_id(Tuple) when is_tuple(Tuple) ->
    {Length,_ListLen} = element(2,Tuple),
    case Length of
	0 ->
	    0;
	Length ->
	    {Id,_Op} = element(?FIRST_OP+(Length-1),Tuple),
	    Id
    end.

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
%%      the sixth element is an integer the counts the number of operations applied to make the snapshot
-spec materialize(type(),
		  transaction() | ignore,
		  #snapshot_get_response{}
		 ) ->
			 {ok, snapshot(), integer(), snapshot_time() | ignore,
			  boolean(), non_neg_integer()} | {error, reason()}.
materialize(Type, Transaction, #snapshot_get_response{
	commit_parameters=ProtocolIndependentSnapshotCommitParams,
	ops_list=Ops,
	materialized_snapshot=#materialized_snapshot{last_op_id=LastOp, value=Snapshot}})->
	FirstId=get_first_id(Ops),
	PrevOpCommitParams=case ProtocolIndependentSnapshotCommitParams of
		{CommitVC, DepVC, _ReadTime} ->
			%% the following is used for setting the read time of a snapshot.
			%% Initially, we start with the current clock time of the vnode, that will be used
			%% in the case the
			NowVC = vectorclock:set_clock_of_dc(dc_utilities:get_my_dc_id(), dc_utilities:now_microsec(), vectorclock:new()),
			{CommitVC, DepVC, NowVC};
		_->ProtocolIndependentSnapshotCommitParams
	end,
	{ok, OpList, NewLastOp, LastOpCt, IsNewSS}=
		materialize_intern(Type, [], LastOp, FirstId, ProtocolIndependentSnapshotCommitParams, Transaction,
			Ops, PrevOpCommitParams, false, 0, 0),
	case apply_operations(Type, Snapshot, 0, OpList) of
		{ok, NewSS, Count}->
			{ok, NewSS, NewLastOp, LastOpCt, IsNewSS, Count};
		{error, Reason}->
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
-spec apply_operations(type(), snapshot(), non_neg_integer(), [operation_payload()]) ->
			      {ok, snapshot(), non_neg_integer()} | {error, reason()}.
apply_operations(_Type,Snapshot,Count,[]) ->
    {ok, Snapshot, Count};
apply_operations(Type,Snapshot,Count,[Op | Rest]) ->
    case materializer:update_snapshot(Type, Snapshot, Op#operation_payload.op_param) of
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
%%      Transaction: tx record that contains the threshold time given by the reading transaction
%%      Ops: The list of operations to apply in causal order
%%      LastOpCommitTime: The snapshot time of the last operation in the list of operations to apply
%%      NewSS: Boolean that is true if any operations should be applied, fale otherwise.  Should start as false.
%%      Output: A tuple with 4 elements or an error.  The first element of the tuple is the atom ok.
%%      The second element is the list of operations that should be applied to the snapshot.
%%      The third element 1 minus the number of the operation with the smallest id not included in the snapshot.
%%      The fourth element is the snapshot time of the last operation in the list.
%%      The fifth element is a boolean, true if a new snapshot should be generated, false otherwise.
-spec materialize_intern(type(),
			 [operation_payload()],
			 integer(),
			 integer(),
             snapshot_time() | {snapshot_time(), snapshot_time()} | ignore,
			 transaction(),
			 [{integer(),operation_payload()}] | tuple(),
			 snapshot_time() | ignore,
			 boolean(),
			 non_neg_integer(), non_neg_integer()) ->
				{ok,[operation_payload()],integer(),snapshot_time()|ignore,boolean()}.

materialize_intern(_Type, OutputOpList, _LastOp, FirstHole, _SnapshotCommitParams, _Transaction, [], LastOpCt, NewSS, _Location, NumberOfNonAppliedOps) ->
	ok = log_number_of_non_applied_ops(NumberOfNonAppliedOps),
    {ok, OutputOpList, FirstHole, LastOpCt, NewSS};

materialize_intern(Type, OpList, OutputOpList, FirstHole, SnapshotCommitParams, Transaction, [{OpId,Op}|Rest], LastOpCt, NewSS, Location, NumberOfNonAppliedOps) ->
    materialize_intern_perform(Type, OpList, OutputOpList, FirstHole, SnapshotCommitParams, Transaction, {OpId,Op}, Rest, LastOpCt, NewSS, Location + 1, NumberOfNonAppliedOps);

materialize_intern(Type, OpList, FirstNotIncludedOperationId, OutputFirstNotIncludedOperationId,
                    InitSnapshotCommitParams, Transaction, TupleOps, OutputSnapshotCommitParams, DidGenerateNewSnapshot, Location, NumberOfNonAppliedOps) ->
    {Length,_ListLen} = element(2, TupleOps),
    case Length == Location of
	true ->
	    {ok, OpList, OutputFirstNotIncludedOperationId, OutputSnapshotCommitParams, DidGenerateNewSnapshot};
	false ->
	    materialize_intern_perform(Type, OpList, FirstNotIncludedOperationId, OutputFirstNotIncludedOperationId, InitSnapshotCommitParams, Transaction,
				       element((?FIRST_OP+Length-1) - Location, TupleOps), TupleOps, OutputSnapshotCommitParams, DidGenerateNewSnapshot, Location + 1, NumberOfNonAppliedOps)
    end.

-spec materialize_intern_perform(type(),
  [operation_payload()],
  integer(),
  integer(),
  snapshot_time() | {snapshot_time(), snapshot_time()} | ignore,
  transaction(),
  {integer(),operation_payload()},
  [{integer(),operation_payload()}] | tuple(),
  snapshot_time() | ignore,
  boolean(),
  non_neg_integer(),
  non_neg_integer()) ->
	{ok,[operation_payload()],integer(),snapshot_time()|ignore,boolean()}.
materialize_intern_perform(Type, OpList, FirstNotIncludedOperationId, FirstHole, InitSnapshotCommitParams, Transaction,
                            {OpId,Op}, Rest, OutputSnapshotCommitParams, DidGenerateNewSnapshot, PositionInOpList, NumberOfNonAppliedOps) ->
    Result = case Type == Op#operation_payload.type of
		 true ->
			 case is_record(Transaction, transaction) of
				 true -> ok;
				 false ->
					 lager:error("BAD TRANSACTION RECORD: ~p", [Transaction])
			 end,
		     OpDCandCT=Op#operation_payload.dc_and_commit_time,
			 OpSnapshotVC= Op#operation_payload.dependency_vc,
		     %% Check if the op is not in the previous snapshot and should be included in the new one
%%			 {SnapshotCommitVC, SnapshotDependencyVC} =
		     case (is_op_in_snapshot(Op, OpDCandCT, OpSnapshotVC, Transaction, InitSnapshotCommitParams, OutputSnapshotCommitParams)) of
			 {true,_,NewOpCt} ->
			     %% Include the new op because it has a timestamp bigger than the snapshot being generated
			     {ok, [Op | OpList], NewOpCt, false, true, FirstHole, NumberOfNonAppliedOps};
			 {false,false,_} ->
			     %% Dont include the op
			     {ok, OpList, OutputSnapshotCommitParams, false, DidGenerateNewSnapshot, OpId-1, NumberOfNonAppliedOps+1}; % no update
			 {false,true,_} ->
			     %% Dont Include the op, because it was already in the SS
			     {ok, OpList, OutputSnapshotCommitParams, true, DidGenerateNewSnapshot, FirstHole, NumberOfNonAppliedOps}
		     end;
		 false -> %% Op is not for this {Key, Type}
		     %% @todo THIS CASE PROBABLY SHOULD NOT HAPPEN?!
		     {ok, OpList, OutputSnapshotCommitParams, false, DidGenerateNewSnapshot, FirstHole, NumberOfNonAppliedOps} %% no update
	     end,
    case Result of
	{ok, NewOpList1, NewLastOpCt, false, NewSS1, NewHole, NewNumberOfNonAppliedOps} ->
	    materialize_intern(Type,NewOpList1, FirstNotIncludedOperationId,NewHole, InitSnapshotCommitParams,
		    Transaction,Rest,NewLastOpCt,NewSS1, PositionInOpList, NewNumberOfNonAppliedOps);
	{ok, NewOpList1, NewLastOpCt, true, NewSS1, NewHole, NewNumberOfNonAppliedOps} ->
	    case OpId - 1 =<FirstNotIncludedOperationId of
		true ->
		    %% can skip the rest of the ops because they are already included in the SS
		    materialize_intern(Type,NewOpList1, FirstNotIncludedOperationId,NewHole, InitSnapshotCommitParams,
			    Transaction,[],NewLastOpCt,NewSS1, PositionInOpList, NewNumberOfNonAppliedOps);
		false ->
		    materialize_intern(Type,NewOpList1, FirstNotIncludedOperationId,NewHole, InitSnapshotCommitParams,
			    Transaction,Rest,NewLastOpCt,NewSS1, PositionInOpList, NewNumberOfNonAppliedOps)
	    end
    end.

%% @doc This function is used only for benchmark purposes.
%% It logs on disk the number of snapshot and the number of operations
%% that it had to leave out to build the transaction's snapshot.
%% this is used to measure staleness.
log_number_of_non_applied_ops(_NumberOfNonAppliedOps) ->
	ok.

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
-spec is_op_in_snapshot(operation_payload(), dc_and_commit_time(), snapshot_time(), transaction(),
			snapshot_time() | ignore, snapshot_time()) -> {boolean(),boolean(),snapshot_time()}.
is_op_in_snapshot(Op, {OpDC, OpCT}, OpDependencyVC, Transaction=#transaction{transactional_protocol=physics}, {SnapshotCT, _SnapshotDep, _SnapshotRT}, PrevIncludedOpParams)->
	TxId = Transaction#transaction.txn_id,
	OpCommitVC = vectorclock:set_clock_of_dc(OpDC, OpCT, OpDependencyVC),
	{PrevOpCT, PrevOpDep, PrevOpRT} = PrevIncludedOpParams,
	%% first, check if the op is not already included in the object snapshot.
%%	lager:info("IS OP IN SNAPSHOT CALLED WITH ~n
%%	Op ~p ~n, {OpDC, OpCT}  ~p ~n, OpDependencyVC  ~p ~n, Transaction  ~p ~n  SnapshotCT  ~p ~n PrevIncludedOpParams ~p ~n ~p~n ~p ~n",
%%		[Op, {OpDC, OpCT}, vectorclock:to_list(OpDependencyVC), Transaction, vectorclock:to_list(SnapshotCT), vectorclock:to_list(PrevOpCT), vectorclock:to_list(PrevOpDep), vectorclock:to_list(PrevOpRT)]),
	case materializer_vnode:op_not_already_in_snapshot(SnapshotCT, OpCommitVC) or (TxId == Op#operation_payload.txid) of
		true->
%%			lager:info("~n OP NOT ALREADY IN SNAPSHOT!"),
			%% the operation is newer than the snapshot, might need to include it
			%% if it is compatible with the transaction's snapshot.

			case compat(OpDependencyVC, Transaction) of
				true->
%%					lager:info("~n OP COMPATIBLE, WILL INCLUDE"),
					%% the operation must be included
					%% now update the commit time of our output
					FinalCT=vectorclock:max([PrevOpCT, OpCommitVC]),
					%% now update the dependency time of our output
					FinalDep=vectorclock:max([PrevOpDep, OpDependencyVC]),
					%% the Operation Read time is not updated as we assume
					%% operations to be ordered from newer to older, and
					%% the final read time is given by the newest included op.
%%					lager:info("~n REPLYING ~p", [{true, false, {vectorclock:to_list(FinalCT), vectorclock:to_list(FinalDep), vectorclock:to_list(PrevOpRT)}}]),
					{true, false, {FinalCT, FinalDep, PrevOpRT}};

				false->
%%					lager:info("~n OP INCOMPATIBLE, WILL  NOT NOT NOT INCLUDE"),
					%% the operation was not already in snapshot, but is incompatible (too new).
					%% now update the commit time of our output
					ReadTimeOfNextOp=vectorclock:set_clock_of_dc(OpDC, OpCT-1, OpDependencyVC),
					FinalRT=vectorclock:min([PrevOpRT, ReadTimeOfNextOp]),
%%					lager:info("~n REPLYING ~p", [{false, false, {vectorclock:to_list(PrevOpCT), vectorclock:to_list(PrevOpDep), vectorclock:to_list(FinalRT)}}]),
					{false, false, {PrevOpCT, PrevOpDep, FinalRT}}
			end;
		false->
			%% the operation is older than the snapshot, discard.
%%			lager:info("~n Op too old, already in the snapshot, do nothing."),
			{false, true, {PrevOpCT, PrevOpDep, PrevOpRT}}
	end;

is_op_in_snapshot(Op, {OpDc, OpCommitTime}, OperationSnapshotTime, Transaction, LastSnapshot, PrevTime) ->
    %% First check if the op was already included in the previous snapshot
    %% Is the "or TxId ==" part necessary and correct????
	SnapshotTime = Transaction#transaction.snapshot_vc,
	TxId = Transaction#transaction.txn_id,
	OpSSCommit = vectorclock:set_clock_of_dc(OpDc, OpCommitTime, OperationSnapshotTime),
    case materializer_vnode:op_not_already_in_snapshot(
	   LastSnapshot,OpSSCommit) or (TxId == Op#operation_payload.txid) of
	true ->
	    %% If not, check if it should be included in the new snapshot
	    %% Replace the snapshot time of the dc where the transaction committed with the commit time
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
    	    {Result,NewTime} =
		dict:fold(fun(DcIdOp,TimeOp,{Acc,PrevTime3}) ->
				  Res1 = case dict:find(DcIdOp,SnapshotTime) of
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
				  Res2 = dict:update(DcIdOp,fun(Val) ->
								    case TimeOp > Val of
									true ->
									    TimeOp;
									false ->
									    Val
								    end
							    end,TimeOp,PrevTime3),
				  {Res1,Res2}
			  end, {true,PrevTime2}, OpSSCommit),
	    case Result of
		true ->
		    {true,false,NewTime};
		false ->
		    {false,false,PrevTime}
	    end;
	false->
	    %% was already in the prev ss, done searching ops
	    {false,true,PrevTime}
    end.

%% @doc Returns true if an operation defined by its commit vectorclock and
%%      its base snapshot is compatible with a transaction's snapshot, as
%%      defined by the metadata encoded in the Transaction record.
compat(OpDependencyVC, Transaction) ->
			%% we can use the snapshot vc here, as it has been updated by the
	        %% transaction coordinator to fulfill this purpose.
            SnapshotTime = Transaction#transaction.snapshot_vc,
			%% @doc: Physics compares that the dependency VC of the operation is le
			%% or concurrent with the snapshot time, that in this case represents the
			%% dependency upbound of the protocol.
            not(vectorclock:strict_le(SnapshotTime, OpDependencyVC)).

%% @doc Apply updates in given order without any checks.
%%    Careful: In contrast to materialize/6, it takes just operations, not clocksi_payloads!
-spec materialize_eager(type(), snapshot(), [op()]) -> snapshot().
materialize_eager(Type, Snapshot, Ops) ->
    materializer:materialize_eager(Type, Snapshot, Ops).


-ifdef(TEST).

materializer_clocksi_test()->
    Type = antidote_crdt_counter,
    PNCounter = new(Type),
    ?assertEqual(0, Type:value(PNCounter)),
    %%  need to add the snapshot time for these for the test to pass
	Op1 = #operation_payload{key = abc, type = Type,
		op_param = {increment,2},
		dc_and_commit_time = {1, 1}, txid = 1, dependency_vc=vectorclock:from_list([{1,1}])},
	Op2 = #operation_payload{key = abc, type = Type,
		op_param = {increment,1},
		dc_and_commit_time = {1, 2}, txid = 2, dependency_vc=vectorclock:from_list([{1,2}])},
	Op3 = #operation_payload{key = abc, type = Type,
		op_param = {increment,1},
		dc_and_commit_time = {1, 3}, txid = 3, dependency_vc=vectorclock:from_list([{1,3}])},
	Op4 = #operation_payload{key = abc, type = Type,
		op_param = {increment,2},
		dc_and_commit_time = {1, 4}, txid = 4, dependency_vc=vectorclock:from_list([{1,4}])},
	
	Ops = [{4,Op4},{3,Op3},{2,Op2},{1,Op1}],

    SS = #snapshot_get_response{commit_parameters = ignore, ops_list = Ops,
				materialized_snapshot = #materialized_snapshot{last_op_id = 0, value = PNCounter}},
    {ok, PNCounter2, 3, CommitTime2, _SsSave, _} = materialize(Type,
							    #transaction{snapshot_vc = vectorclock:from_list([{1, 3}]),
		    txn_id = ignore, transactional_protocol = clocksi},
							    SS),
    ?assertEqual({4, vectorclock:from_list([{1,3}])}, {Type:value(PNCounter2), CommitTime2}),
    {ok, PNcounter3, 4, CommitTime3, _SsSave1, _} = materialize(Type,
							     #transaction{snapshot_vc = vectorclock:from_list([{1, 4}]),
		    txn_id = ignore, transactional_protocol = clocksi}, SS),
	
	?assertEqual({6, vectorclock:from_list([{1,4}])}, {Type:value(PNcounter3), CommitTime3}),

    {ok, PNcounter4, 4,CommitTime4, _SsSave2, _} = materialize(Type,
							    #transaction{snapshot_vc = vectorclock:from_list([{1, 7}]),
		    txn_id = ignore, transactional_protocol = clocksi},
							    SS),
    ?assertEqual({6, vectorclock:from_list([{1,4}])}, {Type:value(PNcounter4), CommitTime4}).

%% This test tests when a a snapshot is generated that does not include all of the updates in the
%% list of operations, precisely in the case where an operation is not taken, but the operations to
%% the left and right of it in the list are taken.  When this snapshot is then used for a future
%% read with a different timestamp, this missing value must be checked.
materializer_missing_op_test() ->
    Type = antidote_crdt_counter,
    PNCounter = new(Type),
    ?assertEqual(0,Type:value(PNCounter)),
    Op1 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,1},
                           dc_and_commit_time= {1, 1}, txid = 1, dependency_vc=vectorclock:from_list([{1,1},{2,1}])},
    Op2 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,1},
	    dc_and_commit_time = {1, 2}, txid = 2, dependency_vc=vectorclock:from_list([{1,2},{2,1}])},
    Op3 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,1},
	    dc_and_commit_time = {2, 2}, txid = 3, dependency_vc=vectorclock:from_list([{1,1},{2,1}])},
    Op4 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,1},
	    dc_and_commit_time = {1, 3}, txid = 2, dependency_vc=vectorclock:from_list([{1,2},{2,1}])},
    Ops = [{4,Op4},{3,Op3},{2,Op2},{1,Op1}],

    SS = #snapshot_get_response{commit_parameters = ignore, ops_list = Ops,
				materialized_snapshot = #materialized_snapshot{last_op_id = 0, value = PNCounter}},
    {ok, PNCounter2, LastOp, CommitTime2, _SsSave, _} = materialize(Type,
								 #transaction{snapshot_vc = vectorclock:from_list([{1,3},{2,1}]),
		    txn_id = ignore, transactional_protocol = clocksi},
								 SS),
    ?assertEqual({3, vectorclock:from_list([{1,3},{2,1}])}, {Type:value(PNCounter2), CommitTime2}),

    SS2 = #snapshot_get_response{commit_parameters = CommitTime2, ops_list = Ops,
				materialized_snapshot = #materialized_snapshot{last_op_id = LastOp, value = PNCounter2}},
    {ok, PNCounter3, 4, CommitTime3, _SsSave, _} = materialize(Type,
							    #transaction{snapshot_vc = vectorclock:from_list([{1,3},{2,2}]),
		    txn_id = ignore, transactional_protocol = clocksi},
							    SS2),
    ?assertEqual({4, vectorclock:from_list([{1,3},{2,2}])}, {Type:value(PNCounter3), CommitTime3}).

%% This test tests the case when there are updates that only snapshots that contain entries from one of the DCs.
%% This can happen for example if an update is commited before the DCs have been connected.
%% It ensures that when we read using a snapshot with and without all the DCs we still include the correct updates.
materializer_missing_dc_test() ->
    Type = antidote_crdt_counter,
    PNCounter = new(Type),
    ?assertEqual(0,Type:value(PNCounter)),
    Op1 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,1},
                           dc_and_commit_time= {1, 1}, txid = 1, dependency_vc=vectorclock:from_list([{1,1}])},
    Op2 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,1},
                           dc_and_commit_time = {1, 2}, txid = 2, dependency_vc=vectorclock:from_list([{1,2}])},
    Op3 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,1},
                           dc_and_commit_time = {2, 2}, txid = 3, dependency_vc=vectorclock:from_list([{2,1}])},
    Op4 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,1},
                           dc_and_commit_time = {1, 3}, txid = 2, dependency_vc=vectorclock:from_list([{1,2}])},
    Ops = [{4,Op4},{3,Op3},{2,Op2},{1,Op1}],

    SS = #snapshot_get_response{commit_parameters = ignore, ops_list = Ops,
				materialized_snapshot = #materialized_snapshot{last_op_id = 0, value = PNCounter}},
    {ok, PNCounterA, LastOpA, CommitTimeA, _SsSave, _} = materialize(Type,
								  #transaction{snapshot_vc = vectorclock:from_list([{1,3}]),
txn_id = ignore, transactional_protocol = clocksi},
								  SS),
    ?assertEqual({3, vectorclock:from_list([{1,3}])}, {Type:value(PNCounterA), CommitTimeA}),

    SS2 = #snapshot_get_response{commit_parameters = CommitTimeA, ops_list = Ops,
				materialized_snapshot = #materialized_snapshot{last_op_id = LastOpA, value = PNCounterA}},
    {ok, PNCounterB, 4, CommitTimeB, _SsSave, _} = materialize(Type,
							    #transaction{snapshot_vc = vectorclock:from_list([{1,3},{2,2}]),
txn_id = ignore, transactional_protocol = clocksi},
							    SS2),
    ?assertEqual({4, vectorclock:from_list([{1,3},{2,2}])}, {Type:value(PNCounterB), CommitTimeB}),
    
    {ok, PNCounter2, LastOp, CommitTime2, _SsSave, _} = materialize(Type,
								 #transaction{snapshot_vc = vectorclock:from_list([{1,3},{2,1}]),
txn_id = ignore, transactional_protocol = clocksi},
								 SS),
    ?assertEqual({3, vectorclock:from_list([{1,3}])}, {Type:value(PNCounter2), CommitTime2}),

    SS3 = #snapshot_get_response{commit_parameters = CommitTime2, ops_list = Ops,
				materialized_snapshot = #materialized_snapshot{last_op_id = LastOp, value = PNCounter2}},
    {ok, PNCounter3, 4, CommitTime3, _SsSave, _} = materialize(Type,
							    #transaction{snapshot_vc = vectorclock:from_list([{1,3},{2,2}]),
txn_id = ignore, transactional_protocol = clocksi},
							    SS3),
    ?assertEqual({4, vectorclock:from_list([{1,3},{2,2}])}, {Type:value(PNCounter3), CommitTime3}).
       
materializer_clocksi_concurrent_test() ->
    Type = antidote_crdt_counter,
    PNCounter = new(Type),
    ?assertEqual(0,Type:value(PNCounter)),
    Op1 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,2},
	    dc_and_commit_time = {1, 1}, txid = 1, dependency_vc=vectorclock:from_list([{1,1},{2,1}])},
    Op2 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,1},
	    dc_and_commit_time = {1, 2}, txid = 2, dependency_vc=vectorclock:from_list([{1,2},{2,1}])},
    Op3 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,1},
	    dc_and_commit_time = {2, 2}, txid = 3, dependency_vc=vectorclock:from_list([{1,1},{2,1}])},

    Ops = [{3,Op2},{2,Op3},{1,Op1}],
    {ok, PNCounter2, 3, CommitTime2, _Keep} = materialize_intern(Type,
                                      [], 0, 3, ignore,
	    #transaction{snapshot_vc = vectorclock:from_list([{2,2},{1,2}]),
		    txn_id = ignore, transactional_protocol = clocksi},
                                      Ops, ignore, false, 0),
    {ok, PNCounter3, _} = apply_operations(Type, PNCounter, 0, PNCounter2),
    ?assertEqual({4, vectorclock:from_list([{1,2},{2,2}])}, {Type:value(PNCounter3), CommitTime2}),
    Snapshot=new(Type),
	
    SS = #snapshot_get_response{commit_parameters = ignore, ops_list = Ops,
				materialized_snapshot = #materialized_snapshot{last_op_id = 0, value = Snapshot}},
    {ok, PNcounter3, 1, CommitTime3, _SsSave1, _} = materialize(Type,
	    #transaction{snapshot_vc = vectorclock:from_list([{1,2},{2,1}]),
		    txn_id = ignore, transactional_protocol = clocksi}, SS),
    ?assertEqual({3, vectorclock:from_list([{1,2},{2,1}])}, {Type:value(PNcounter3), CommitTime3}),
    {ok, PNcounter4, 2, CommitTime4, _SsSave2, _} = materialize(Type,
	    #transaction{snapshot_vc = vectorclock:from_list([{1,1},{2,2}]),
		    txn_id = ignore, transactional_protocol = clocksi}, SS),
    ?assertEqual({3, vectorclock:from_list([{1,1},{2,2}])}, {Type:value(PNcounter4), CommitTime4}),
    {ok, PNcounter5, 1, CommitTime5, _SsSave3, _} = materialize(Type,
	    #transaction{snapshot_vc = vectorclock:from_list([{1,1},{2,1}]),
		    txn_id = ignore, transactional_protocol = clocksi}, SS),
    ?assertEqual({2, vectorclock:from_list([{1,1},{2,1}])}, {Type:value(PNcounter5), CommitTime5}).

%% @doc Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    Type = antidote_crdt_counter,
    PNCounter = new(Type),
    ?assertEqual(0,Type:value(PNCounter)),
    Ops = [],
    {ok, PNCounter2, 0, ignore, _SsSave} = materialize_intern(Type, [], 0, 0,ignore,
	    #transaction{snapshot_vc = vectorclock:from_list([{1,1}]),
		    txn_id = ignore, transactional_protocol = clocksi},
	        Ops, ignore, false, 0),
    {ok, PNCounter3, _} = apply_operations(Type, PNCounter, 0, PNCounter2),
    ?assertEqual(0,Type:value(PNCounter3)).

materializer_eager_clocksi_test()->
    Type = antidote_crdt_counter,
    PNCounter = new(Type),
    ?assertEqual(0,Type:value(PNCounter)),
    % test - no ops
    PNCounter2 = materialize_eager(Type, PNCounter, []),
    ?assertEqual(0, Type:value(PNCounter2)),
    % test - several ops
    Op1 = {increment,1},
    Op2 = {increment,2},
    Op3 = {increment,3},
    Op4 = {increment,4},
    Ops = [Op1, Op2, Op3, Op4],
    PNCounter3 = materialize_eager(Type, PNCounter, Ops),
    ?assertEqual(10, Type:value(PNCounter3)).

is_op_in_snapshot_test() ->
    Type = antidote_crdt_counter,
    Op1 = #operation_payload{key = abc, type = Type,
                           op_param = {increment,2},
                           dc_and_commit_time= {dc1, 1}, txid = 1, dependency_vc=vectorclock:from_list([{dc1,1}])},
    OpCT1 = {dc1, 1},
	OpCT1SS = vectorclock:from_list([OpCT1]),
	ST1 = vectorclock:from_list([{dc1, 2}]),
	ST2 = vectorclock:from_list([{dc1, 0}]),
	Tx1 = #transaction{snapshot_vc = ST1, transactional_protocol = clocksi, txn_id = 2},
	Tx2 = #transaction{snapshot_vc = ST2, transactional_protocol = clocksi, txn_id = 2},
	?assertEqual({true,false,OpCT1SS}, is_op_in_snapshot(Op1, OpCT1, OpCT1SS, Tx1, ignore,ignore)),
	?assertEqual({false,false,ignore}, is_op_in_snapshot(Op1,OpCT1, OpCT1SS, Tx2, ignore,ignore)).

is_op_in_snapshot_physics_test() ->
	
	SnapshotParams = {SnapshotCT, SnapshotDep, SnapshotRT} = {vectorclock:from_list([{dc1, 2}, {dc2, 2}]), vectorclock:from_list([{dc1, 2}, {dc2, 1}]), vectorclock:from_list([{dc1, 10}, {dc2, 10}])},
	TransactionDepUpbound = vectorclock:from_list([{dc1, 3}, {dc2, 3}]),
	Transaction = #transaction{snapshot_vc = TransactionDepUpbound, transactional_protocol = physics, txn_id = 2},
	
	Op = #operation_payload{txid = 1},
	{OpDC, OpCT} = {dc1, 1},
	OpDependencyVC = vectorclock:from_list([{dc1, 0}, {dc2, 0}]),
	
	%% this op has commit time smaller than the snapshot, should return the snapshot parameters.
	?assertEqual({false,true,SnapshotParams},is_op_in_snapshot(Op, {OpDC, OpCT}, OpDependencyVC, Transaction, SnapshotParams, SnapshotParams)),
	
	Op2 = #operation_payload{txid = 1},
	{OpDC2, OpCT2} = {dc2, 3},
	
	OpDependencyVC2 = vectorclock:from_list([{dc1, 0}, {dc2, 0}]),
	ExpectedCT = vectorclock:from_list([{dc1, 2}, {dc2, 3}]),
	%% this op has commit time bigger than the snapshot, should return the snapshot parameters.
	?assertEqual({true, false,{ExpectedCT, SnapshotDep, SnapshotRT}},is_op_in_snapshot(Op2, {OpDC2, OpCT2}, OpDependencyVC2, Transaction, SnapshotParams, SnapshotParams)),
	
	Op3 = #operation_payload{txid = 1},
	{OpDC3, OpCT3} = {dc2, 4}, %% commit time is bigger
	OpDependencyVC3 = vectorclock:from_list([{dc1, 4}, {dc2, 2}]), %% dependencuy time is concurrent with the transaction snapshot and bigger than the snapshot.
	
	ExpectedCT3 = vectorclock:from_list([{dc1, 4}, {dc2, 4}]), %% we expect the dependency time and the commit time of the op.
	ExpectedDep3 = vectorclock:from_list([{dc1, 4}, {dc2, 2}]),
	%% this should be included and modify the commit time and dependency time.
	?assertEqual({true, false,{ExpectedCT3, ExpectedDep3, SnapshotRT}},is_op_in_snapshot(Op3, {OpDC3, OpCT3}, OpDependencyVC3, Transaction, SnapshotParams, SnapshotParams)),
	
	Op4 = #operation_payload{txid = 1},
	{OpDC4, OpCT4} = {dc2, 8}, %% commit time is bigger
	OpDependencyVC4 = vectorclock:from_list([{dc1, 8}, {dc2, 7}]), %% dependency is bigger than snapshot limit, should be left out.
	
	ExpectedRT4 = vectorclock:from_list([{dc1, 8}, {dc2, 7}]), %% operation should be left out, we expect that the read time will change.
	
	?assertEqual({false, false,{SnapshotCT, SnapshotDep, ExpectedRT4}},is_op_in_snapshot(Op4, {OpDC4, OpCT4}, OpDependencyVC4, Transaction, SnapshotParams, SnapshotParams)).
	
	
	
compat_test() ->
	ST1 = vectorclock:from_list([{dc1, 2}, {dc2, 0}]),
	ST2 = vectorclock:from_list([{dc1, 0}, {dc2, 2}]), %% concurrent, should return true
	ST3 = vectorclock:from_list([{dc1, 2}, {dc2, 2}]), %% bigger, should return false
	ST4 = vectorclock:from_list([{dc1, 0}, {dc2, 2}]), %% smaller, should return true
	ST5 = vectorclock:from_list([{dc1, 2}, {dc2, 0}]), %% equal, should return true
	Transaction = #transaction{snapshot_vc = ST1, transactional_protocol = physics, txn_id = 2},
	?assertEqual(compat(ST2, Transaction), true),
	?assertEqual(compat(ST3, Transaction), false),
	?assertEqual(compat(ST4, Transaction), true),
	?assertEqual(compat(ST5, Transaction), true).

-endif.
