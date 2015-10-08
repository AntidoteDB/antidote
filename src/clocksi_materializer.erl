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
%%      Snapshot: Current state of the CRDT
%%      SnapshotCommitTime: The time used to describe the state of the CRDT given in Snapshot
%%      MinSnapshotTime: The threshold time given by the reading transaction
%%      Ops: The list of operations to apply in causal order
%%      TxId: The Id of the transaction requesting the snapshot
%%      Output: A tuple. The first element is ok, the seond is the CRDT after appliying the operations,
%%      the third element is the smallest vectorclock that describes this snapshot,
%%      the fourth element is a boolean, it it is true it means that the returned snapshot contains
%%      more operations than the one given as input, false otherwise.
-spec materialize(type(), snapshot(),
		  snapshot_time() | ignore,
		  snapshot_time(),
		  [clocksi_payload()], txid() | ignore) -> {ok, snapshot(), 
						   snapshot_time() | ignore, boolean()} | {error, reason()}.
materialize(Type, Snapshot, SnapshotCommitTime, MinSnapshotTime, Ops, TxId) ->
    case materialize_intern(Type, [], SnapshotCommitTime, MinSnapshotTime, Ops, TxId, SnapshotCommitTime,false) of
	 {ok, OpList, LastOpCt, IsNewSS} ->
	    case apply_operations(Type, Snapshot, OpList) of
		{ok, NewSS} ->
		    {ok, NewSS, LastOpCt, IsNewSS};
		{error, Reason} ->
		    {error, Reason}
	    end;
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
    case materializer:update_snapshot(Type, Snapshot, Op#clocksi_payload.op_param) of
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
%%      SnapshotCommitTime: The time used to describe the intitial state of the CRDT given in Snapshot
%%      MinSnapshotTime: The threshold time given by the reading transaction
%%      Ops: The list of operations to apply in causal order
%%      TxId: The Id of the transaction requesting the snapshot
%%      LastOpCommitTime: The snapshot time of the last operation in the list of operations to apply
%%      NewSS: Boolean that is true if any operations should be applied, fale otherwise.  Should start as false.
%%      Output: A tuple with 4 elements or an error.  The first element of the tuple is the atom ok.
%%      The second element is the list of operations that should be applied to the snapshot.
%%      The thrid element is the snapshot time of the last operation in the list.
%%      The fourth element is a boolean, true if a new snapshot should be generated, false otherwise.
-spec materialize_intern(type(), 
		  [clocksi_payload()],
		  snapshot_time() | ignore,
		  snapshot_time(),
		  [clocksi_payload()], 
		  txid() | ignore, 
		  snapshot_time() | ignore,
		  boolean()) ->
			 {ok,[clocksi_payload()],snapshot_time()|ignore,boolean()} | {error, reason()}.
materialize_intern(_Type, OpList, _SnapshotCommitTime, _MinSnapshotTime, [], _TxId, LastOpCt, NewSS) ->
    {ok, OpList, LastOpCt, NewSS};

materialize_intern(Type, OpList, SnapshotCommitTime, MinSnapshotTime, [Op|Rest], TxId, LastOpCt, NewSS) ->
    Result = case Type == Op#clocksi_payload.type of
		 true ->
		     OpCom=Op#clocksi_payload.commit_time,
		     OpSS=Op#clocksi_payload.snapshot_time,
		     %% Check if the op is not in the previous snapshot and should be included in the new one
		     case (is_op_in_snapshot(TxId, Op, OpCom, OpSS, MinSnapshotTime, SnapshotCommitTime, LastOpCt)) of
			 {true,_,NewOpCt} ->
			     {ok, [Op | OpList], NewOpCt, false, true};			     
			 {false,false,_} ->
			     {ok, OpList, LastOpCt, false, NewSS}; % no update
			 {false,true,_} ->
			     {ok, OpList, LastOpCt, true, NewSS}
		     end;
		 false -> %% Op is not for this {Key, Type}
		     %% @todo THIS CASE PROBABLY SHOULD NOT HAPPEN?! 
		     {ok, OpList, LastOpCt, false, NewSS} %% no update
	     end,
    case Result of
	{error, Reason1} ->
	    {error,Reason1};
	{ok, NewOpList1, NewLastOpCt, false, NewSS1} ->
	    materialize_intern(Type,NewOpList1,SnapshotCommitTime,MinSnapshotTime,Rest,TxId,NewLastOpCt,NewSS1);
	{ok, NewOpList1, NewLastOpCt, true, NewSS1} ->
	    %% can skip the rest of the ops because they are already included in the SS
	    materialize_intern(Type,NewOpList1,SnapshotCommitTime,MinSnapshotTime,[],TxId,NewLastOpCt,NewSS1)
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
-spec is_op_in_snapshot(txid(), clocksi_payload(), commit_time(), snapshot_time(), snapshot_time(),
			snapshot_time() | ignore, snapshot_time()) -> {boolean(),boolean(),snapshot_time()}.
is_op_in_snapshot(TxId, Op, {OpDc, OpCommitTime}, OperationSnapshotTime, SnapshotTime, LastSnapshot, PrevTime) ->
    %% First check if the op was already included in the previous snapshot
    %% Is the "or TxId ==" part necessary and correct????
    case materializer_vnode:belongs_to_snapshot_op(
	   LastSnapshot,{OpDc,OpCommitTime},OperationSnapshotTime) or (TxId == Op#clocksi_payload.txid) of
	true ->
	    %% If not, check if it should be included in the new snapshot
	    %% Replace the snapshot time of the dc where the transaction committed with the commit time
	    OpSSCommit = dict:store(OpDc, OpCommitTime, OperationSnapshotTime),
	    PrevTime2 = case PrevTime of
			    ignore ->
				OpSSCommit;
			    _ ->
				PrevTime
			end,
    	    {Result,NewTime} = dict:fold(fun(DcIdOp,TimeOp,{Acc,PrevTime3}) ->
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
									   end,0,PrevTime3),
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
    Op1 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,2},1}},
                           commit_time = {1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{1,1}])},
    Op2 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,1},1}},
                           commit_time = {1, 2}, txid = 2, snapshot_time=vectorclock:from_list([{1,2}])},
    Op3 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,1},1}},
                           commit_time = {1, 3}, txid = 3, snapshot_time=vectorclock:from_list([{1,3}])},
    Op4 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,2},1}},
                           commit_time = {1, 4}, txid = 4, snapshot_time=vectorclock:from_list([{1,4}])},

    Ops = [Op1,Op2,Op3,Op4],
    {ok, PNCounter2, CommitTime2, _SsSave} = materialize(crdt_pncounter,
						PNCounter, ignore, vectorclock:from_list([{1,3}]),
						Ops, ignore),
    ?assertEqual({4, vectorclock:from_list([{1,3}])}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    {ok, PNcounter3, CommitTime3, _SsSave1} = materialize(crdt_pncounter, PNCounter, ignore,
                                   vectorclock:from_list([{1,4}]), Ops, ignore),
    ?assertEqual({6, vectorclock:from_list([{1,4}])}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    {ok, PNcounter4, CommitTime4, _SsSave2} = materialize(crdt_pncounter, PNCounter, ignore,
                                   vectorclock:from_list([{1,7}]), Ops, ignore),
    ?assertEqual({6, vectorclock:from_list([{1,4}])}, {crdt_pncounter:value(PNcounter4), CommitTime4}).


materializer_clocksi_concurrent_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Op1 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,2}, actor1}},
                           commit_time = {1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{1,1},{2,1}])},
    Op2 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           commit_time = {1, 2}, txid = 2, snapshot_time=vectorclock:from_list([{1,2},{2,1}])},
    Op3 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           commit_time = {2, 2}, txid = 3, snapshot_time=vectorclock:from_list([{1,1},{2,1}])},

    Ops = [Op1,Op2,Op3],
    {ok, PNCounter2, CommitTime2, _Keep} = materialize_intern(crdt_pncounter,
                                      [], ignore,
                                      vectorclock:from_list([{2,2},{1,2}]),
                                      Ops, ignore, ignore, false),
    {ok, PNCounter3} = apply_operations(crdt_pncounter, PNCounter, PNCounter2),
    ?assertEqual({4, vectorclock:from_list([{1,2},{2,2}])}, {crdt_pncounter:value(PNCounter3), CommitTime2}),
    
    Snapshot=new(crdt_pncounter),
    {ok, PNcounter3, CommitTime3, _SsSave1} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,2},{2,1}]), Ops, ignore),
    ?assertEqual({3, vectorclock:from_list([{1,2},{2,1}])}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    
    {ok, PNcounter4, CommitTime4, _SsSave2} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,1},{2,2}]),Ops, ignore),
    ?assertEqual({3, vectorclock:from_list([{1,1},{2,2}])}, {crdt_pncounter:value(PNcounter4), CommitTime4}),
    
    {ok, PNcounter5, CommitTime5, _SsSave3} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,1},{2,1}]),Ops, ignore),
    ?assertEqual({2, vectorclock:from_list([{1,1},{2,1}])}, {crdt_pncounter:value(PNcounter5), CommitTime5}).

%% @doc Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Ops = [],
    {ok, PNCounter2, ignore, _SsSave} = materialize_intern(crdt_pncounter, [], ignore,
						    vectorclock:from_list([{1,1}]),
						    Ops, ignore, ignore, false),
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
    Op1 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,2}, actor1}},
                           commit_time = {dc1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{dc1,1}])},
    OpCT1 = {dc1, 1},
    OpCT1SS = vectorclock:from_list([OpCT1]),
    ST1 = vectorclock:from_list([{dc1, 2}]),
    ST2 = vectorclock:from_list([{dc1, 0}]),
    ?assertEqual({true,false,OpCT1SS}, is_op_in_snapshot(2,Op1,OpCT1, OpCT1SS, ST1, ignore,ignore)),
    ?assertEqual({false,false,ignore}, is_op_in_snapshot(2,Op1,OpCT1, OpCT1SS, ST2, ignore,ignore)).
    
  
-endif.
