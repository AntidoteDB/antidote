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


%% @doc Applies the operation of a list to a CRDT. Only the
%%      operations with smaller timestamp than the specified
%%      are considered. Newer operations are discarded.
%%      Two materialized objects are returned, one that will be cached
%%      and one that will be returned to the read.
%%      Input:
%%      Type: The type of CRDT to create
%%      Snapshot: Current state of the CRDT
%%      SnapshotCommitTime: The time used to describe the state of the CRDT given in Snapshot
%%      MinFromSnapshotTime: The threshold time given by the reading transaction
%%      Ops: The list of operations to apply in causal order
%%      TxId: The Id of the transaction requesting the snapshot
%%      Output: The CRDT after appliying the operations and its commit
%%      time taken from the last operation that was applied to the snapshot.
%%      SnapshotSave is the snapshot that will be cached and is described by time
%%      CommitTime
-spec materialize(type(), snapshot(),
		  snapshot_time() | ignore,
		  snapshot_time(),
		  [clocksi_payload()], txid() | ignore) -> {ok, snapshot(), 
						   snapshot_time() | ignore, boolean()} | {error, reason()}.
materialize(Type, Snapshot, SnapshotCommitTime, MinSnapshotTime, Ops, TxId) ->
    materialize_intern(Type, Snapshot, SnapshotCommitTime, MinSnapshotTime, Ops, TxId, SnapshotCommitTime,false,0).


-spec materialize_intern(type(), 
		  snapshot(),
		  snapshot_time() | ignore,
		  snapshot_time(),
		  [clocksi_payload()], 
		  txid() | ignore, 
		  snapshot_time() | ignore,
		  boolean(), non_neg_integer()) ->
			 {ok,snapshot(),snapshot_time()|ignore,boolean()} | {error, reason()}.
materialize_intern(_Type, Snapshot, _SnapshotCommitTime, _MinSnapshotTime, [], _TxId, LastOpCt, NewSS, _Count) ->
    {ok, Snapshot, LastOpCt, NewSS};

materialize_intern(Type, Snapshot, SnapshotCommitTime, MinSnapshotTime, [Op|Rest], TxId, LastOpCt, NewSS,Count) ->
    Result = case Type == Op#clocksi_payload.type of
		 true ->
		     OpCom=Op#clocksi_payload.commit_time,
		     OpSS=Op#clocksi_payload.snapshot_time,
		     %% Check if the op is not in the previous snapshot and should be included in the new one
		     case (is_op_in_snapshot(TxId, Op, OpCom, OpSS, MinSnapshotTime, SnapshotCommitTime, LastOpCt)) of
			 {true,_,NewOpCt} ->
			     case materializer:update_snapshot(Type, Snapshot, Op#clocksi_payload.op_param) of
				 {ok, NewSnapshot} -> 
				     {ok, NewSnapshot, NewOpCt, false, true};
				 {error, Reason} ->
				     {error, Reason}
			     end;
			 {false,false,_} ->
			     {ok, Snapshot, LastOpCt, false, NewSS}; % no update
			 {false,true,_} ->
			     {ok, Snapshot, LastOpCt, true, NewSS}
		     end;
		 false -> %% Op is not for this {Key, Type}
		     %% @todo THIS CASE PROBABLY SHOULD NOT HAPPEN?! 
		     {ok, Snapshot, LastOpCt, false, NewSS} %% no update
	     end,
    case Result of
	{error, Reason1} ->
	    {error,Reason1};
	{ok, NewSnapshot1, NewLastOpCt, false, NewSS1} ->
	    materialize_intern(Type,NewSnapshot1,SnapshotCommitTime,MinSnapshotTime,Rest,TxId,NewLastOpCt,NewSS1,Count+1);
	{ok, NewSnapshot1, NewLastOpCt, true, NewSS1} ->
	    %% can skip the rest of the ops because they are already included in the SS
	    materialize_intern(Type,NewSnapshot1,SnapshotCommitTime,MinSnapshotTime,[],TxId,NewLastOpCt,NewSS1,Count+1)
    end.

%% @doc Check whether an udpate is included in a snapshot and also
%%		if that update is newer than a snapshot's commit time
%%      Input: Dc = Datacenter Id
%%             CommitTime = local commit time of this update at DC
%%             SnapshotTime = Orddict of [{Dc, Ts}]
%%			   SnapshotCommitTime = commit time of that snapshot.
%%      Outptut: true or false
%% SnapshotCommitTime time is the snapshot that already exists, so if this op
%% is already in the snapshot, should not include it
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
                                      PNCounter, ignore,
                                      vectorclock:from_list([{2,2},{1,2}]),
                                      Ops, ignore, ignore, false,0),
    ?assertEqual({4, vectorclock:from_list([{1,2},{2,2}])}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    
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
    {ok, PNCounter2, ignore, _SsSave} = materialize_intern(crdt_pncounter, PNCounter, ignore,
						    vectorclock:from_list([{1,1}]),
						    Ops, ignore, ignore, false,0),
    ?assertEqual(0,crdt_pncounter:value(PNCounter2)).

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
