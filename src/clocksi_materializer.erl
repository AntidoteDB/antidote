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

%% @doc Creates an empty CRDT
%%      Input: Type: The type of CRDT to create
%%      Output: The newly created CRDT
-spec new(type()) -> term().
new(Type) ->
    Type:new().


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
		  SnapshotCommitTime::{dcid(),CommitTime::non_neg_integer()} | ignore,
		  snapshot_time(),
		  [clocksi_payload()], txid()) -> {ok, snapshot(), 
						   {dcid(),CommitTime::non_neg_integer()} | ignore, snapshot(), [clocksi_payload()]} | {error, term()}.
materialize(Type, Snapshot, SnapshotCommitTime, MinSnapshotTime, Ops, TxId) ->
    {FirstDc,FirstTime} = hd(dict:to_list(MinSnapshotTime)),
    %% SSTime is the time of the snapshot that will be cached
    %% It is described by a single scalar, so that the cached snapshots can be ordred
    %% (also reducing the number of snapshots stored)
    %% It is the smallest entry of the vector provided for the read
    SSTime = dict:fold(fun(DcId,CT,{AccDc,AccTime}) ->
			       case CT < AccTime of
				   true ->
				       {DcId,CT};
				   false ->
				       {AccDc,AccTime}
			       end
		       end, {FirstDc,FirstTime}, MinSnapshotTime),
    materialize(Type, Snapshot, SnapshotCommitTime, SSTime, MinSnapshotTime, Ops, TxId, Snapshot, SnapshotCommitTime).


-spec materialize(type(), 
		  snapshot(),
		  SnapshotCommitTime::{dcid(),CommitTime::non_neg_integer()} | ignore,
		  {dcid(),non_neg_integer()},
		  snapshot_time(),
		  [clocksi_payload()], 
		  txid(), 
		  snapshot(),
		  {dcid(),non_neg_integer()} | ignore) ->
			 {ok,snapshot(), {dcid(),non_neg_integer()} | ignore, snapshot(), [clocksi_payload()]} | {error, term()}.
materialize(_Type, Snapshot, _SnapshotCommitTime, {SSDcId,SSTime}, _MinSnapshotTime, [], _TxId, SnapshotUse, _LastOpCt) ->
    {ok, Snapshot, {SSDcId,SSTime}, SnapshotUse};

materialize(Type, Snapshot, SnapshotCommitTime, SSTime, MinSnapshotTime, [Op|Rest], TxId, SnapshotSave, LastOpCt) ->
    case Type == Op#clocksi_payload.type of
        true ->
            OpCom=Op#clocksi_payload.commit_time,
	    OpSS=Op#clocksi_payload.snapshot_time,
	    %% Check if the op is not in the previous snapshot and should be included in the new one
            case (is_op_in_snapshot(OpCom, OpSS, MinSnapshotTime, SnapshotCommitTime)
                  or (TxId == Op#clocksi_payload.txid)) of
                true ->
		    Ds = Op#clocksi_payload.op_param,
		    case Ds of
                        {merge, State} ->
                            NewSnapshot = Type:merge(Snapshot, State),
			    %% Check if the op should be included in the snapshot that will be cached
			    case materializer_vnode:belongs_to_snapshot_op(SSTime,OpCom,OpSS) of
				false ->
				    materialize(Type,
						NewSnapshot,
						SnapshotCommitTime,
						SSTime,
						MinSnapshotTime,
						Rest,
						TxId,
					        Type:merge(SnapshotSave, State),
						OpCom
						);
				true ->
				    materialize(Type,
						NewSnapshot,
						SnapshotCommitTime,
						SSTime,
						MinSnapshotTime,
						Rest,
						TxId,
					        SnapshotSave,
						LastOpCt
						)
			    end;
                        {update, DownstreamOp} ->
                            case Type:update(DownstreamOp, Snapshot) of
                                {ok, NewSnapshot} ->
				    %% Check if the op should be included in the snapshot that will be cached
				    case materializer_vnode:belongs_to_snapshot_op(SSTime,OpCom,OpSS) of
					false ->
					    case Type:update(DownstreamOp, SnapshotSave) of
						{ok, NewSnapshotSave} ->
						    materialize(Type,
								NewSnapshot,
								SnapshotCommitTime,
								SSTime,
								MinSnapshotTime,
								Rest,
								TxId,
							        NewSnapshotSave,
								OpCom
								);
						{error, Reason} ->
						    {error, Reason}
					    end;
					true ->
					    materialize(Type,
							NewSnapshot,
							SnapshotCommitTime,
							SSTime,
							MinSnapshotTime,
							Rest,
							TxId,
						        SnapshotSave,
							LastOpCt
							)
				    end;
                                {error, Reason} ->
                                    {error, Reason}
                            end
                    end;
                false ->
		    materialize(Type, Snapshot, SnapshotCommitTime, SSTime, MinSnapshotTime,
				Rest, TxId, SnapshotSave, LastOpCt)
	    end;
	false -> %% Op is not for this {Key, Type}
	    materialize(Type, Snapshot, SnapshotCommitTime, SSTime, MinSnapshotTime, Rest, TxId, SnapshotSave, LastOpCt)
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
-spec is_op_in_snapshot({dcid(),non_neg_integer()}, vectorclock:vectorclock(), vectorclock:vectorclock(),
			{dcid(),non_neg_integer()} | ignore) -> boolean().
is_op_in_snapshot({OpDc, OpCommitTime}, OperationSnapshotTime, SnapshotTime, LastSnapshot) ->
    %% First check if the op was already included in the previous snapshot
    case materializer_vnode:belongs_to_snapshot_op(
	   LastSnapshot,{OpDc,OpCommitTime},SnapshotTime) of
	true ->
	    %% If not, check if it should be included in the new snapshot
	    %% Replace the snapshot time of the dc where the transaction committed with the commit time
	    OpSSCommit = dict:store(OpDc, OpCommitTime, OperationSnapshotTime),
    	    dict:fold(fun(DcIdOp,TimeOp,Acc) ->
			      case dict:find(DcIdOp,SnapshotTime) of
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
			      end
		      end, true, OpSSCommit);
	false->
	    false
    end.
	    

%% @doc materialize_eager: apply updates in order without any checks
-spec materialize_eager(type(), snapshot(), [clocksi_payload()]) -> snapshot().
materialize_eager(_, Snapshot, []) ->
    Snapshot;
materialize_eager(Type, Snapshot, [Op|Rest]) ->
   case Op of
        {merge, State} ->
            NewSnapshot = Type:merge(Snapshot, State);
        {update, DownstreamOp} ->
            {ok, NewSnapshot} = Type:update(DownstreamOp, Snapshot)
    end,
    materialize_eager(Type, NewSnapshot, Rest).


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
    ?assertEqual({4, {1,3}}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    {ok, PNcounter3, CommitTime3, _SsSave1} = materialize(crdt_pncounter, PNCounter, ignore,
                                   vectorclock:from_list([{1,4}]), Ops, ignore),
    ?assertEqual({6, {1,4}}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    {ok, PNcounter4, CommitTime4, SsSave2} = materialize(crdt_pncounter, PNCounter, ignore,
                                   vectorclock:from_list([{1,7}]), Ops, ignore),
    ?assertEqual({6, {1,7}, {6,0}}, {crdt_pncounter:value(PNcounter4), CommitTime4, SsSave2}).

materializer_clocksi_concurrent_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Op1 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,2}, actor1}},
                           commit_time = {1, 1}, txid = 1, snapshot_time=vectorclock:from_list([{1,1}])},
    Op2 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           commit_time = {1, 2}, txid = 2, snapshot_time=vectorclock:from_list([{1,2}])},
    Op3 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           commit_time = {2, 1}, txid = 3, snapshot_time=vectorclock:from_list([{2,1}])},

    Ops = [Op1,Op2,Op3],
    {ok, PNCounter2, CommitTime2, _SsSave} = materialize(crdt_pncounter,
                                      PNCounter, ignore, {2,1},
                                      vectorclock:from_list([{2,2},{1,2}]),
                                      Ops, ignore, PNCounter, ignore),
    ?assertEqual({4, {2,1}}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    
    
    
    Snapshot=new(crdt_pncounter),
    {ok, PNcounter3, CommitTime3, _SsSave1} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,2}]), Ops, ignore),
    ?assertEqual({3, {1,2}}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    
    {ok, PNcounter4, CommitTime4, _SsSave2} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{2,1}]),Ops, ignore),
    ?assertEqual({1, {2,1}}, {crdt_pncounter:value(PNcounter4), CommitTime4}),
    
    {ok, PNcounter5, CommitTime5, _SsSave3} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,1}]),Ops, ignore),
    ?assertEqual({2, {1,1}}, {crdt_pncounter:value(PNcounter5), CommitTime5}).

%% @doc Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Ops = [],
    {ok, PNCounter2, {1,1}, _SsSave} = materialize(crdt_pncounter, PNCounter, ignore, {1,1},
                                vectorclock:from_list([{1,1}]),
                                Ops, ignore, PNCounter, ignore),
    ?assertEqual(0,crdt_pncounter:value(PNCounter2)).
    
    
    
    
is_op_in_snapshot_test()->
    OpCT1 = {dc1, 1},
    OpCT1SS = vectorclock:from_list([OpCT1]),
    ST1 = vectorclock:from_list([{dc1, 2}]),
    ST2 = vectorclock:from_list([{dc1, 0}]),
    true = is_op_in_snapshot(OpCT1, OpCT1SS, ST1, ignore),
    false = is_op_in_snapshot(OpCT1, OpCT1SS, ST2, ignore).
    
    
-endif.
