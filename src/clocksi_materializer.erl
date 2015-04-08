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
         materialize/7,
         materialize_eager/3]).

%% @doc Creates an empty CRDT
%%      Input: Type: The type of CRDT to create
%%      Output: The newly created CRDT
-spec new(type()) -> term().
new(Type) ->
    Type:new().


%% @doc Calls the internal function materialize/6, with no TxId.
-spec materialize(type(), snapshot(),
		  SnapshotCommitTime::{dcid(),CommitTime::non_neg_integer()} | ignore,
		  snapshot_time(), snapshot_time(),
		  [clocksi_payload()], txid()) -> {ok, snapshot(), 
						   {dcid(),CommitTime::non_neg_integer()} | ignore, snapshot(), [clocksi_payload()]} | {error, term()}.
materialize(Type, Snapshot, SnapshotCommitTime, MinSnapshotTime, MaxSnapshotTime, Ops, TxId) ->
    {FirstDc,FirstTime} = hd(dict:to_list(MinSnapshotTime)),
    SSTime = dict:fold(fun(DcId,CT,{AccDc,AccTime}) ->
			       case CT < AccTime of
				   true ->
				       {DcId,CT};
				   false ->
				       {AccDc,AccTime}
			       end
		       end, {FirstDc,FirstTime}, MinSnapshotTime),
    case materialize(Type, Snapshot, SnapshotCommitTime, SSTime, MinSnapshotTime, MaxSnapshotTime, Ops, TxId, Snapshot, SnapshotCommitTime, []) of
	{ok, Val, CommitTime, SnapshotSave, Rem} ->
	    {ok, Val, CommitTime, SnapshotSave, Rem};
	{error, Reason} ->
	    {error, Reason}
    end.



%% @doc Applies the operation of a list to a CRDT. Only the
%%      operations with smaller timestamp than the specified
%%      are considered. Newer operations are discarded.
%%      Input:
%%      Type: The type of CRDT to create
%%      Snapshot: Current state of the CRDT
%%      SnapshotTime: Threshold for the operations to be applied.
%%      Ops: The list of operations to apply in causal order
%%      Output: The CRDT after appliying the operations and its commit
%%      time taken from the last operation that was applied to the snapshot.
-spec materialize(type(), 
		  snapshot(),
		  SnapshotCommitTime::{dcid(),CommitTime::non_neg_integer()} | ignore,
		  {dcid(),non_neg_integer()},
		  snapshot_time(),
		  snapshot_time(),
		  [clocksi_payload()], 
		  txid(), 
		  snapshot(),
		  {dcid(),non_neg_integer()} | ignore,
		  Remainder::[clocksi_payload]) ->
			 {ok,snapshot(), {dcid(),non_neg_integer()} | ignore, snapshot(), [clocksi_payload()]} | {error, term()}.
materialize(_Type, Snapshot, _SnapshotCommitTime, {SSDcId,SSTime}, _MinSnapshotTime, _MaxSnapshotTime, [], _TxId, SnapshotUse, LastOpCt, Remainder) ->
    %% case AddedOp of
    %% 	dont_ignore ->
    %% 	    SSTime = dict:fold(fun(DcId,Time,{PrevDcId,PrevTime}) ->
    %% 					case Time < PrevTime of
    %% 					    true ->
    %% 						{DcId,Time};
    %% 					    false ->
    %% 						{PrevDcId,PrevTime}
    %% 					end
    %% 				end, hd(dict:to_list(MinSnapshotTime)),MinSnapshotTime),
    %% 	    {ok, Snapshot, SSTime, Remainder};
    %% 	ignore ->
    %% 	    {ok, Snapshot, ignore, Remainder}
    %% end;
    RetSS = case LastOpCt of
		ignore ->
		    ignore;
		{LastOpDcId,LastOpTime} ->
		    case SSTime > LastOpTime of
			true ->
			    {LastOpDcId,LastOpTime};
			false ->
			    {SSDcId,SSTime}
		    end
	    end,
    {ok, Snapshot, RetSS, SnapshotUse, Remainder};

materialize(Type, Snapshot, SnapshotCommitTime, SSTime, MinSnapshotTime, MaxSnapshotTime, [Op|Rest], TxId, SnapshotSave, LastOpCt, Remainder) ->
    case Type == Op#clocksi_payload.type of
        true ->
            OpCom=Op#clocksi_payload.commit_time,
            %OpCommitTime=Op#clocksi_payload.commit_time,
	    %{OpCom,OpComTs}=OpCommitTime,
	    %OpSSCommit = dict:store(OpCom, OpComTs, Op#clocksi_payload.snapshot_time),
            case (is_op_in_snapshot(OpCom, MinSnapshotTime, SnapshotCommitTime)
                  or (TxId == Op#clocksi_payload.txid)) of
                true ->
		    Ds = case Op#clocksi_payload.op_generate of
			     upstream ->
				 {ok,DsOp} = clocksi_downstream:
				     generate_downstream_op(#transaction{},Op#clocksi_payload.key,Type,
							    Op#clocksi_payload.op_param,[],local,[{Op#clocksi_payload.key,Snapshot}]),
				 DsOp;
			     downstream ->
				 Op#clocksi_payload.op_param
			 end,
		    case Ds of
                        {merge, State} ->
                            NewSnapshot = Type:merge(Snapshot, State),
			    case is_op_in_min_snapshot(OpCom,SSTime) of
				true ->
				    materialize(Type,
						NewSnapshot,
						SnapshotCommitTime,
						SSTime,
						MinSnapshotTime,
						MaxSnapshotTime,
						Rest,
						TxId,
					        Type:merge(SnapshotSave, State),
						OpCom,
						Remainder);
				false ->
				    materialize(Type,
						NewSnapshot,
						SnapshotCommitTime,
						SSTime,
						MinSnapshotTime,
						MaxSnapshotTime,
						Rest,
						TxId,
					        SnapshotSave,
						LastOpCt,
						Remainder)
			    end;
                        {update, DownstreamOp} ->
                            case Type:update(DownstreamOp, Snapshot) of
                                {ok, NewSnapshot} ->
				    case is_op_in_min_snapshot(OpCom,SSTime) of
					true ->
					    case Type:update(DownstreamOp, SnapshotSave) of
						{ok, NewSnapshotSave} ->
						    materialize(Type,
								NewSnapshot,
								SnapshotCommitTime,
								SSTime,
								MinSnapshotTime,
								MaxSnapshotTime,
								Rest,
								TxId,
							        NewSnapshotSave,
								OpCom,
								Remainder);
						{error, Reason} ->
						    {error, Reason}
					    end;
					false ->
					    materialize(Type,
							NewSnapshot,
							SnapshotCommitTime,
							SSTime,
							MinSnapshotTime,
							MaxSnapshotTime,
							Rest,
							TxId,
						        SnapshotSave,
							LastOpCt,
							Remainder)
				    end;
                                {error, Reason} ->
                                    {error, Reason}
                            end
                    end;
                false ->
		    case MaxSnapshotTime /= ignore andalso 
			is_op_in_snapshot(OpCom, MaxSnapshotTime, SnapshotCommitTime) of
			true ->
			    materialize(Type, Snapshot, SnapshotCommitTime, SSTime, MinSnapshotTime,
					MaxSnapshotTime, Rest, TxId, SnapshotSave, LastOpCt, Remainder ++ [Op]);
			false ->
			    materialize(Type, Snapshot, SnapshotCommitTime, SSTime, MinSnapshotTime,
					MaxSnapshotTime, Rest, TxId, SnapshotSave, LastOpCt, Remainder)
		    end
	    end;
	false -> %% Op is not for this {Key, Type}
	    materialize(Type, Snapshot, SnapshotCommitTime, SSTime, MinSnapshotTime, MaxSnapshotTime, Rest, TxId, SnapshotSave, LastOpCt, Remainder)
    end.

%% max_ss(PrevSS,OpSS) ->
%%     dict:fold(fun(DcId,Time,Acc) ->
%% 		      case dict:find(DcId,Acc) of
%% 			  {ok, ATime} ->
%% 			      case ATime > Time of
%% 				  true ->
%% 				      dict:store(DcId,ATime,Acc);
%% 				  false ->
%% 				      dict:store(DcId,Time,Acc)
%% 			      end;
%% 			  error ->
%% 			      dict:store(DcId,Time,Acc)
%% 		      end
%% 	      end, PrevSS, OpSS).


-spec is_op_in_min_snapshot({dcid(),non_neg_integer()}, non_neg_integer()) -> boolean().
is_op_in_min_snapshot({_OpDc,OpCommitTime},MinSnapshotTime) ->
    OpCommitTime =< MinSnapshotTime.

%% @doc Check whether an udpate is included in a snapshot and also
%%		if that update is newer than a snapshot's commit time
%%      Input: Dc = Datacenter Id
%%             CommitTime = local commit time of this update at DC
%%             SnapshotTime = Orddict of [{Dc, Ts}]
%%			   SnapshotCommitTime = commit time of that snapshot.
%%      Outptut: true or false
%% SnapshotCommitTime time is the snapshot that already exists, so if this op
%% is already in the snapshot, should not include it
-spec is_op_in_snapshot({dcid(),non_neg_integer()}, vectorclock:vectorclock(), {dcid(),non_neg_integer()} | ignore) -> boolean().
is_op_in_snapshot({OpDc, OpCommitTime}, SnapshotTime, LastSnapshot) ->
    {ok, Ts} = vectorclock:get_clock_of_dc(OpDc, SnapshotTime),
    %%lager:info("is op in ss opss ~p, check ss ~p, old ss ~p", [OperationSnapshotTime, SnapshotTime, SnapshotCommitTime]),
    case LastSnapshot of
	ignore -> 
	    %% ignore means that there was no snapshot, so should take all ops that happened before
	    OpCommitTime =< Ts;
	{_LastSnapshotDc, LastSnapshotCommitTime} ->
	    %% case (SnapshotDc == OpDc) of
	    %%  	true ->
	    %%  	    (OpCommitTime =< Ts) and (SnapshotCT < OpCommitTime);
	    %%  	false ->
	    %%  	    OpCommitTime =< Ts
	    %% end
	    (OpCommitTime =< Ts) and (LastSnapshotCommitTime < OpCommitTime)
    end.
    %% 	    Check = dict:fold(fun(DcId,Time,Acc) ->
    %% 				      case dict:find(DcId,SnapshotCommitTime) of
    %% 					  {ok, SsCt} ->
    %% 					      case Time > SsCt of
    %% 						  true ->
    %% 						      true;
    %% 						  false ->
    %% 						      Acc
    %% 					      end;
    %% 					  error ->
    %% 					      lager:error("should have all Dcs in SS ~p", [SnapshotCommitTime]),
    %% 					      false
    %% 				      end
    %% 			      end, false, OperationSnapshotTime)
    %% end,
    %% case Check of
    %% 	true ->
    %% 	    dict:fold(fun(DcIdOp,TimeOp,Acc) ->
    %% 			       case dict:find(DcIdOp,SnapshotTime) of
    %% 				   {ok, TimeSS} ->
    %% 				       case TimeOp > TimeSS of
    %% 					   true ->
    %% 					       false;
    %% 					   false ->
    %% 					       Acc
    %% 				       end;
    %% 				   error ->
    %% 				       lager:error("Could not find DC in SS ~p", [SnapshotTime]),
    %% 				       false
    %% 			       end
    %% 		       end, true, OperationSnapshotTime);
    %% 	false ->
    %% 	    false
    %% end.

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
    Op1 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,2},1}},
                           commit_time = {1, 1}, txid = 1},
    Op2 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,1},1}},
                           commit_time = {1, 2}, txid = 2},
    Op3 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,1},1}},
                           commit_time = {1, 3}, txid = 3},
    Op4 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,2},1}},
                           commit_time = {1, 4}, txid = 4},

    Ops = [Op1,Op2,Op3,Op4],
    {ok, PNCounter2, CommitTime2, _SsSave, _Rem} = materialize(crdt_pncounter,
						PNCounter, ignore, vectorclock:from_list([{1,3}]),
						ignore,
						Ops, ignore),
    ?assertEqual({4, {1,3}}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    {ok, PNcounter3, CommitTime3, _SsSave1, _Rem} = materialize(crdt_pncounter, PNCounter, ignore,
                                   vectorclock:from_list([{1,4}]), ignore ,Ops, ignore),
    ?assertEqual({6, {1,4}}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    {ok, PNcounter4, CommitTime4, SsSave2, _Rem} = materialize(crdt_pncounter, PNCounter, ignore,
                                   vectorclock:from_list([{1,7}]), ignore, Ops, ignore),
    ?assertEqual({6, {1,4}, {6,0}}, {crdt_pncounter:value(PNcounter4), CommitTime4, SsSave2}).

materializer_clocksi_concurrent_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Op1 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,2}, actor1}},
                           commit_time = {1, 1}, txid = 1},
    Op2 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           commit_time = {1, 2}, txid = 2},
    Op3 = #clocksi_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           commit_time = {2, 1}, txid = 3},

    Ops = [Op1,Op2,Op3],
    {ok, PNCounter2, CommitTime2, _SsSave, _Rem} = materialize(crdt_pncounter,
                                      PNCounter, ignore, {2,1},
                                      vectorclock:from_list([{2,2},{1,2}]), ignore,
                                      Ops, ignore, PNCounter, ignore, []),
    ?assertEqual({4, {2,1}}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    
    
    
    Snapshot=new(crdt_pncounter),
    {ok, PNcounter3, CommitTime3, _SsSave1, _Rem} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,2}]), ignore, Ops, ignore),
    ?assertEqual({3, {1,2}}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    
    {ok, PNcounter4, CommitTime4, _SsSave2, _Rem} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{2,1}]),ignore,Ops, ignore),
    ?assertEqual({1, {2,1}}, {crdt_pncounter:value(PNcounter4), CommitTime4}),
    
    {ok, PNcounter5, CommitTime5, _SsSave3, _Rem} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,1}]),ignore,Ops, ignore),
    ?assertEqual({2, {1,1}}, {crdt_pncounter:value(PNcounter5), CommitTime5}).

%% @doc Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Ops = [],
    {ok, PNCounter2, ignore, _SsSave, _Rem} = materialize(crdt_pncounter, PNCounter, ignore, {1,1},
                                vectorclock:from_list([{1,1}]), ignore,
                                Ops, ignore, PNCounter, ignore, []),
    ?assertEqual(0,crdt_pncounter:value(PNCounter2)).
    
    
    
    
is_op_in_snapshot_test()->
	OpCT1 = {dc1, 1},
	ST1 = vectorclock:from_list([{dc1, 2}]),
	ST2 = vectorclock:from_list([{dc1, 0}]),
	true = is_op_in_snapshot(OpCT1, ST1, ignore),
	false = is_op_in_snapshot(OpCT1, ST2, ignore).
    
    
-endif.
