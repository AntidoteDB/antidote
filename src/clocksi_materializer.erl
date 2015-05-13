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


%% @doc Calls the internal function materialize/6, with no TxId.
%% @todo Documentation!
-spec materialize(type(), snapshot(), commit_time() | ignore, snapshot_time(), 
                      [clocksi_payload()], txid()) 
                    -> {ok, snapshot(), commit_time() | ignore} | {error, reason()}.
materialize(Type, Snapshot, SnapshotCommitTime, SnapshotTime, Ops, TxId) ->
    materialize(Type, Snapshot, SnapshotCommitTime, SnapshotTime, Ops, TxId, SnapshotCommitTime).



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
-spec materialize(type(), snapshot(),	commit_time() | ignore,
                      snapshot_time(),
                      [clocksi_payload()], 
                      txid(), 
                      commit_time() | ignore) ->
                             {ok,snapshot(), commit_time() | ignore} | {error, reason()}.
materialize(_, Snapshot, _SnapshotCommitTime, _SnapshotTime, [], _TxId, CommitTime) ->
    {ok, Snapshot, CommitTime};

materialize(Type, Snapshot, SnapshotCommitTime, SnapshotTime, [Op|Rest], TxId, LastOpCommitTime) ->
    case Type == Op#clocksi_payload.type of
        true ->
            OpCommitTime=Op#clocksi_payload.commit_time,
            case (is_op_in_snapshot(OpCommitTime, SnapshotTime, SnapshotCommitTime)
                  or (TxId == Op#clocksi_payload.txid)) of
                true ->
                	    case Op#clocksi_payload.op_param of
                        {merge, State} ->
                            NewSnapshot = Type:merge(Snapshot, State),
                            materialize(Type,
                                        NewSnapshot,
                                        SnapshotCommitTime,
                                        SnapshotTime,
                                        Rest,
                                        TxId,
                                        OpCommitTime);
                        {update, DownstreamOp} ->
                            case Type:update(DownstreamOp, Snapshot) of
                                {ok, NewSnapshot} ->
                                    materialize(Type,
                                                NewSnapshot,
                                                SnapshotCommitTime,
                                                SnapshotTime,
                                                Rest,
                                                TxId,
                                                OpCommitTime);
                                {error, Reason} ->
                                    {error, Reason}
                            end
                    end;
                false ->
                    materialize(Type, Snapshot, SnapshotCommitTime, SnapshotTime, Rest, TxId, LastOpCommitTime)
            end;
        false -> %% Op is not for this {Key, Type}
            materialize(Type, Snapshot, SnapshotCommitTime, SnapshotTime, Rest, TxId, LastOpCommitTime)
    end.

%% @doc Check whether an udpate is included in a snapshot and also
%%		if that update is newer than a snapshot's commit time
%%      Input: Dc = Datacenter Id
%%             CommitTime = local commit time of this update at DC
%%             SnapshotTime = Orddict of [{Dc, Ts}]
%%			   SnapshotCommitTime = commit time of that snapshot.
%%      Outptut: true or false
-spec is_op_in_snapshot(commit_time(), snapshot_time(), commit_time() | ignore) -> boolean().
is_op_in_snapshot(OperationCommitTime, SnapshotTime, SnapshotCommitTime) ->
  {OpDc, OpCommitTime} = OperationCommitTime,
  {ok, Ts} = vectorclock:get_clock_of_dc(OpDc, SnapshotTime),
  case SnapshotCommitTime of
    ignore -> 
      OpCommitTime =< Ts;
    {SnapshotDc, SnapshotCT} ->
      case (SnapshotDc == OpDc) of
        true ->
          (OpCommitTime =< Ts) and (SnapshotCT < OpCommitTime);
        false ->
          OpCommitTime =< Ts
  end
end.


  % {ok, Ts} = vectorclock:get_clock_of_dc(OpDc, SnapshotTime),
  % SnapshotIncluded =  
  %   case SnapshotCommitTime of
  %     {SnapshotDc, SnapshotCT} ->
  %   	  (SnapshotDc /= OpDc) or (SnapshotCT < OpCommitTime);
  %     ignore ->
  %       true
	 % end,
  % (OpCommitTime =< Ts) andalso SnapshotIncluded.





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
    {ok, PNCounter2, CommitTime2} = materialize(crdt_pncounter,
                                      PNCounter, ignore, vectorclock:from_list([{1,3}]),
                                      Ops, ignore),
    ?assertEqual({4, {1,3}}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    {ok, PNcounter3, CommitTime3} = materialize(crdt_pncounter, PNCounter, ignore,
                                   vectorclock:from_list([{1,4}]),Ops, ignore),
    ?assertEqual({6, {1,4}}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    {ok, PNcounter4, CommitTime4} = materialize(crdt_pncounter, PNCounter, ignore,
                                   vectorclock:from_list([{1,7}]),Ops, ignore),
    ?assertEqual({6, {1,4}}, {crdt_pncounter:value(PNcounter4), CommitTime4}).

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
    {ok, PNCounter2, CommitTime2} = materialize(crdt_pncounter,
                                      PNCounter, ignore,
                                      vectorclock:from_list([{2,2},{1,2}]),
                                      Ops, ignore, ignore),
    ?assertEqual({4, {2,1}}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    
    
    
    Snapshot=new(crdt_pncounter),
    {ok, PNcounter3, CommitTime3} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,2}]),Ops, ignore),
    ?assertEqual({3, {1,2}}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    
    {ok, PNcounter4, CommitTime4} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{2,1}]),Ops, ignore),
    ?assertEqual({1, {2,1}}, {crdt_pncounter:value(PNcounter4), CommitTime4}),
    
    {ok, PNcounter5, CommitTime5} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,1}]),Ops, ignore),
    ?assertEqual({2, {1,1}}, {crdt_pncounter:value(PNcounter5), CommitTime5}).

%% @doc Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Ops = [],
    {ok, PNCounter2, ignore} = materialize(crdt_pncounter, PNCounter, ignore,
                                vectorclock:from_list([{1,1}]),
                                Ops, ignore, ignore),
    ?assertEqual(0,crdt_pncounter:value(PNCounter2)).
    
    
    
    
is_op_in_snapshot_test()->
	OpCT1 = {dc1, 1},
	ST1 = vectorclock:from_list([{dc1, 2}]),
	ST2 = vectorclock:from_list([{dc1, 0}]),
	true = is_op_in_snapshot(OpCT1, ST1, ignore),
	false = is_op_in_snapshot(OpCT1, ST2, ignore).
    
    
-endif.
