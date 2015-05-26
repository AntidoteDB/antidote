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


%% @doc Applies a list of operations to a CRDT.
%%    The operations are given as part of clocksi_payloads, i.e. with some additional meta-data.
%%    Only the operations with belonging to the snapshot_time are applied. More recent operations are discarded.
%%    The list of operations must be in causal order and is applied in the specified order.
%%   
%%      Type: Type of the CRDT;
%%      Snapshot: Current state of the CRDT;
%%      SnapshotCommitTime: Commit time corresponding to the snapshot;
%%      SnapshotTime: Threshold for the operations to be applied, only operations included in the SnapshotTime are applied;
%%      Ops: The list of clocksi_payloads containing the operations to apply in causal order
%%      TxId: ???
%% 
%%     It returns the CRDT after applying the operations and its commit
%%     time taken from the last operation that was applied to the snapshot.

-spec materialize(type(), snapshot(), commit_time() | ignore, snapshot_time(), 
  [clocksi_payload()], txid()) -> 
  {ok, snapshot(), commit_time() | ignore} | {error, reason()}.

materialize(Type, Snapshot, SnapshotCommitTime, SnapshotTime, Ops, TxId) ->
    materialize_intern(Type, Snapshot, SnapshotCommitTime, SnapshotTime, Ops, TxId, SnapshotCommitTime).


-spec materialize_intern(type(), snapshot(), commit_time() | ignore, snapshot_time(),
  [clocksi_payload()], txid(), commit_time() | ignore) ->
  {ok, snapshot(), commit_time() | ignore} | {error, reason()}.

materialize_intern(_, Snapshot, _SnapshotCommitTime, _SnapshotTime, [], _TxId, CommitTime) ->
    {ok, Snapshot, CommitTime};
materialize_intern(Type, Snapshot, SnapshotCommitTime, SnapshotTime, [Op|Rest], TxId, LastOpCommitTime) ->
    % Check whether operation is for the right crdt type
    Result = case Type == Op#clocksi_payload.type of
        true ->
            OpCommitTime=Op#clocksi_payload.commit_time,
            % Check whether operation is contained in the snapshot time, but not already applied, or belongs to a txn
            case (is_op_in_snapshot(OpCommitTime, SnapshotTime, SnapshotCommitTime)
                  or (TxId == Op#clocksi_payload.txid)) of
                true ->
                    case materializer:update_snapshot(Type, Snapshot, Op#clocksi_payload.op_param) of
                        {ok, NewSnapshot} -> 
                            {ok, NewSnapshot, OpCommitTime};
                	      {error, Reason} ->
                            {error, Reason}
                    end;
                false ->
                    {ok, Snapshot, LastOpCommitTime} % no update
            end;
        false -> %% Op is not for this {Key, Type}
            %% @todo THIS CASE PROBABLY SHOULD NOT HAPPEN?! 
            {ok, Snapshot, LastOpCommitTime} %% no update
    end,
    case Result of
      {error, Reason1} ->
          {error, Reason1};
      {ok, NewSnapshot1, NewLastOpCommitTime1} -> 
          materialize_intern(Type, NewSnapshot1, SnapshotCommitTime, SnapshotTime, Rest, TxId, NewLastOpCommitTime1)
    end.
    
%% @doc Checks whether a commit time is included in a snapshot time and, if not ignored,
%%		whether the commit time is from the same DC and more recent than some commit time.
-spec is_op_in_snapshot(commit_time(), snapshot_time(), commit_time() | ignore) -> boolean().
is_op_in_snapshot(OperationCommitTime, SnapshotTime, SnapshotCommitTime) ->
	{OpDc, OpCommitTime} = OperationCommitTime,
  {ok, Ts} = vectorclock:get_clock_of_dc(OpDc, SnapshotTime),
  InSnapshot = OpCommitTime =< Ts,
  case SnapshotCommitTime of
    ignore -> 
    	  InSnapshot;
    {_SnapshotDc, _SnapshotCT} ->
		    InSnapshot andalso is_smaller(SnapshotCommitTime, OperationCommitTime)
	end.

%% @doc Checks whether a commit time is smaller than another one.
%%    Returns also false if times are incomparable.
%% @todo Fix the types! It should be:
%-spec is_smaller(commit_time(), commit_time()) -> boolean.
is_smaller({OpDc1, OpCommitTime1}, {OpDc2, OpCommitTime2}) ->
    (OpDc1 /= OpDc2) or (OpCommitTime1 < OpCommitTime2).

%% @doc Apply updates in given order without any checks.
%%    Careful: In contrast to materialize/6, it takes just operations, not clocksi_payloads!
-spec materialize_eager(type(), snapshot(), [op()]) -> snapshot().
materialize_eager(Type, Snapshot, Ops) ->
    materializer:materialize_eager(Type, Snapshot, Ops).


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
    Ops = [Op1, Op2, Op3, Op4],

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
                                      Ops, ignore),
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
                                Ops, ignore),
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
	OpCommitTime = {dc1, 5},
	SnapshotTime1 = vectorclock:from_list([{dc1, 10}]),
	SnapshotTime2 = vectorclock:from_list([{dc1, 1}]),
	SnapshotTime3 = vectorclock:from_list([{dc2, 6}]),
  ?assertEqual(true,  is_op_in_snapshot(OpCommitTime, SnapshotTime1, ignore)),
	?assertEqual(false, is_op_in_snapshot(OpCommitTime, SnapshotTime2, ignore)),
  ?assertEqual(false, is_op_in_snapshot(OpCommitTime, SnapshotTime3, ignore)),
    
  OpCommitTime2 = {dc1, 2},
  ?assertEqual(true,  is_op_in_snapshot(OpCommitTime, SnapshotTime1, OpCommitTime2)),
  ?assertEqual(false, is_op_in_snapshot(OpCommitTime, SnapshotTime2, OpCommitTime2)),
  
  OpCommitTime3 = {dc1, 8},
  ?assertEqual(false, is_op_in_snapshot(OpCommitTime, SnapshotTime1, OpCommitTime3)),
  ?assertEqual(false, is_op_in_snapshot(OpCommitTime, SnapshotTime2, OpCommitTime3)).
  
-endif.
