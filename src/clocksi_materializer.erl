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
         materialize/5,
         materialize_eager/3]).

%% @doc Creates an empty CRDT
%%      Input: Type: The type of CRDT to create
%%      Output: The newly created CRDT
-spec new(type()) -> term().
new(Type) ->
    Type:new().


%% @doc Calls the internal function materialize/6, with no TxId.
-spec materialize(type(), snapshot(),
                      snapshot_time(),
                      [clocksi_payload()], txid()) -> {ok, snapshot(), {dcid(),CommitTime::non_neg_integer()}} | {error, atom()}.
%materialize(_Type, Snapshot, _SnapshotTime, []) ->
%    {ok, Snapshot};
materialize(Type, Snapshot, SnapshotTime, Ops, TxId) ->
    materialize(Type, Snapshot, SnapshotTime, Ops, TxId, ignore).



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
-spec materialize(type(), snapshot(),
                      snapshot_time(),
                      [clocksi_payload()], txid(), {dcid(),CommitTime::non_neg_integer()} | ignore) ->
                             {ok,snapshot(), {dcid(),CommitTime::non_neg_integer()}} | {error, atom()}.
materialize(_, Snapshot, _SnapshotTime, [], _TxId, CommitTime) ->
    {ok, Snapshot, CommitTime};

materialize(Type, Snapshot, SnapshotTime, [Op|Rest], TxId, LastOpCommitTame) ->
	lager:info("params: ~p", [Op#clocksi_payload.op_param]),
    case Type == Op#clocksi_payload.type of
        true ->
            OpCommitTime=Op#clocksi_payload.commit_time,
            case (is_op_in_snapshot(OpCommitTime, SnapshotTime)
                  or (TxId == Op#clocksi_payload.txid)) of
                true ->
                	    case Op#clocksi_payload.op_param of
                        {merge, State} ->
                            NewSnapshot = Type:merge(Snapshot, State),
                            materialize(Type,
                                            NewSnapshot,
                                            SnapshotTime,
                                            Rest,
                                            TxId,
                                            OpCommitTime);
                        {Update, Actor} ->
                            case Type:update(Update, Actor, Snapshot) of
                                {ok, NewSnapshot} ->
                                    materialize(Type,
                                                    NewSnapshot,
                                                    SnapshotTime,
                                                    Rest,
                                                    TxId,
                                                    OpCommitTime);
                                {error, Reason} ->
                                    {error, Reason}
                            end
                    end;
                false ->
                    materialize(Type, Snapshot, SnapshotTime, Rest, TxId, LastOpCommitTame)
            end;
        false -> %% Op is not for this {Key, Type}
            materialize(Type, Snapshot, SnapshotTime, Rest, TxId, LastOpCommitTame)
    end.

%% @doc Check whether an udpate is included in a snapshot
%%      Input: Dc = Datacenter Id
%%             CommitTime = local commit time of this update at DC
%%             SnapshotTime = Orddict of [{Dc, Ts}]
%%      Output: true or false
-spec is_op_in_snapshot({dcid(),CommitTime::non_neg_integer()},
                        snapshot_time()) -> boolean().
is_op_in_snapshot({Dc, CommitTime}, SnapshotTime) ->
    {ok, Ts} = vectorclock:get_clock_of_dc(Dc, SnapshotTime),
    CommitTime =< Ts.

%% @doc materialize_eager: apply updates in order without any checks
-spec materialize_eager(type(), snapshot(), [clocksi_payload()]) -> snapshot().
materialize_eager(_, Snapshot, []) ->
    Snapshot;
materialize_eager(Type, Snapshot, [Op|Rest]) ->
   case Op of
        {merge, State} ->
            NewSnapshot = Type:merge(Snapshot, State);
        {OpParam, Actor} ->
            {ok, NewSnapshot} = Type:update(OpParam, Actor, Snapshot)
    end,
    materialize_eager(Type, NewSnapshot, Rest).


-ifdef(TEST).

materializer_clocksi_sequential_test() ->
    GCounter = new(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Op1 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,2}, actor1},
                           commit_time = {1, 1}, txid = 1},
    Op2 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,1}, actor1},
                           commit_time = {1, 2}, txid = 2},
    Op3 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,1}, actor1},
                           commit_time = {1, 3}, txid = 3},
    Op4 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,2}, actor1},
                           commit_time = {1, 4}, txid = 4},

    Ops = [Op1,Op2,Op3,Op4],
    {ok, GCounter2, CommitTime2} = materialize(riak_dt_gcounter,
                                      GCounter, vectorclock:from_list([{1,3}]),
                                      Ops, ignore, ignore),
    ?assertEqual({4, {1,3}}, {riak_dt_gcounter:value(GCounter2), CommitTime2}),
    Snapshot=new(riak_dt_gcounter),
    {ok, Gcounter3, CommitTime3} = materialize(riak_dt_gcounter, Snapshot,
                                   vectorclock:from_list([{1,4}]),Ops, ignore),
    ?assertEqual({6, {1,4}}, {riak_dt_gcounter:value(Gcounter3), CommitTime3}),
    {ok, Gcounter4, CommitTime4} = materialize(riak_dt_gcounter, Snapshot,
                                   vectorclock:from_list([{1,7}]),Ops, ignore),
    ?assertEqual({6, {1,4}}, {riak_dt_gcounter:value(Gcounter4), CommitTime4}).

materializer_clocksi_concurrent_test() ->
    GCounter = new(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Op1 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,2}, actor1},
                           commit_time = {1, 1}, txid = 1},
    Op2 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,1}, actor1},
                           commit_time = {1, 2}, txid = 2},
    Op3 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,1}, actor1},
                           commit_time = {2, 1}, txid = 3},

    Ops = [Op1,Op2,Op3],
    {ok, GCounter2, CommitTime2} = materialize(riak_dt_gcounter,
                                      GCounter,
                                      vectorclock:from_list([{2,2},{1,2}]),
                                      Ops, ignore, ignore),
    ?assertEqual({4, {2,1}}, {riak_dt_gcounter:value(GCounter2), CommitTime2}),
    
    
    
    Snapshot=new(riak_dt_gcounter),
    {ok, Gcounter3, CommitTime3} = materialize(riak_dt_gcounter, Snapshot,
                                   vectorclock:from_list([{1,2}]),Ops, ignore),
    ?assertEqual({3, {1,2}}, {riak_dt_gcounter:value(Gcounter3), CommitTime3}),
    
    {ok, Gcounter4, CommitTime4} = materialize(riak_dt_gcounter, Snapshot,
                                   vectorclock:from_list([{2,1}]),Ops, ignore),
    ?assertEqual({1, {2,1}}, {riak_dt_gcounter:value(Gcounter4), CommitTime4}).

%% @doc Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    GCounter = new(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Ops = [],
    {ok, GCounter2, ignore} = materialize(riak_dt_gcounter, GCounter,
                                vectorclock:from_list([{1,1}]),
                                Ops, ignore, ignore),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter2)).
-endif.
