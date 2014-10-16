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
-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_snapshot/3,
         get_snapshot/4,
         update_snapshot/5,
         update_snapshot_eager/3]).

%% @doc Creates an empty CRDT
%%      Input: Type: The type of CRDT to create
%%      Output: The newly created CRDT
-spec create_snapshot(type()) -> term().
create_snapshot(Type) ->
    Type:new().


%% @doc Calls the internal function update_snapshot/6, with no TxId.
-spec update_snapshot(type(), snapshot(),
                      snapshot_time(),
                      [clocksi_payload()], tx_id:tx_id()) -> {ok,snapshot()} | {error, atom()}.
%update_snapshot(_Type, Snapshot, _SnapshotTime, []) ->
%    {ok, Snapshot};
update_snapshot(Type, Snapshot, SnapshotTime, Ops, TxId) ->
    update_snapshot(Type, Snapshot, SnapshotTime, Ops, TxId, ignore).


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
-spec update_snapshot(type(), snapshot(),
                      snapshot_time(),
                      [clocksi_payload()], txid(), {dc(),CommitTime::non_neg_integer()} | ignore) ->
                             {ok,snapshot(), {dc(),CommitTime::non_neg_integer()}} | {error, atom()}.
update_snapshot(_, Snapshot, _SnapshotTime, [], _TxId, CommitTime) ->
    {ok, Snapshot, CommitTime};

update_snapshot(Type, Snapshot, SnapshotTime, [Op|Rest], TxId, LastOpCommitTame) ->
    case Type == Op#clocksi_payload.type of
        true ->
            OpCommitTime=Op#clocksi_payload.commit_time,
            case (is_op_in_snapshot(OpCommitTime, SnapshotTime)
                  or (TxId =:= Op#clocksi_payload.txid)) of
                true ->
                    case Op#clocksi_payload.op_param of
                        {merge, State} ->
                            NewSnapshot = Type:merge(Snapshot, State),
                            update_snapshot(Type,
                                            NewSnapshot,
                                            SnapshotTime,
                                            Rest,
                                            TxId,
                                            OpCommitTime);
                        {Update, Actor} ->
                            case Type:update(Update, Actor, Snapshot) of
                                {ok, NewSnapshot} ->
                                    update_snapshot(Type,
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
                    update_snapshot(Type, Snapshot, SnapshotTime, Rest, TxId, LastOpCommitTame)
            end;
        false -> %% Op is not for this {Key, Type}
            update_snapshot(Type, Snapshot, SnapshotTime, Rest, TxId)
    end.

%% @doc Check whether an udpate is included in a snapshot
%%      Input: Dc = Datacenter Id
%%             CommitTime = local commit time of this update at DC
%%             SnapshotTime = Orddict of [{Dc, Ts}]
%%      Output: true or false
-spec is_op_in_snapshot({dc(),CommitTime::non_neg_integer()},
                        snapshot_time()) -> boolean().
is_op_in_snapshot({Dc, CommitTime}, SnapshotTime) ->
    case vectorclock:get_clock_of_dc(Dc, SnapshotTime) of
        {ok, Ts} ->
            CommitTime =< Ts;
        error  ->
            false
    end.

%% @doc update_snapshot_eager: apply updates in order without any checks
-spec update_snapshot_eager(type(), snapshot(), [clocksi_payload()]) -> snapshot().
update_snapshot_eager(_, Snapshot, []) ->
    Snapshot;
update_snapshot_eager(Type, Snapshot, [Op|Rest]) ->
    {OpParam, Actor} = Op,
    {ok, NewSnapshot} = Type:update(OpParam, Actor, Snapshot),
    update_snapshot_eager(Type, NewSnapshot, Rest).

%% @doc Materialize a CRDT from its logged operations.
%%      - First creates an empty CRDT
%%      - Second apply the corresponding logged operations
%%      - Finally, transform the CRDT state into its value.
%%      Input:  Type: The type of the CRDT
%%      SnapshotTime: Threshold for the operations to be applied, including the threshold.
%%      Ops: The list of operations to apply
%%      Output: The value of the CRDT after applying the operations
-spec get_snapshot(type(), snapshot_time(), [clocksi_payload()]) ->
                          {ok, snapshot()} | {error, atom()}.
get_snapshot(Type, SnapshotTime, Ops) ->
    Init = create_snapshot(Type),
    update_snapshot(Type, Init, SnapshotTime, Ops, ignore).

-spec get_snapshot(type(), snapshot_time(),
                   [clocksi_payload()], txid()) -> {ok, snapshot()} | {error, atom()}.
get_snapshot(Type, SnapshotTime, Ops, TxId) ->
    Init = create_snapshot(Type),
    update_snapshot(Type, Init, SnapshotTime, Ops, TxId).

-ifdef(TEST).

materializer_clocksi_sequential_test() ->
    GCounter = create_snapshot(riak_dt_gcounter),
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
    {ok, GCounter2, CommitTime2} = update_snapshot(riak_dt_gcounter,
                                      GCounter, vectorclock:from_list([{1,3}]),
                                      Ops, ignore, ignore),
    ?assertEqual({4, {1,3}}, {riak_dt_gcounter:value(GCounter2), CommitTime2}),
    {ok, Gcounter3, CommitTime3} = get_snapshot(riak_dt_gcounter,
                                   vectorclock:from_list([{1,4}]),Ops),
    ?assertEqual({6, {1,4}}, {riak_dt_gcounter:value(Gcounter3), CommitTime3}),
    {ok, Gcounter4, CommitTime4} = get_snapshot(riak_dt_gcounter,
                                   vectorclock:from_list([{1,7}]),Ops),
    ?assertEqual({6, {1,4}}, {riak_dt_gcounter:value(Gcounter4), CommitTime4}).

materializer_clocksi_concurrent_test() ->
    GCounter = create_snapshot(riak_dt_gcounter),
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
    {ok, GCounter2, CommitTime2} = update_snapshot(riak_dt_gcounter,
                                      GCounter,
                                      vectorclock:from_list([{2,2},{1,2}]),
                                      Ops, ignore, ignore),
    ?assertEqual({4, {2,1}}, {riak_dt_gcounter:value(GCounter2), CommitTime2}),
    {ok, Gcounter3, CommitTime3} = get_snapshot(riak_dt_gcounter,
                                   vectorclock:from_list([{1,2}]),Ops),
    ?assertEqual({3, {1,2}}, {riak_dt_gcounter:value(Gcounter3), CommitTime3}).

%% @doc Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    GCounter = create_snapshot(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Ops = [],
    {ok, GCounter2, ignore} = update_snapshot(riak_dt_gcounter, GCounter,
                                vectorclock:from_list([{1,1}]),
                                Ops, ignore, ignore),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter2)).
-endif.
