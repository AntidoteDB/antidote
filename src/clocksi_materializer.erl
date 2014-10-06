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
         update_snapshot/4,
         update_snapshot_eager/3]).

%% @doc Creates an empty CRDT
%%      Input: Type: The type of CRDT to create
%%      Output: The newly created CRDT
-spec create_snapshot(type()) -> term().
create_snapshot(Type) ->
    Type:new().


%% @doc Calls the internal function update_snapshot/5, with no TxId.
-spec update_snapshot(type(), snapshot(),
                      snapshot_time(),
                      [clocksi_payload()]) -> {ok,snapshot()} | {error, atom()}.
update_snapshot(_Type, Snapshot, _SnapshotTime, []) ->
    {ok, Snapshot};
update_snapshot(Type, Snapshot, SnapshotTime, [Op|Rest]) ->
    update_snapshot(Type, Snapshot, SnapshotTime, [Op|Rest], ignore).


%% @doc Applies the operation of a list to a CRDT. Only the
%%      operations with smaller timestamp than the specified
%%      are considered. Newer operations are discarded.
%%      Input:
%%      Type: The type of CRDT to create
%%      Snapshot: Current state of the CRDT
%%      SnapshotTime: Threshold for the operations to be applied.
%%      Ops: The list of operations to apply in causal order
%%      Output: The CRDT after appliying the operations
-spec update_snapshot(type(), snapshot(),
                      snapshot_time(),
                      [clocksi_payload()], txid() | ignore) ->
                             {ok,snapshot()} | {error, atom()}.
update_snapshot(_, Snapshot, _SnapshotTime, [], _TxId) ->
    {ok, Snapshot};

update_snapshot(Type, Snapshot, SnapshotTime, [Op|Rest], TxId) ->
    Type = Op#clocksi_payload.type,
    case (is_op_in_snapshot(Op#clocksi_payload.commit_time, SnapshotTime)
          or (TxId =:= Op#clocksi_payload.txid)) of
        true ->
            case Op#clocksi_payload.op_param of
                {merge, State} ->
                    NewSnapshot = Type:merge(Snapshot, State),
                    update_snapshot(Type,
                                    NewSnapshot,
                                    SnapshotTime,
                                    Rest,
                                    TxId);
                {Update, Actor} ->
                    case Type:update(Update, Actor, Snapshot) of
                        {ok, NewSnapshot} ->
                            update_snapshot(Type,
                                            NewSnapshot,
                                            SnapshotTime,
                                            Rest,
                                            TxId);
                        {error, Reason} ->
                            {error, Reason}
                    end
            end;
        false ->
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
-spec get_snapshot(type(), snapshot_time(), [clocksi_payload()]) -> {ok, snapshot()} | {error, atom()}.
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
    {ok, GCounter2} = update_snapshot(riak_dt_gcounter,
                                      GCounter, vectorclock:from_list([{1,3}]),
                                      Ops, ignore),
    ?assertEqual(4,riak_dt_gcounter:value(GCounter2)),
    {ok, Gcounter3} = get_snapshot(riak_dt_gcounter,
                                   vectorclock:from_list([{1,4}]),Ops),
    ?assertEqual(6,riak_dt_gcounter:value(Gcounter3)),
    {ok, Gcounter4} = get_snapshot(riak_dt_gcounter,
                                   vectorclock:from_list([{1,7}]),Ops),
    ?assertEqual(6,riak_dt_gcounter:value(Gcounter4)).

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
    {ok, GCounter2} = update_snapshot(riak_dt_gcounter,
                                      GCounter,
                                      vectorclock:from_list([{2,2},{1,2}]),
                                      Ops, ignore),
    ?assertEqual(4,riak_dt_gcounter:value(GCounter2)),
    {ok, Gcounter3} = get_snapshot(riak_dt_gcounter,
                                   vectorclock:from_list([{1,2}]),Ops),
    ?assertEqual(3,riak_dt_gcounter:value(Gcounter3)).
-endif.
