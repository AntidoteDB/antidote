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
-module(materializer).
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([create_snapshot/1,
    update_snapshot/3,
    materialize_eager/3, check_operations/1]).

%% @doc Creates an empty CRDT
-spec create_snapshot(type()) -> snapshot().
create_snapshot(Type) ->
    Type:new().

%% @doc Applies an operation to a snapshot of a crdt. 
%%      This function yields an error if the crdt does not have a corresponding update operation.
-spec update_snapshot(type(), snapshot(), op()) -> {ok, snapshot()} | {error, reason()}.
update_snapshot(Type, Snapshot, Op) ->
    case Op of
        {merge, State} ->
            {ok, Type:merge(Snapshot, State)};
        {update, DownstreamOp} ->
            Type:update(DownstreamOp, Snapshot);
        _ ->
            lager:info("Unexpected log record: ~p for snapshot: ~p", [Op, Snapshot]),
            {error, unexpected_format}
    end.

%% @doc Applies updates in given order without any checks, errors are simply propagated.
-spec materialize_eager(type(), snapshot(), [op()]) -> snapshot() | {error, reason()}.
materialize_eager(_Type, Snapshot, []) ->
    Snapshot;
materialize_eager(Type, Snapshot, [Op | Rest]) ->
    case update_snapshot(Type, Snapshot, Op) of
        {error, Reason} ->
            {error, Reason};
        {ok, Result} ->
            materialize_eager(Type, Result, Rest)
    end.


-spec check_operations(list()) -> ok | {error, term()}.
check_operations(Operations) ->
    check_operations(ok, Operations).

-spec check_operations(ok, list()) -> ok | {error, term()}.
check_operations(ok, []) ->
    ok;
check_operations(ok, [Op | Rest]) ->
    try
        case Op of
            {update, {_, Type, {OpParams, Actor}}} ->
                Type:update(OpParams, Actor, Type:new()),
                check_operations(ok, Rest);
            {read, {_, Type}} ->
                Type:value(Type:new()),
                check_operations(ok, Rest);
            _ ->
                {error, {"Unknown operation format", Op}}
        end
    catch
        error : Reason ->
            {error, Reason}
    end.
-ifdef(TEST).

%% @doc Testing update with pn_counter.
update_pncounter_test() ->
    Counter = create_snapshot(crdt_pncounter),
    ?assertEqual(0, crdt_pncounter:value(Counter)),
    Op = {update, {{increment, 1}, actor1}},
    {ok, Counter2} = update_snapshot(crdt_pncounter, Counter, Op),
    ?assertEqual(1, crdt_pncounter:value(Counter2)).

%% @doc Testing update with gcounter and merge.
update_gcounter_test() ->
    Counter1 = riak_dt_gcounter:new(actor1, 5),
    Counter2 = riak_dt_gcounter:new(actor2, 2),
    ?assertEqual(5, riak_dt_gcounter:value(Counter1)),
    ?assertEqual(2, riak_dt_gcounter:value(Counter2)),
    {ok, Counter3} = update_snapshot(riak_dt_gcounter, Counter1, {merge, Counter2}),
    ?assertEqual(7, riak_dt_gcounter:value(Counter3)).


%% @doc Testing pn_counter with update log
materializer_counter_withlog_test() ->
    Counter = create_snapshot(crdt_pncounter),
    ?assertEqual(0, crdt_pncounter:value(Counter)),
    Ops = [{update, {{increment, 1}, actor1}},
        {update, {{increment, 1}, actor2}},
        {update, {{increment, 2}, actor3}},
        {update, {{increment, 3}, actor4}}],
    Counter2 = materialize_eager(crdt_pncounter, Counter, Ops),
    ?assertEqual(7, crdt_pncounter:value(Counter2)).

%% @doc Testing counter with empty update log
materializer_counter_emptylog_test() ->
    Counter = create_snapshot(crdt_pncounter),
    ?assertEqual(0, crdt_pncounter:value(Counter)),
    Ops = [],
    Counter2 = materialize_eager(crdt_pncounter, Counter, Ops),
    ?assertEqual(0, crdt_pncounter:value(Counter2)).

%% @doc Testing non-existing crdt
materializer_error_nocreate_test() ->
    ?assertException(error, undef, create_snapshot(bla)).

%% @doc Testing crdt with invalid update operation
materializer_error_invalidupdate_test() ->
    Counter = create_snapshot(crdt_pncounter),
    ?assertEqual(0, crdt_pncounter:value(Counter)),
    Ops = [{non_existing_op_type, {non_existing_op, actor1}}],
    ?assertEqual({error, unexpected_format}, materialize_eager(crdt_pncounter, Counter, Ops)).

%% @doc Testing that the function check_operations works properly
check_operations_test() ->
    Operations =
        [{read, {key1, riak_dt_gcounter}},
            {update, {key1, riak_dt_gcounter, {increment, a}}},
            {update, {key2, riak_dt_gset, {{add, elem}, a}}},
            {read, {key1, riak_dt_gcounter}}],
    ?assertEqual(ok, check_operations(Operations)),

    Operations2 = [{read, {key1, riak_dt_gcounter}},
        {update, {key1, riak_dt_gcounter, {{add, elem}, a}}},
        {update, {key2, riak_dt_gcounter, {increment, a}}},
        {read, {key1, riak_dt_gcounter}}],
    ?assertMatch({error, _}, check_operations(Operations2)).
-endif.
