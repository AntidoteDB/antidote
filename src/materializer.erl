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
    materialize_eager/3,
    check_operations/1,
    check_operation/1,
    is_crdt/1]).

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


%% @doc Check that in a list of operations, all of them are correctly typed.
-spec check_operations(list()) -> ok | {error, {type_check, term()}}.
check_operations([]) ->
    ok;
check_operations([Op | Rest]) ->
    case check_operation(Op) of
        true ->
            check_operations(Rest);
        false ->
            {error, {type_check, Op}}
    end.

%% @doc Check that an operation is correctly typed.
-spec check_operation(term()) -> boolean().
check_operation(Op) ->
    case Op of
        {update, {_, Type, {OpParams, _Actor}}} ->
            (riak_dt:is_riak_dt(Type) or materializer:is_crdt(Type)) andalso
                Type:is_operation(OpParams);
        {read, {_, Type}} ->
            (riak_dt:is_riak_dt(Type) or materializer:is_crdt(Type));
        _ ->
            false
    end.


%% @doc Check that an atom is an op_based CRDT type.
%%      The list of op_based CRDTS is defined in antidote.hrl
-spec is_crdt(term()) -> boolean().
is_crdt(Term) ->
    is_atom(Term) andalso lists:member(Term, ?CRDTS).



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
    ?assertMatch({error, _}, check_operations(Operations2)),

    Type1 = crdt_bcounter,
    Key1 = bcounter2,
    Operations3 = [{update, {Key1, Type1, {{increment, 7}, r1}}}, {update,
        {Key1, Type1, {{increment, 5}, r2}}}, {read, {Key1, Type1}}],
    ?assertEqual(ok, check_operations(Operations3)),

    Type2 = crdt_rga,
    Key2 = rga,
    Operations4 = [{update,{Key2, Type2, {{addRight,a,0},r}}}],
    ?assertEqual(ok, check_operations(Operations4)),

    ?assertEqual(ok,check_operations([{update, {key_add, crdt_rga, {{remove, 0}, xxx}}}])).
    

is_crdt_test() ->
    ?assertEqual(true, is_crdt(crdt_orset)),
    ?assertEqual(true, is_crdt(crdt_rga)),
    ?assertEqual(false, is_crdt(whatever)).
-endif.
