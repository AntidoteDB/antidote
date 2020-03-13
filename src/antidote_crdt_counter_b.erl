%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.

%% @doc
%% An operation based implementation of the bounded counter CRDT.
%% This counter is able to maintain a non-negative value by
%% explicitly exchanging permissions to execute decrement operations.
%% All operations on this CRDT are monotonic and do not keep extra tombstones.
%% @end

-module(antidote_crdt_counter_b).

-behaviour(antidote_crdt).

-include("antidote_crdt.hrl").

%% Call backs
-export([ new/0,
          value/1,
          downstream/2,
          update/2,
          equal/2,
          to_binary/1,
          from_binary/1,
          is_operation/1,
          require_state_downstream/1,
          generate_downstream_check/4
        ]).

%% API
-export([localPermissions/2,
         permissions/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type antidote_crdt_counter_b() :: {orddict:orddict(), orddict:orddict()}.
-type antidote_crdt_counter_b_op() :: antidote_crdt_counter_b_anon_op() | antidote_crdt_counter_b_src_op().
-type antidote_crdt_counter_b_anon_op() :: {transfer, {pos_integer(), id(), id()}} |
    {increment, {pos_integer(), id()}} | {decrement, {pos_integer(), id()}}.
-type antidote_crdt_counter_b_src_op() :: {antidote_crdt_counter_b_anon_op(), id()}.
-type id() :: term(). %% A replica's identifier.

%% @doc Return a new, empty `antidote_crdt_counter_b()'.
-spec new() -> antidote_crdt_counter_b().
new() ->
    {orddict:new(), orddict:new()}.

%% @doc Return the available permissions of replica `Id' in a `antidote_crdt_counter_b()'.
-spec localPermissions(id(), antidote_crdt_counter_b()) -> non_neg_integer().
localPermissions(Id, {P, D}) ->
    Received = lists:foldl(
                 fun(
                   {_, V}, Acc) ->
                         Acc + V
                 end,
                 0, orddict:filter(
                      fun(
                        {_, To}, _) when To == Id ->
                              true;
                         (_, _) ->
                              false
                      end, P)),
    Granted  = lists:foldl(
                 fun
                     ({_, V}, Acc) ->
                         Acc + V
                 end, 0, orddict:filter(
                           fun
                               ({From, To}, _) when From == Id andalso To /= Id ->
                                   true;
                               (_, _) ->
                                   false
                           end, P)),
    case orddict:find(Id, D) of
        {ok, Decrements} ->
            Received - Granted - Decrements;
        error ->
            Received - Granted
    end.

%% @doc Return the total available permissions in a `antidote_crdt_counter_b()'.
-spec permissions(antidote_crdt_counter_b()) -> non_neg_integer().
permissions({P, D}) ->
    TotalIncrements = orddict:fold(
                        fun
                            ({K, K}, V, Acc) ->
                                V + Acc;
                            (_, _, Acc) ->
                                Acc
                        end, 0, P),
    TotalDecrements = orddict:fold(
                        fun
                            (_, V, Acc) ->
                                V + Acc
                        end, 0, D),
    TotalIncrements - TotalDecrements.

%% @doc Return the read value of a given `antidote_crdt_counter_b()', itself.
-spec value(antidote_crdt_counter_b()) -> antidote_crdt_counter_b().
value(Counter) -> Counter.

%% @doc Generate a downstream operation.
%% The first parameter is either `{increment, pos_integer()}' or `{decrement, pos_integer()}',
%% which specify the operation and amount, or `{transfer, pos_integer(), id()}'
%% that additionally specifies the target replica.
%% The second parameter is an `actor()' who identifies the source replica,
%% and the third parameter is a `antidote_crdt_counter_b()' which holds the current snapshot.
%%
%% Return a tuple containing the operation and source replica.
%% This operation fails and returns `{error, no_permissions}'
%% if it tries to consume resources unavailable to the source replica
%% (which prevents logging of forbidden attempts).
-spec downstream(antidote_crdt_counter_b_op(), antidote_crdt_counter_b()) -> {ok, term()} | {error, no_permissions}.
downstream({increment, {V, Actor}}, _Counter) when is_integer(V), V > 0 ->
    {ok, {{increment, V}, Actor}};
downstream({decrement, {V, Actor}}, Counter) when is_integer(V), V > 0 ->
    generate_downstream_check({decrement, V}, Actor, Counter, V);
downstream({transfer, {V, To, Actor}}, Counter) when is_integer(V), V > 0 ->
    generate_downstream_check({transfer, V, To}, Actor, Counter, V).

generate_downstream_check(Op, Actor, Counter, V) ->
    Available = localPermissions(Actor, Counter),
    if Available >= V -> {ok, {Op, Actor}};
       Available < V -> {error, no_permissions}
    end.

%% @doc Update a `antidote_crdt_counter_b()' with a downstream operation,
%% usually created with `generate_downstream'.
%%
%% Return the resulting `antidote_crdt_counter_b()' after applying the operation.
-spec update(term(), antidote_crdt_counter_b()) -> {ok, antidote_crdt_counter_b()}.
update({{increment, V}, Id}, Counter) ->
    increment(Id, V, Counter);
update({{decrement, V}, Id}, Counter) ->
    decrement(Id, V, Counter);
update({{transfer, V, To}, From}, Counter) ->
    transfer(From, To, V, Counter).

%% Add a given amount of permissions to a replica.
increment(Id, V, {P, D}) ->
    {ok, {orddict:update_counter({Id, Id}, V, P), D}}.

%% Consume a given amount of permissions from a replica.
decrement(Id, V, {P, D}) ->
    {ok, {P, orddict:update_counter(Id, V, D)}}.

%% Transfer a given amount of permissions from one replica to another.
transfer(From, To, V, {P, D}) ->
    {ok, {orddict:update_counter({From, To}, V, P), D}}.

%% doc Return the binary representation of a `antidote_crdt_counter_b()'.
-spec to_binary(antidote_crdt_counter_b()) -> binary().
to_binary(C) -> term_to_binary(C).

%% doc Return a `antidote_crdt_counter_b()' from its binary representation.
-spec from_binary(binary()) -> {ok, antidote_crdt_counter_b()}.
from_binary(<<B/binary>>) -> {ok, binary_to_term(B)}.

%% @doc The following operation verifies
%%      that Operation is supported by this particular CRDT.
-spec is_operation(term()) -> boolean().
is_operation(Operation) ->
    case Operation of
        {decrement, {Number, _Actor}} ->
            is_integer(Number) andalso (Number >= 0);
        {increment, {Number, _Actor}} ->
            is_integer(Number) andalso (Number >= 0);
        {transfer, {Number, _, _Actor}} ->
            is_integer(Number) andalso (Number >= 0);
        _ ->
            false
    end.

require_state_downstream(_) ->
     true.

equal(BCounter1, BCounter2) ->
    BCounter1 == BCounter2.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

%% Utility to generate and apply downstream operations.
apply_op(Op, Counter) ->
    {ok, OP_DS} = downstream(Op, Counter),
    {ok, NewCounter} = update(OP_DS, Counter),
    NewCounter.

%% Tests creating a new `antidote_crdt_counter_b()'.
new_test() ->
    ?assertEqual({[], []}, new()).

%% Tests increment operations.
increment_test() ->
    Counter0 = new(),
    Counter1 = apply_op({increment, {10, r1}}, Counter0),
    Counter2 = apply_op({increment, {5, r2}}, Counter1),
    %% Test replicas' values.
    ?assertEqual(5, localPermissions(r2, Counter2)),
    ?assertEqual(10, localPermissions(r1, Counter2)),
    %% Test total value.
    ?assertEqual(15, permissions(Counter2)).

%% Tests the function `localPermissions()'.
localPermisisons_test() ->
    Counter0 = new(),
    Counter1 = apply_op({increment, {10, r1}}, Counter0),
    %% Test replica with positive amount of permissions.
    ?assertEqual(10, localPermissions(r1, Counter1)),
    %% Test nonexistent replica.
    ?assertEqual(0, localPermissions(r2, Counter1)).

%% Tests decrement operations.
decrement_test() ->
    Counter0 = new(),
    Counter1 = apply_op({increment, {10, r1}}, Counter0),
    %% Test allowed decrement.
    Counter2 = apply_op({decrement, {6, r1}}, Counter1),
    ?assertEqual(4, permissions(Counter2)),
    %% Test nonexistent replica.
    ?assertEqual(0, localPermissions(r2, Counter1)),
    %% Test forbidden decrement.
    OP_DS = downstream({decrement, {6, r1}}, Counter2),
    ?assertEqual({error, no_permissions}, OP_DS).

%% Tests a more complex chain of increment and decrement operations.
decrement_increment_test() ->
    Counter0 = new(),
    Counter1 = apply_op({increment, {10, r1}}, Counter0),
    Counter2 = apply_op({decrement, {6, r1}}, Counter1),
    Counter3 = apply_op({increment, {6, r2}}, Counter2),
    %% Test several replicas (balance each other).
    ?assertEqual(10, permissions(Counter3)),
    %% Test forbidden permissions, when total is higher than consumed.
    OP_DS = downstream({decrement, {6, r1}}, Counter3),
    ?assertEqual({error, no_permissions}, OP_DS),
    %% Test the same operation is allowed on another replica with enough permissions.
    Counter4 = apply_op({decrement, {6, r2}}, Counter3),
    ?assertEqual(4, permissions(Counter4)).

%% Tests transferring permissions.
transfer_test() ->
    Counter0 = new(),
    Counter1 = apply_op({increment, {10, r1}}, Counter0),
    %% Test transferring permissions from one replica to another.
    Counter2 = apply_op({transfer, {6, r2, r1}}, Counter1),
    ?assertEqual(4, localPermissions(r1, Counter2)),
    ?assertEqual(6, localPermissions(r2, Counter2)),
    ?assertEqual(10, permissions(Counter2)),
    %% Test transference forbidden by lack of previously transfered resources.
    OP_DS = downstream({transfer, {5, r2, r1}}, Counter2),
    ?assertEqual({error, no_permissions}, OP_DS),
    %% Test transference enabled by previously transfered resources.
    Counter3 = apply_op({transfer, {5, r1, r2}}, Counter2),
    ?assertEqual(9, localPermissions(r1, Counter3)),
    ?assertEqual(1, localPermissions(r2, Counter3)),
    ?assertEqual(10, permissions(Counter3)).

%% Tests the function `value()'.
value_test() ->
    %% Test on `antidote_crdt_counter_b()' resulting from applying all kinds of operation.
    Counter0 = new(),
    Counter1 = apply_op({increment, {10, r1}}, Counter0),
    Counter2 = apply_op({decrement, {6, r1}}, Counter1),
    Counter3 = apply_op({transfer, {2, r2, r1}}, Counter2),
    %% Assert `value()' returns `antidote_crdt_counter_b()' itself.
    ?assertEqual(Counter3, value(Counter3)).

%% Tests serialization functions `to_binary()' and `from_binary()'.
binary_test() ->
    %% Test on `antidote_crdt_counter_b()' resulting from applying all kinds of operation.
    Counter0 = new(),
    Counter1 = apply_op({increment, {10, r1}}, Counter0),
    Counter2 = apply_op({decrement, {6, r1}}, Counter1),
    Counter3 = apply_op({transfer, {2, r2, r1}}, Counter2),
    %% Assert marshaling and unmarshaling holds the same `antidote_crdt_counter_b()'.
    B = to_binary(Counter3),
    ?assertEqual({ok, Counter3}, from_binary(B)).

is_operation_test() ->
    ?assertEqual(true, is_operation({transfer, {2, r2, r1}})),
    ?assertEqual(true, is_operation({increment, {50, r1}})),
    ?assertEqual(false, is_operation(increment)),
    ?assertEqual(true, is_operation({decrement, {50, r1}})),
    ?assertEqual(false, is_operation(decrement)),
    ?assertEqual(false, is_operation({anything, [1, 2, 3]})).

-endif.
