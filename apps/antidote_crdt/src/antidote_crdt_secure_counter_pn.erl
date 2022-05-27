%% -------------------------------------------------------------------
%% Copyright <2020> <
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
%% -------------------------------------------------------------------

%% antidote_crdt_secure_counter_pn: A convergent, replicated, operation
%% based secure (using the Paillier cryptosystem) PN-Counter.

-module(antidote_crdt_secure_counter_pn).

-behaviour(antidote_crdt).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/0,
    value/1,
    downstream/2,
    update/2,
    equal/2,
    to_binary/1,
    from_binary/1,
    is_operation/1,
    require_state_downstream/1
]).

-type freshness() :: fresh | spoiled.
-type delta() :: integer().
-type nsquare() :: pos_integer().
-type state() :: {freshness(), integer()}.
-type op() :: {increment, {delta(), nsquare()}}.
-type effect() :: {delta(), nsquare()}.

%% @doc Create a new, empty 'antidote_crdt_secure_counter_pn'.
%%
%% The first element of the state tuple indicates whether the counter
%% is newly created (no increments done yet) or not. The second element
%% represents the encrypted (as specified by the Paillier scheme) total
%% value of the counter.
-spec new() -> state().
new() ->
    {fresh, 0}.

%% @doc Returns the encrypted (as specified by the Paillier scheme) total
%% value of a secure pn-counter.
-spec value(state()) -> integer().
value({_, Value}) when is_integer(Value) ->
    Value.

%% @doc Generate a downstream operation.
%%
%% The first parameter is a tuple of the form `{increment, {integer(), pos_integer()}}'.
%% Where the first integer represents the encrypted delta value by which the counter
%% will be incremented. The second integer represents the N squared value calculated
%% during the key generation phase of the Paillier cryptosystem.
%%
%% The value of N is part of the user's public key, it is ok for the server to know this
%% value. Invalid `NSquare' values (less than or equal to zero) are rejected, and a
%% downstream effect is not generated.
%%
%% The second parameter is the secure pn-counter, this parameter is not actually used.
-spec downstream(op(), state()) -> {ok, effect()}.
downstream({increment, {Delta, NSquare}}, _SecurePNCounter) when
    is_integer(Delta) and is_integer(NSquare) and (NSquare > 0)
->
    {ok, {Delta, NSquare}}.

%% @doc Updates a given secure pn-counter, incrementing it by a given
%% encrypted `Delta'. By incrementing we mean performing the homomorphic
%% addition of the counter value with the given delta. As described by
%% the Paillier cryptosystem, the homomorphic addition of two plaintexts
%% translates to the product of two ciphertexts modulo N squared.
%%
%% Returns the updated secure pn-counter.
-spec update(effect(), state()) -> {ok, state()}.
update({Delta, _NSquare}, {fresh, _Value}) ->
    {ok, {spoiled, Delta}};
update({Delta, NSquare}, {spoiled, Value}) ->
    {ok, {spoiled, (Value * Delta) rem NSquare}}.

%% @doc Compare if two secure counters are equal.
-spec equal(state(), state()) -> boolean().
equal(SecurePNCounter1, SecurePNCounter2) ->
    SecurePNCounter1 =:= SecurePNCounter2.

-spec to_binary(state()) -> binary().
to_binary(SecurePNCounter) ->
    term_to_binary(SecurePNCounter).

-spec from_binary(binary()) -> {ok, state()}.
from_binary(Bin) ->
    {ok, binary_to_term(Bin)}.

%% @doc The following function verifies that a given operation is supported by
%% this particular CRDT.
-spec is_operation(term()) -> boolean().
is_operation({increment, {Delta, NSquare}}) when
    is_integer(Delta) and is_integer(NSquare) and (NSquare > 0)
->
    true;
is_operation(_) ->
    false.

%% @doc Returns true if ?MODULE:downstream/2 needs the state of crdt
%% to generate downstream effect.
require_state_downstream(_) ->
    false.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

prepare_and_effect(Op, PNCounter) ->
    {ok, Downstream} = downstream(Op, PNCounter),
    update(Downstream, PNCounter).

new_test() ->
    ?assertEqual({fresh, 0}, new()).

value_test() ->
    ?assertEqual(0, value({fresh, 0})),
    ?assertEqual(4, value({spoiled, 4})).

update_test() ->
    Counter = new(),
    % Fresh counter becomes spoiled.
    {ok, Counter1} = prepare_and_effect({increment, {2, 1}}, Counter),
    ?assertEqual({spoiled, 2}, Counter1),
    % Spoiled counter stays spoiled and correctly updates value.
    {ok, Counter2} = prepare_and_effect({increment, {3, 36}}, Counter1),
    ?assertEqual({spoiled, 6}, Counter2).

reject_invalid_nsquare_test() ->
    Counter = new(),
    Operation1 = {increment, {1,  0}},
    Operation2 = {increment, {1, -1}},
    ?assertNot(is_operation(Operation1)),
    ?assertNot(is_operation(Operation2)),
    ?assertError(function_clause, downstream(Operation1, Counter)),
    ?assertError(function_clause, downstream(Operation2, Counter)).

equal_test() ->
    Counter1 = {fresh, 4},
    Counter2 = {fresh, 2},
    Counter3 = {fresh, 2},
    ?assertNot(equal(Counter1, Counter2)),
    ?assert(equal(Counter2, Counter3)),

    Counter4 = {spoiled, 5},
    Counter5 = {spoiled, 3},
    Counter6 = {spoiled, 3},
    ?assertNot(equal(Counter4, Counter5)),
    ?assert(equal(Counter5, Counter6)),

    ?assertNot(equal(Counter1, Counter4)).

binary_test() ->
    Counter1 = {spoiled, 4, 5},
    BinaryCounter1 = to_binary(Counter1),
    {ok, Counter2} = from_binary(BinaryCounter1),
    ?assert(equal(Counter1, Counter2)).

-endif.
