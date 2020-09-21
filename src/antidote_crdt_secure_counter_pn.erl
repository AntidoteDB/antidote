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
-type nsquare() :: integer().
-type state() :: {freshness(), integer(), nsquare()}.
-type op() :: {increment, delta()} | {increment, {delta(), nsquare()}}.
-type effect() :: delta() | {delta(), nsquare()}.

%% @doc Create a new, empty 'antidote_crdt_secure_counter_pn'
-spec new() -> state().
new() ->
    {fresh, 0, 0}.

%% @doc The single, total value of a secure pn-counter.
-spec value(state()) -> integer().
value({_, Value, _}) when is_integer(Value) ->
    Value.

%% @doc Generate a downstream operation.
%% The first parameter is either a tuple of the form `{increment, integer()}'
%% or a tuple of the form `{increment, {integer(), integer()}}'. The first
%% integer represents the delta value by which the counter will be incremented,
%% while the second integer is the n squared value used by the Paillier
%% cryptosystem. The second parameter is the secure pn-counter, this parameter
%% is not actually used.
-spec downstream(op(), state()) -> {ok, effect()}.
downstream({increment, {Delta, NSquare}}, _SecurePNCounter)
    when is_integer(Delta) and is_integer(Delta)
->
    {ok, {Delta, NSquare}};
downstream({increment, Delta}, _SecurePNCounter) when is_integer(Delta) ->
    {ok, Delta}.

%% @doc Updates a given secure pn-counter, incrementing it by a given `Delta'.
%% By incrementing we mean performing the homomorphic addition of the counter
%% value with the given delta. As described by the Paillier cryptosystem, the
%% homomorphic addition of plaintexts translates to the product of two
%% ciphertexts modulo n squared.
%%
%% A value for `NSquare` may also be provided, if it isn't, the one in the
%% counter state will be used.
%%
%% Returns the updated secure pn-counter.
-spec update(effect(), state()) -> {ok, state()}.
update({Delta, NSquare}, {fresh, _, _}) ->
    {ok, {spoiled, Delta, NSquare}};
update({Delta, NSquare}, {spoiled, Value, _}) ->
    {ok, {spoiled, (Value * Delta) rem NSquare, NSquare}};
update(Delta, {fresh, _, NSquare}) ->
    {ok, {spoiled, Delta, NSquare}};
update(Delta, {spoiled, Value, NSquare}) ->
    {ok, {spoiled, (Value * Delta) rem NSquare, NSquare}}.

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
is_operation({increment, {Delta, NSquare}}) when is_integer(Delta) and is_integer(NSquare) -> true;
is_operation({increment, Delta}) when is_integer(Delta) -> true;
is_operation(_) -> false.

%% @doc Returns true if ?MODULE:downstream/2 needs the state of crdt
%% to generate downstream effect.
require_state_downstream(_) ->
    false.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% @priv
prepare_and_effect(Op, PNCounter) ->
    {ok, Downstream} = downstream(Op, PNCounter),
    update(Downstream, PNCounter).

new_test() ->
    ?assertEqual({fresh, 0, 0}, new()).

value_test() ->
    ?assertEqual(3, value({fresh, 3, 2})),
    ?assertEqual(4, value({spoiled, 4, 2})).

%% @doc Tests wether the secure pn-counter is correctly updated.
update_test() ->
    Counter = new(),
    % Fresh counter without a given n squared.
    {ok, Counter1} = prepare_and_effect({increment, 1}, Counter),
    ?assertEqual({spoiled, 1, 0}, Counter1),
    % Fresh counter with a given n squared.
    {ok, Counter2} = prepare_and_effect({increment, {1, 1}}, Counter),
    ?assertEqual({spoiled, 1, 1}, Counter2),
    % Spoiled counter.
    {ok, Counter3} = prepare_and_effect({increment, 2}, Counter2),
    ?assertEqual({spoiled, 0, 1}, Counter3),
    {ok, Counter4} = prepare_and_effect({increment, {2, 3}}, Counter2),
    ?assertEqual({spoiled, 2, 3}, Counter4).

equal_test() ->
    Counter1 = {fresh, 4, 0},
    Counter2 = {fresh, 2, 0},
    Counter3 = {fresh, 2, 0},
    ?assertNot(equal(Counter1, Counter2)),
    ?assert(equal(Counter2, Counter3)).

binary_test() ->
    Counter1 = {spoiled, 4, 5},
    BinaryCounter1 = to_binary(Counter1),
    {ok, Counter2} = from_binary(BinaryCounter1),
    ?assert(equal(Counter1, Counter2)).

-endif.
