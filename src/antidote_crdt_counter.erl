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

%% antidote_crdt_pncounter: A convergent, replicated, operation
%% based PN-Counter

-module(antidote_crdt_counter).

-behaviour(antidote_crdt).

-include("antidote_crdt.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([ new/0,
          new/1,
          value/1,
          downstream/2,
          update/2,
          equal/2,
          to_binary/1,
          from_binary/1,
          is_operation/1,
          require_state_downstream/1
        ]).

%% @doc Create a new, empty 'pncounter()'
new() ->
    0.

%% @doc Create 'pncounter()' with initial value
-spec new(integer()) -> pncounter().
new(Value) when is_integer(Value) ->
    Value;
new(_) ->
    new().

%% @doc The single, total value of a `pncounter()'
-spec value(pncounter()) -> integer().
value(PNCnt) when is_integer(PNCnt) ->
    PNCnt.

%% @doc Generate a downstream operation.
%% The first parameter is either `increment' or `decrement' or the two tuples
%% `{increment, pos_integer()}' or `{decrement, pos_integer()}'. The second parameter
%%  is the pncounter (this parameter is not actually used).
-spec downstream(pncounter_update(), pncounter()) -> {ok, pncounter_effect()}.
downstream(increment, _PNCnt) ->
    {ok, 1};
downstream(decrement, _PNCnt) ->
    {ok, -1};
downstream({increment, By}, _PNCnt) when is_integer(By) ->
    {ok, By};
downstream({decrement, By}, _PNCnt) when is_integer(By) ->
    {ok, -By}.

%% @doc Update a `pncounter()'. The first argument is either the atom
%% `increment' or `decrement' or the two tuples `{increment, pos_integer()}' or
%% `{decrement, pos_integer()}'.
%% In the case of the former, the operation's amount
%% is `1'. Otherwise it is the value provided in the tuple's second element.
%% The 2nd argument is the `pncounter()' to update.
%%
%% returns the updated `pncounter()'
-spec update(pncounter_effect(), pncounter()) -> {ok, pncounter()}.
update(N, PNCnt) ->
    {ok, PNCnt + N}.

%% @doc Compare if two `pncounter()' are equal. Only returns `true()' if both
%% of their positive and negative entries are equal.
-spec equal(pncounter(), pncounter()) -> boolean().
equal(PNCnt1, PNCnt2) ->
    PNCnt1 =:= PNCnt2.

-spec to_binary(pncounter()) -> binary().
to_binary(PNCounter) ->
    term_to_binary(PNCounter).

from_binary(Bin) ->
    %% @TODO something smarter
    {ok, binary_to_term(Bin)}.

%% @doc The following operation verifies
%%      that Operation is supported by this particular CRDT.
-spec is_operation(term()) -> boolean().
is_operation(increment) -> true;
is_operation(decrement) -> true;
is_operation({increment, By}) when is_integer(By) -> true;
is_operation({decrement, By}) when is_integer(By)-> true;
is_operation(_) -> false.

%% @doc Returns true if ?MODULE:downstream/2 needs the state of crdt
%%      to generate downstream effect
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
    ?assertEqual(0, new()).

%% @doc test the correctness of `value()' function
value_test() ->
    PNCnt = 4,
    ?assertEqual(4, value(PNCnt)).

%% @doc test the correctness of increment without parameter.
update_increment_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = prepare_and_effect({increment, 1}, PNCnt0),
    {ok, PNCnt2} = prepare_and_effect({increment, 2}, PNCnt1),
    {ok, PNCnt3} = prepare_and_effect({increment, 1}, PNCnt2),
    ?assertEqual(4, value(PNCnt3)).

%% @doc test the correctness of increment by some numbers.
update_increment_by_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = prepare_and_effect({increment, 7}, PNCnt0),
    ?assertEqual(7, value(PNCnt1)).

%% @doc test the correctness of decrement.
update_decrement_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = prepare_and_effect({increment, 1}, PNCnt0),
    {ok, PNCnt2} = prepare_and_effect({increment, 2}, PNCnt1),
    {ok, PNCnt3} = prepare_and_effect({increment, 1}, PNCnt2),
    {ok, PNCnt4} = prepare_and_effect({decrement, 1}, PNCnt3),
    ?assertEqual(3, value(PNCnt4)).

update_negative_params_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = prepare_and_effect({increment, -7}, PNCnt0),
    {ok, PNCnt2} = prepare_and_effect({decrement, -5}, PNCnt1),
    ?assertEqual(-2, value(PNCnt2)).

equal_test() ->
    PNCnt1 = 4,
    PNCnt2 = 2,
    PNCnt3 = 2,
    ?assertNot(equal(PNCnt1, PNCnt2)),
    ?assert(equal(PNCnt2, PNCnt3)).

binary_test() ->
    PNCnt1 = 4,
    BinaryPNCnt1 = to_binary(PNCnt1),
    {ok, PNCnt2} = from_binary(BinaryPNCnt1),
    ?assert(equal(PNCnt1, PNCnt2)).

-endif.
