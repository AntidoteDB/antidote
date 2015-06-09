%% -------------------------------------------------------------------
%%
%% crdt_pncounter: A convergent, replicated, operation based PN-Counter.
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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


%% @doc
%% An operation-based PN-Counter CRDT.
%% A PN-Counter is represented as two non-negative integers: one for increments
%% and one for decrements. The value of the counter is the difference between the
%% value of the positive counter and the value of the negative counter.
%%
%% As the data structure is operation-based, to issue an operation, one should 
%% firstly call `generate_downstream/3' to get the downstream version of the 
%% operation and then call `update/2'. 
%%
%% This file is adapted from riak_dt_pncounter, a state-based implementation of 
%% PN-Counter.
%% The changes are as follows:
%% 1. `generate_downstream/3' is added, as this is a requirement for op-based CRDTs.
%%    For PN-Counter, there is actually little difference between their op-based and 
%%    state-based implementation. Having `generate_downstream/3' is merely to keep 
%%    the interface of op-based CRDTs compatible with each other.
%% 2. `merge/2' is removed.
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. [http://hal.upmc.fr/inria-00555588/]
%%
%% @end

-module(crdt_pncounter).

-export([new/0, new/1, value/1, value/2, update/2, generate_downstream/3, to_binary/1, from_binary/1]).
-export([equal/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([pncounter/0, pncounter_op/0, binary_pncounter/0]).

-opaque pncounter()  :: {Inc::non_neg_integer(), Dec::non_neg_integer()}.
-type binary_pncounter() :: binary().
-type pncounter_op() :: {riak_dt_gcounter:gcounter_op(), binary()} | decrement_op().
-type decrement_op() :: decrement | {decrement, pos_integer(), binary()}.
-type pncounter_q()  :: positive | negative.

%% @doc Create a new, empty `pncounter()'
-spec new() -> pncounter().
new() ->
    {0,0}.

%% @doc Create a `pncounter()' with an initial `Value' for `Actor'.
-spec new(integer()) -> pncounter().
new(Value) when Value > 0 ->
    update({{increment, Value}, unique(1)}, new());
new(Value) when Value < 0 ->
    update({decrement, Value * -1, unique(1)}, new());
new(_Zero) ->
    new().

%% @doc The single, total value of a `pncounter()'
-spec value(pncounter()) -> integer().
value(PNCnt) ->
    {Inc, Dec} = PNCnt,
    Inc-Dec.

%% @doc query the parts of a `pncounter()'
%% valid queries are `positive' or `negative'.
-spec value(pncounter_q(), pncounter()) -> integer().
value(positive, PNCnt) ->
    {Inc, _Dec} = PNCnt,
    Inc;
value(negative, PNCnt) ->
    {_Inc, Dec} = PNCnt,
    Dec.

%% @doc Generate a downstream operation.
%% The first parameter is either `increment' or `decrement' or the two tuples 
%% `{increment, pos_integer()}' or `{decrement, pos_integer()}'. The second parameter
%% is the actor and the third parameter is the pncounter (both parameters are not actually used).
%% 
%% Returns a tuple representing the downstream operation and a unique identifier. The unique 
%% identifier is to ensure that the two same operations (e.g. two {increment, 1}) in a single 
%% transaction will not be ingored, which is caused by our implementation.
-spec generate_downstream(pncounter_op(), riak_dt:actor(), pncounter()) -> {ok, pncounter_op()}.
generate_downstream(increment, Actor, _PNCnt) ->
    {ok, {{increment, 1}, unique(Actor)}};
generate_downstream(decrement, Actor, _PNCnt) ->
    {ok, {{decrement, 1}, unique(Actor)}};
generate_downstream({increment, By}, Actor, _PNCnt) -> 
    {ok, {{increment, By}, unique(Actor)}};
generate_downstream({decrement, By}, Actor, _PNCnt) -> 
    {ok, {{decrement, By}, unique(Actor)}}.


%% @doc Update a `pncounter()'. The first argument is either the atom
%% `increment' or `decrement' or the two tuples `{increment, pos_integer()}' or
%% `{decrement, pos_integer()}', followed by a unique token, which is not used here. 
%% In the case of the former, the operation's amount
%% is `1'. Otherwise it is the value provided in the tuple's second element.
%% The 2nd argument is the `pncounter()' to update.
%%
%% returns the updated `pncounter()'
-spec update(pncounter_op(), pncounter()) -> {ok, pncounter()}.
update({{_IncrDecr, 0}, _Token}, PNCnt) ->
    {ok, PNCnt};
update({{increment, By}, _Token}, PNCnt) when is_integer(By), By > 0 ->
    {ok, increment_by(By, PNCnt)};
update({{increment, By}, _Token}, PNCnt) when is_integer(By), By < 0 ->
    {ok, decrement_by(-By, PNCnt)};
update({{decrement, By}, _Token}, PNCnt) when is_integer(By), By > 0 ->
    {ok, decrement_by(By, PNCnt)};
update({{decrement, By}, _Token}, PNCnt) when is_integer(By), By < 0 ->
    {ok, increment_by(-By, PNCnt)}.


%% @doc Compare if two `pncounter()' are equal. Only returns `true()' if both 
%% of their positive and negative entries are equal.
-spec equal(pncounter(), pncounter()) -> boolean().
equal({Inc1, Dec1}, {Inc2, Dec2}) ->
    case Inc1 of
        Inc2 ->
            case Dec1 of
                Dec2 ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.

-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(TAG, ?DT_PNCOUNTER_TAG).
-define(V1_VERS, 1).
-define(V2_VERS, 2).

-spec to_binary(pncounter()) -> binary_pncounter().
to_binary(PNCounter) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(PNCounter))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    binary_to_term(Bin).

% Priv
-spec increment_by(pos_integer(), pncounter()) -> pncounter().
increment_by(Increment, PNCnt) ->
    {Inc, Dec} = PNCnt,
    {Inc+Increment, Dec}.

-spec decrement_by(pos_integer(), pncounter()) -> pncounter().
decrement_by(Decrement, PNCnt) ->
    {Inc, Dec} = PNCnt,
    {Inc, Dec+Decrement}.

unique(_Actor) ->
    crypto:strong_rand_bytes(20).
%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({0,0}, new()).

%% @doc test the correctness of `value()' function
value_test() ->
    PNCnt1 = {4,0}, 
    PNCnt2 = {8,4},
    PNCnt3 = {4,4},
    ?assertEqual(4, value(PNCnt1)),
    ?assertEqual(4, value(PNCnt2)),
    ?assertEqual(0, value(PNCnt3)).

%% @doc test the correctness of increment without parameter.
update_increment_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({{increment, 1}, 1}, PNCnt0),
    {ok, PNCnt2} = update({{increment, 2}, 1}, PNCnt1),
    {ok, PNCnt3} = update({{increment, 1}, 1}, PNCnt2),
    ?assertEqual({4,0}, PNCnt3).

%% @doc test the correctness of increment by some numbers.
update_increment_by_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({{increment, 7}, 1}, PNCnt0),
    ?assertEqual({7,0}, PNCnt1).

%% @doc test the correctness of decrement.
update_decrement_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({{increment, 1}, 1}, PNCnt0),
    {ok, PNCnt2} = update({{increment, 2}, 1}, PNCnt1),
    {ok, PNCnt3} = update({{increment, 1}, 1}, PNCnt2),
    {ok, PNCnt4} = update({{decrement, 1}, 1}, PNCnt3),
    ?assertEqual({4,1}, PNCnt4).

update_decrement_by_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({{increment, 7}, 1}, PNCnt0),
    {ok, PNCnt2} = update({{decrement, 5}, 1}, PNCnt1),
    ?assertEqual({7, 5}, PNCnt2).

equal_test() ->
    PNCnt1 = {4,2},
    PNCnt2 = {2,0},
    PNCnt3 = {2,0}, 
    ?assertNot(equal(PNCnt1, PNCnt2)),
    ?assert(equal(PNCnt2, PNCnt3)).

binary_test() ->
    PNCnt1 = {4,2},
    BinaryPNCnt1 = to_binary(PNCnt1),
    PNCnt2 = from_binary(BinaryPNCnt1),
    ?assert(equal(PNCnt1, PNCnt2)).

-endif.
