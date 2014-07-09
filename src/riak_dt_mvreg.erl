%% -------------------------------------------------------------------
%%
%% riak_dt_mvreg: A DVVSet based multi value register
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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
%% An Multi-Value Register CRDT.
%% There are two kinds of updates: assign and propagate.  Assign is used
%% for a single replica of MVReg where updates are linealizable.
%% Propagate is used to propagate update from a replica to other
%% replicas. It is similar to the 'merge' operation of the state-based
%% specifiction of MVReg.  Detailed usage is explained later.
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end

-module(riak_dt_mvreg).
-behaviour(riak_dt).

-export([new/0, value/1, value/2, update/3, merge/2,
         equal/2, to_binary/1, from_binary/1, stats/1, stat/2]).
-export([parent_clock/2, update/4]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([mvreg/0, mvreg_op/0]).

%% TODO: make opaque
-type mvreg() :: [{term(), riak_dt_vclock:vclock()}].

-type mvreg_op() :: {assign, term(), non_neg_integer()}
                  | {assign, term()}
                  | {propagate, term(), term()}.

-type mv_q() :: timestamp.

%% @doc Create a new, empty `mvreg()'
-spec new() -> mvreg().
new() ->
    [{<<>>, riak_dt_vclock:fresh()}].

-spec init(term(), riak_dt_vclock:vclock()) -> mvreg().
init(Value, VC) ->
    [{Value, VC}].

-spec parent_clock(riak_dt_vclock:vclock(), mvreg()) -> mvreg().
parent_clock(_Clock, Reg) ->
    Reg.

%% @doc The values of this `mvreg()'. Multiple values can be returned,
%% since there can be diverged value in this register.
-spec value(mvreg()) -> [term()].
value(MVReg) ->
    [Val || {Val, _TS} <- MVReg].

%% @doc query for this `mvreg()' of its timestamp.
%% `timestamp' is the only query option.
-spec value(mv_q(), mvreg()) -> [term()].
value(timestamp, MVReg) ->
    [TS || {_Val, TS} <- MVReg].

%% @doc Assign `Value' to this register. The vector clock of this
%% register will be incremented by one for the corresponding `Actor'.
%% In case the register has multiple diverged values, firstly a vector
%% clock that dominates all of them is calculated, then incrementation
%% for `Actor' is applied.
%%
%% This kind of update is supposed to be linealizable so the operation
%% issuer does not need to provide the vector clock it has observed.
-spec update(mvreg_op(), term(), mvreg()) ->
                    {ok, mvreg()}.
update({assign, Value}, Actor, MVReg) ->
    VV = inc_vv(MVReg, Actor),
    NewMVReg = [{Value, VV}],
    {ok, NewMVReg};

%% @doc Assign a `Value' to the `mvreg()' associating the update with
%% time `TS', if `TS' is larger than the current timestamp for `Actor'.
update({assign, Value, TS}, Actor, MVReg) ->
    case larger_than(TS, Actor, MVReg) of
        true ->
            VV = inc_vv(MVReg, Actor),
            NewMVReg = init(Value, VV),
            update({assign, Value, TS}, Actor, NewMVReg);
        false ->
            {ok, MVReg}
    end;

%% @doc Propagate the `Value' of a `mvreg()' to other replicas.  The
%% `Value' and its vector clock `TS' is the parameter. It is assumed
%% that propagation only happens after one update is applied, therefore
%% `Value' is a single term and `TS' can only be one vector clock.  All
%% compatible vector clocks (being descendent of another) will be
%% merged; non-compatible vector clocks will be kept in a list.
%% Corresponding values of non-compatible vector clocks will also be
%% kept.
update({propagate, Value, TS}, _, MVReg ) ->
    case if_dominate(value(timestamp, MVReg), TS) of
        true ->
            {ok, MVReg};
        false ->
            NewMVReg = merge_to(MVReg, init(Value, TS)),
            {ok, NewMVReg}
    end.

update(Op, Actor, Reg, _Ctx) ->
    update(Op, Actor, Reg).

%% @doc Find a least-uppder bound for all non-compatible vector clocks
%% in MVReg (if there is any) and then increment timestamp for `Actor'
%% by one.
-spec inc_vv(mvreg(), term()) -> riak_dt_vclock:vclock().
inc_vv(MVReg, Actor) ->
    TSL = [TS || {_Val, TS}<- MVReg],
    [H|T] = TSL,
    MaxVC = lists:foldl(fun(VC, Acc) -> riak_dt_vclock:merge([VC, Acc]) end, H, T),
    NewVC = riak_dt_vclock:increment(Actor, MaxVC),
    NewVC.

%% @doc If `TS'(timestamp) is larger than all entries of `Actor' for the
%% MVReg.
-spec larger_than(pos_integer(), term(), mvreg()) -> boolean().
larger_than(_TS, _Actor, []) ->
    true;
larger_than(TS, Actor, [H|T]) ->
    {_Value, VC} = H,
    OldTS = riak_dt_vclock:get_counter(Actor, VC),
    if  TS > OldTS ->
        larger_than(TS, Actor, T);
    true ->
        false
    end.

%% @doc Merge the first `mvreg()' to the second `mvreg()'. Note that the
%% first `mvreg()' is local and can have multiple vector clocks, while
%% the second one is from remote and only has one vector clock, since
%% before propagating its multiple VCs should have been merged.
-spec merge_to(mvreg(), mvreg()) -> mvreg().
merge_to([], MVReg2) ->
    MVReg2;
merge_to([H|T], MVReg2) ->
    First = hd(MVReg2),
    {_, TS1} = H,
    {_, TS2} = First,
    D1 = riak_dt_vclock:dominates(TS2, TS1),
    case D1 of
        true ->
            merge_to(T, MVReg2);
        false ->
            merge_to(T, MVReg2++[H])
    end.

%% @doc If any vector clock in the first list dominates the second vector clock.
if_dominate([], _VC) ->
    false;
if_dominate([H|T], VC) ->
    case riak_dt_vclock:dominates(H, VC) of
        true ->
            true;
        false ->
            if_dominate(T, VC)
    end.


%% @doc Merge two `mvreg()'s to a single `mvreg()'. This is the Least Upper Bound
%% function described in the literature.
%% !!! Not implemented !!! Propagate is basically doing merging. 
-spec merge(mvreg(), mvreg()) -> mvreg().
merge(MVReg1, _MVReg2) ->
    MVReg1.

%% @doc Are two `mvreg()'s structurally equal? This is not `value/1' equality.
%% Two registers might represent the value `armchair', and not be `equal/2'. Equality here is
%% that both registers contain the same value and timestamp.
-spec equal(mvreg(), mvreg()) -> boolean().
equal(MVReg1, MVReg2) ->
    eq(lists:sort(MVReg1), lists:sort(MVReg2)).

eq([], []) ->
    true;
eq([H1|T1], [H2|T2]) ->
    {V1, TS1} = H1,
    {V2, TS2} = H2,
    VEqual = V1 =:= V2,
    TSEqual = riak_dt_vclock:equal(TS1, TS2),
    if VEqual andalso TSEqual ->
            eq(T1, T2);
        true ->
            false
    end;
eq(_, _) ->
    false.

-spec stats(mvreg()) -> [{atom(), non_neg_integer()}].
stats(MVReg) ->
    [{value_size, stat(value_size, MVReg)}].

-spec stat(atom(), mvreg()) -> non_neg_integer() | undefined.
stat(value_size, MVReg) ->
    Values = value(MVReg),
    TS = value(timestamp, MVReg),
    Size =  erlang:external_size(Values) + erlang:external_size(TS),
    Size;
stat(_, _) -> undefined. 

-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(DT_MVREG_TAG, 85).
-define(TAG, ?DT_MVREG_TAG).
-define(V1_VERS, 1).

%% @doc Encode an effecient binary representation of an `mvreg()'
%% Not working yet...
-spec to_binary(mvreg()) -> binary().
to_binary(MVReg) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(MVReg))/binary>>.

%% @doc Decode binary `mvreg()'
-spec from_binary(binary()) -> mvreg().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    binary_to_term(Bin).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

init_state() ->
    [{<<>>, []}].

%% @doc Check if `new()' returns an empty string and an empty vector
%% clock.
new_test() ->
    ?assertEqual(init_state(), new()).

%% @doc Check if `value()' properly returns a list of value or a list of
%% timestamp of MVReg.  Checks for an empty register, a register with
%% two values and a register with only one value.
value_test() ->
    Val1 = "the rain in spain falls mainly on the plane",
    Val2 = "there is no rain",
    VC0 = riak_dt_vclock:fresh(),
    VC1 = riak_dt_vclock:increment(actor1, VC0),
    VC2 = riak_dt_vclock:increment(actor2, VC0),
    MVReg0 = new(),
    MVReg1 = [{Val1, VC1}, {Val2, VC2}],
    MVReg2 = [{Val1, VC1}],
    ?assertEqual([<<>>], value(MVReg0)),
    ?assertEqual(lists:sort([Val1, Val2]), lists:sort(value(MVReg1))),
    ?assertEqual(lists:sort([VC1, VC2]), lists:sort(value(timestamp, MVReg1))),
    ?assertEqual([Val1], value(MVReg2)),
    ?assertEqual([VC1], value(timestamp, MVReg2)).

%% @doc Check if `equal()' works.
equal_test() ->
    %% Test equal for empty MVReg
    ?assert(equal(init_state(), new())),
    MVReg1 = [{value1, [{actor1, 2}, {actor2, 1}]}, {value2, [{actor4, 1}, {actor3, 2}]}],
    MVReg2 = [{value2, [{actor4, 1}, {actor3, 2}]}, {value1, [{actor2, 1}, {actor1, 2}]}],
    %%Test if different order does not matter.
    ?assert(equal(MVReg1, MVReg2)),
    MVReg3 = [{value1, [{actor1, 2}, {actor2, 1}]}],
    %% MVReg1 is superset of MVReg3
    ?assertNot(equal(MVReg1, MVReg3)),
    MVReg4 = [{value1, [{actor1, 2}, {actor2, 1}]}, {value2, [{actor4, 1}, {actor3, 2}]}, {value3, [{actor5, 2}]}],
    %% MVReg1 is subset of MVReg4
    ?assertNot(equal(MVReg1, MVReg4)),
    MVReg5 = [{value0, [{actor1, 1}, {actor2, 1}]}, {value2, [{actor4, 1}, {actor3, 2}]}],
    %% MVReg5 has a value different from MVReg1
    ?assertNot(equal(MVReg1, MVReg5)).

%% @doc Check if `merge_to()' works.
merge_to_test() ->
    VC = riak_dt_vclock:fresh(),
    VC0 = riak_dt_vclock:increment(actor0, VC),
    VC1 = riak_dt_vclock:increment(actor1, VC),
    %% Basic merge: merge two MVRegs that are not dominating any of them 
    MVReg = merge_to([{value0, VC0}], [{value1, VC1}]),
    ?assert(equal(lists:sort([{value1, VC1}, {value0, VC0}]), lists:sort(MVReg))),
    MVReg1 = [{value1, [{actor1, 2}, {actor2, 1}]}, {value2, [{actor4, 1}]}],
    MVReg2 = [{value3, [{actor1, 2}, {actor2, 1}, {actor4, 2}]}],
    %% Merge one to another MVReg that totally dominates it
    Result1 = merge_to(MVReg1, MVReg2),
    ?assert(equal(Result1, MVReg2)),
    %% Merge one to another that does not dominate it
    MVReg3 = [{value4, [{actor1, 1}, {actor2, 1}]}],
    Result2 = merge_to(MVReg1, MVReg3),
    ?assert(equal(Result2,  [{value1, [{actor1, 2}, {actor2, 1}]}, {value2, [{actor4, 1}]}, {value4, [{actor1, 1}, {actor2, 1}]}])),
    %% Merge one to another that dominates part of the left one
    MVReg4 = [{value5, [{actor4, 2}]}],
    Result3 = merge_to(MVReg1, MVReg4),
    ?assert(equal(Result3,  [{value1, [{actor1, 2}, {actor2, 1}]}, {value5, [{actor4, 2}]}])).

% @doc Check if `if_dominate()' works.
if_dominate_test() ->
    VC1 = [[{actor1, 2}, {actor2, 3}, {actor3,2}], [{actor1,3}, {actor2,1}, {actor4,2}]],
    VC2 = [{actor1, 1}],
    ?assert(if_dominate(VC1, VC2)),
    VC3 = [{actor1, 2}, {actor3,1}],
    ?assert(if_dominate(VC1, VC3)),
    VC4 = [{actor3, 1}, {actor4,1}],
    ?assertNot(if_dominate(VC1, VC4)).

% @doc Check if `update()' by assign without providing timestamp works.
basic_assign_test() ->
    MVReg0 = new(),
    VC0 = riak_dt_vclock:fresh(),
    VC1 = riak_dt_vclock:increment(actor0, VC0),
    VC2 = riak_dt_vclock:increment(actor0, VC1),
    {ok, MVReg1} = update({assign, value0}, actor0, MVReg0),
    ?assertEqual([{value0, VC1}], MVReg1),
    {ok, MVReg2} = update({assign, value1}, actor0, MVReg1),
    ?assertEqual([{value1, VC2}], MVReg2).

%% @doc Check if `update()' by assign with timestamp works.
update_assign_withts_test() ->
    MVReg0 = new(),
    VC0 = riak_dt_vclock:fresh(),
    VC1 = riak_dt_vclock:increment(actor1, VC0),
    VC2 = riak_dt_vclock:increment(actor1, VC1),
    VC3 = riak_dt_vclock:increment(actor1, VC2),
    VC4 = riak_dt_vclock:increment(actor1, VC3),
    VC5 = riak_dt_vclock:increment(actor2, VC4),
    VC6 = riak_dt_vclock:increment(actor2, VC5),
    %% Update a MVReg with a large timestamp. The timestmap of MVReg will be updated to given value.
    {ok, MVReg1} = update({assign, value1, 2}, actor1, MVReg0),
    ?assertEqual([{value1, VC2}], MVReg1),
    %% Update a MVReg with a lower timestamp than the current one; should have no effect.
    {ok, MVReg2} = update({assign, value0, 1}, actor1, MVReg1),
    ?assertEqual([{value1, VC2}], MVReg2),
    %% Test again with higher timestamp for actor1
    {ok, MVReg3} = update({assign, value2, 4}, actor1, MVReg2),
    ?assertEqual([{value2, VC4}], MVReg3),
    %% Test with actor2
    {ok, MVReg4} = update({assign, value3, 2}, actor2, MVReg3),
    ?assertEqual([{value3, VC6}], MVReg4).

%% @doc Update a MVReg with two different actors. Check if both actors are kept in the vector clock.
update_diff_actor_test() ->
    MVR0 = new(),
    VC0 = riak_dt_vclock:fresh(),
    VC1 = riak_dt_vclock:increment(actor1, VC0),
    VC2 = riak_dt_vclock:increment(actor2, VC1),
    {ok, MVR1} = update({assign, value1}, actor1, MVR0),
    {ok, MVR2} = update({assign, value2}, actor2, MVR1),
    Value = value(MVR2),
    TS = value(timestamp, MVR2),
    ?assertEqual([value2], Value),
    ?assertEqual([VC2], TS).

%% @doc Check if `update()' with propagate works.
propagate_test() ->
    MVReg1_0 = new(),
    VC0 = riak_dt_vclock:fresh(),
    VC1 = riak_dt_vclock:increment(actor1, VC0),
    VC2 = riak_dt_vclock:increment(actor2, VC0),
    {ok, MVReg1_1} = update({assign, value1}, actor1, MVReg1_0),
    %% Propagate a timestamp that is not compatible with the current ones
    {ok, MVRMerge1} = update({propagate, value2, VC2}, nothing, MVReg1_1),
    ?assertEqual(lists:sort([value1, value2]), lists:sort(value(MVRMerge1))),
    ?assertEqual(lists:sort([VC1, VC2]), lists:sort(value(timestamp, MVRMerge1))),
    VC12 = riak_dt_vclock:increment(actor2, VC1),
    %% Propagate a timestamp that dominates all current ones
    {ok, MVRMerge2} = update({propagate, value2, VC12}, nothing, MVRMerge1),
    ?assertEqual([value2], value(MVRMerge2)),
    ?assertEqual([VC12], value(timestamp, MVRMerge2)),
    %% Propagate a timestamp that is dominated by the current one
    {ok, MVRMerge3} = update({propagate, value3, VC1}, nothing, MVRMerge2),
    ?assertEqual([value2], value(MVRMerge3)),
    ?assertEqual([VC12], value(timestamp, MVRMerge3)).

%% @doc Check if a diverged MVReg merges all vector clocks after being updated.
update_assign_diverge_test() ->
    VC0 = riak_dt_vclock:fresh(),
    VC1 = riak_dt_vclock:increment(actor1, VC0),
    VC2 = riak_dt_vclock:increment(actor2, VC0),
    %% This represents a MVReg that have two concurrent assignment (one propagated).
    MVReg0 = [{value1, VC1}, {value2, VC2}],
    {ok, MVReg1} = update({assign, value3}, actor1, MVReg0),
    VC1_2 = riak_dt_vclock:increment(actor2, VC1),
    VC1_3 = riak_dt_vclock:increment(actor1, VC1_2),
    %% After updating, the MVReg should only have one vector clock,
    %% having the maximum count of actors from each vector clock.
    %% Update with actor1 (existing)
    ?assert(equal([{value3, VC1_3}], MVReg1)),
    {ok, MVReg2} = update({assign, value4}, actor3, MVReg0),
    VC1_4 = riak_dt_vclock:increment(actor3, VC1_2),
    %% Check when updated with actor3, which was not in MVReg
    ?assert(equal([{value4, VC1_4}], MVReg2)).

%% Do we need a merge? Anyway its functionality is very similar to propagate.
%merge_test() ->
%    MVReg1 = {old_value, 3},
%    MVReg2 = {new_value, 4},
%    ?assertEqual({<<>>, 0}, merge(new(), new())),
%    ?assertEqual({new_value, 4}, merge(MVReg1, MVReg2)),
%    ?assertEqual({new_value, 4}, merge(MVReg2, MVReg1)).

%% @doc Check if serialization (`to_binary()', `from_binary()') works.
roundtrip_bin_test() ->
    MVReg = new(),
    {ok, MVReg1} = update({assign, 2}, a1, MVReg),
    {ok, MVReg2} = update({assign, 4}, a2, MVReg1),
    {ok, MVReg3} = update({assign, 89}, a3, MVReg2),
    {ok, MVReg4} = update({assign, <<"this is a binary">>}, a4, MVReg3),
    Bin = to_binary(MVReg4),
    Decoded = from_binary(Bin),
    ?assert(equal(MVReg4, Decoded)).

%% @doc Check if stas return the correct size of MVReg.
stat_test() ->
    MVReg = new(),
    {ok, MVReg1} = update({assign, <<"abcd">>}, 1, MVReg),
    ?assertEqual([{value_size, 25}], stats(MVReg)),
    ?assertEqual([{value_size, 40}], stats(MVReg1)),
    ?assertEqual(40, stat(value_size, MVReg1)),
    ?assertEqual(undefined, stat(actor_count, MVReg1)).

-endif.
