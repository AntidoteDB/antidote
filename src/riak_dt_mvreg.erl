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
%% An LWW Register CRDT.
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

%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([mvreg/0, mvreg_op/0]).

-opaque mvreg() :: [{term(), riak_dt_vclock:vclock()}].

-type mvreg_op() :: {assign, term(), non_neg_integer()}  | {assign, term()} | {propagate, term(), term()}.

-type mv_q() :: timestamp.

%% @doc Create a new, empty `mvreg()'
-spec new() -> mvreg().
new() ->
    [{<<>>, riak_dt_vclock:fresh()}].

-spec parent_clock(riak_dt_vclock:vclock(), mvreg()) -> mvreg().
parent_clock(_Clock, Reg) ->
    Reg.

%% @doc The single total value of a `gcounter()'.
-spec value(mvreg()) -> term().
value(MVReg) ->
    [Val || {Val, _TS} <- MVReg].

%% @doc query for this `mvreg()'.
%% `timestamp' is the only query option.
-spec value(mv_q(), mvreg()) -> non_neg_integer().
value(timestamp, MVReg) ->
    [TS || {_Val, TS} <- MVReg].

%% @doc Assign a `Value' to the `mvreg()'
%% associating the update with time `TS'
-spec update(mvreg_op(), term(), mvreg()) ->
                    {ok, mvreg()}.
update({assign, Value, TS}, Actor, MVReg) ->
    case dominate_ts(TS, Actor, MVReg) of 
        true ->
            VV = incVV(MVReg, Actor),
            NewMVReg = {Value, VV},
            {ok, NewMVReg};
        false ->
            {ok, MVReg}
    end;

%% For when users don't provide timestamps
%% don't think it is a good idea to mix server and client timestamps
update({assign, Value}, Actor, MVReg) ->
    VV = incVV(MVReg, Actor),
    NewMVReg = {Value, VV},
    {ok, NewMVReg};

update({propagate, Value, TS}, _, MVReg ) ->
    LocalTS = value(timestamp, MVReg),
    LocalDominate = riak_dt_vclock:dominate(LocalTS, TS),
    RemoteDominate = riak_dt_vclock:dominate(TS, LocalTS),
    case {LocalDominate, RemoteDominate} of 
        {true, false} ->
            {ok, MVReg};
        {false, true} ->
            NewMVReg = {Value, TS},
            {ok, NewMVReg};
        {false, false} ->
            NewMVReg = lists:append(MVReg, [{Value, TS}]),
            {ok, NewMVReg}
    end.

update(Op, Actor, Reg, _Ctx) ->
    update(Op, Actor, Reg).

incVV(MVReg, Actor) ->
    TSL = [TS || {_Val, TS}<- MVReg],
    MaxVC = getMax(TSL, riak_dt_vclock:fresh()),
    NewVC = riak_dt_vclock:increment(Actor, MaxVC),
    NewVC.

dominate_ts(TS, Actor, MVReg) ->
    OldTS = riak_dt_vclock:get_counter(Actor, MVReg),
    if OldTS+1 == TS ->
        true;
    true ->
        false
    end.

getMax([], VC) ->
    VC;
getMax([H|T], VC) ->
    NewVC = riak_dt_vclock:merge(H, VC),
    getMax(T, NewVC).   


%% @doc Merge two `mvreg()'s to a single `mvreg()'. This is the Least Upper Bound
%% function described in the literature.
-spec merge(mvreg(), mvreg()) -> mvreg().
merge(MVReg1, _MVReg2) ->
    MVReg1.
    %{Val1, TS1}.

%% @doc Are two `mvreg()'s structurally equal? This is not `value/1' equality.
%% Two registers might represent the value `armchair', and not be `equal/2'. Equality here is
%% that both registers contain the same value and timestamp.
-spec equal(mvreg(), mvreg()) -> boolean().
equal(_MVReg1, _MVReg2) ->
    true.
%equal(_, _) ->
%    false.

-spec stats(mvreg()) -> [{atom(), number()}].
stats(MVReg) ->
    [{value_size, stat(value_size, MVReg)}].

-spec stat(atom(), mvreg()) -> number() | undefined.
stat(value_size, MVReg) ->
    Values = value(MVReg),
    TS = value(timestamp, MVReg),
    Size =  erlang:external_size(Values) + erlang:external_size(TS),
    Size.
%stat(value_size, [], Acc) -> Acc. 

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_MVREG_TAG).
-define(V1_VERS, 1).

%% @doc Encode an effecient binary representation of an `mvreg()'
-spec to_binary(mvreg()) -> binary().
to_binary(MVReg) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(MVReg))/binary>>.

%% @doc Decode binary `mvreg()'
-spec from_binary(binary()) -> mvreg().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    riak_dt:from_binary(Bin).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

%% EQC generator
generate() ->
    ?LET({Op, Actor}, {gen_op(), char()},
         begin
             {ok, Lww} = riak_dt_mvreg:update(Op, Actor, riak_dt_mvreg:new()),
             Lww
         end).

init_state() ->
    [{<<>>, []}].

gen_op() ->
    ?LET(TS, largeint(), {assign, binary(), abs(TS)}).

update_expected(_ID, {assign, Val, TS}, {OldVal, OldTS}) ->
    case TS >= OldTS of
        true ->
            {Val, TS};
        false ->
            {OldVal, OldTS}
    end;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value({Val, _TS}) ->
    Val.
-endif.

new_test() ->
    ?assertEqual(init_state(), new()).

value_test() ->
    Val1 = "the rain in spain falls mainly on the plane",
    LWWREG1 = {Val1, 19090},
    LWWREG2 = new(),
    ?assertEqual(Val1, value(LWWREG1)),
    ?assertEqual(<<>>, value(LWWREG2)).

update_assign_test() ->
    LWW0 = new(),
    {ok, LWW1} = update({assign, value1, 2}, actor1, LWW0),
    {ok, LWW2} = update({assign, value0, 1}, actor1, LWW1),
    ?assertEqual({value1, 2}, LWW2),
    {ok, LWW3} = update({assign, value2, 3}, actor1, LWW2),
    ?assertEqual({value2, 3}, LWW3).

update_assign_ts_test() ->
    LWW0 = new(),
    {ok, LWW1} = update({assign, value0}, actr, LWW0),
    {ok, LWW2} = update({assign, value1}, actr, LWW1),
    ?assertMatch({value1, _}, LWW2).

merge_test() ->
    LWW1 = {old_value, 3},
    LWW2 = {new_value, 4},
    ?assertEqual({<<>>, 0}, merge(new(), new())),
    ?assertEqual({new_value, 4}, merge(LWW1, LWW2)),
    ?assertEqual({new_value, 4}, merge(LWW2, LWW1)).

equal_test() ->
    LWW1 = {value1, 1000},
    LWW2 = {value1, 1000},
    LWW3 = {value1, 1001},
    LWW4 = {value2, 1000},
    ?assertNot(equal(LWW1, LWW3)),
    ?assert(equal(LWW1, LWW2)),
    ?assertNot(equal(LWW4, LWW1)).

roundtrip_bin_test() ->
    LWW = new(),
    {ok, LWW1} = update({assign, 2}, a1, LWW),
    {ok, LWW2} = update({assign, 4}, a2, LWW1),
    {ok, LWW3} = update({assign, 89}, a3, LWW2),
    {ok, LWW4} = update({assign, <<"this is a binary">>}, a4, LWW3),
    Bin = to_binary(LWW4),
    Decoded = from_binary(Bin),
    ?assert(equal(LWW4, Decoded)).

query_test() ->
    LWW = new(),
    {ok, LWW1} = update({assign, value, 100}, a1, LWW),
    ?assertEqual(100, value(timestamp, LWW1)).

stat_test() ->
    LWW = new(),
    {ok, LWW1} = update({assign, <<"abcd">>}, 1, LWW),
    ?assertEqual([{value_size, 11}], stats(LWW)),
    ?assertEqual([{value_size, 15}], stats(LWW1)),
    ?assertEqual(15, stat(value_size, LWW1)),
    ?assertEqual(undefined, stat(actor_count, LWW1)).
-endif.
