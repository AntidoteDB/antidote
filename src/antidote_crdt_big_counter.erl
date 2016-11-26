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

%% antidote_crdt_big_counter: A convergent, replicated, operation based Big Counter
%% The state of this big counter is list of pairs where each pair is an integer 
%% and a related token.
%% Basically when the counter recieves {incrment, N} or {decrement, N} it generates
%% a pair {N, NewToken}. 
%% On update, all seen tokens are removed and the new pair is then added to the state.
%% This token keeps growing ("Big" Counter) but it useful as it allows the reset
%% functionaility, On reset(), all seen tokens are removed.
%% The name BigCounter has no semantic relation with the BigSet. It is only named this
%% way because it potentially grows big and is not state efficient.

-behaviour(antidote_crdt).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([ new/0,
          value/1,
          downstream/2,
          update/2,
          equal/2,
          to_binary/1,
          from_binary/1,
          is_operation/1,
          is_bottom/1,
          require_state_downstream/1
        ]).

-type uniqueToken() :: term().
-type state() :: [{integer(), uniqueToken()}].
-type op() ::
    {increment, integer()}
    | {decrement, integer()}
    | {reset, {}}.
-type effect() ::
      {integer(), uniqueToken()}
      | [uniqueToken()].

%% @doc Create a new, empty big counter
-spec new() -> state().
new() ->
    [].

%% @doc The value of this counter is equal to the sum of all the values
%% having tokens.
-spec value(state()) -> integer().
value(BigCounter) ->
    lists:sum([V || {V, _} <- BigCounter]).


-spec downstream(op(), state()) -> {ok, effect()}.
downstream(Op, BigCtr) ->
    Token = unique(),
    case Op of
        {increment, Value} when is_integer(Value) ->
            {ok, {Value, Token}};
        {decrement, Value} when is_integer(Value) ->
            {ok, {-Value, Token}};
        {reset, {}} ->
            Overridden = [Tok || {_, Tok} <- BigCtr],
            {ok, lists:sort(Overridden)}
    end.

-spec unique() -> uniqueToken().
unique() ->
    crypto:strong_rand_bytes(20).


-spec update(effect(), state()) -> {ok, state()}.
update({Value, Token}, BigCtr) ->
    % insert new value
    {ok, BigCtr ++ [{Value, Token}]};
update(Overridden, BigCtr) ->
  SortedBigCtr = lists:sort(fun({_, A}, {_, B}) -> A =< B end, BigCtr),
  BigCtr2 = [{V, T} || {V, T} <- SortedBigCtr, not lists:member(T, Overridden)],
  {ok, BigCtr2}.

-spec equal(state(), state()) -> boolean().
equal(BigCtr1, BigCtr2) ->
    BigCtr1 == BigCtr2.

-define(TAG, 85).
-define(V1_VERS, 1).

-spec to_binary(state()) -> binary().
to_binary(BigCtr) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(BigCtr))/binary>>.

%% @doc Decode binary
-spec from_binary(binary()) -> {ok, state()} | {error, term()}.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    {ok, riak_dt:from_binary(Bin)}.

is_bottom(BigCtr) ->
  BigCtr == new().

%% @doc The following operation verifies
%%      that Operation is supported by this particular CRDT.
-spec is_operation(term()) -> boolean().
is_operation({increment, Value}) when is_integer(Value) -> true;
is_operation({decrement, Value}) when is_integer(Value)-> true;
is_operation({reset, {}}) -> true;
is_operation(_) -> false.

require_state_downstream(Op) ->
  Op == {reset, {}}.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual(0, value(new())).

%% @doc test the correctness of increment without parameter.
update_increment_test() ->
    BigCnt0 = new(),
    {ok, Increment1} = downstream({increment, 5}, BigCnt0),
    {ok, BigCnt1} = update(Increment1, BigCnt0),
    {ok, Decrement1} = downstream({decrement, 2}, BigCnt1),
    {ok, BigCnt2} = update(Decrement1, BigCnt1),
    {ok, Increment2} = downstream({increment, 1}, BigCnt2),
    {ok, BigCnt3} = update(Increment2, BigCnt2),
    {ok, Reset1} = downstream({reset, {}}, BigCnt3),
    {ok, BigCnt4} = update(Reset1, BigCnt3),
    {ok, Decrement2} = downstream({decrement, 2}, BigCnt4),
    {ok, BigCnt5} = update(Decrement2, BigCnt4),
    ?assertEqual(0, value(BigCnt0)),
    ?assertEqual(5, value(BigCnt1)),
    ?assertEqual(3, value(BigCnt2)),
    ?assertEqual(4, value(BigCnt3)),
    ?assertEqual(0, value(BigCnt4)),
    ?assertEqual(-2, value(BigCnt5)).

-endif.
