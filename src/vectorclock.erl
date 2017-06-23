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
-module(vectorclock).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         get_clock_of_dc/2,
         set_clock_of_dc/3,
         from_list/1,
         new/0,
         eq/2,
         all_dots_smaller/2,
         all_dots_greater/2,
         le/2,
         ge/2,
         gt/2,
         lt/2,
         max/1,
         min/1]).

-export_type([vectorclock/0]).

-spec new() -> vectorclock().
new() ->
    dict:new().

-spec get_clock_of_dc(any(), vectorclock()) -> non_neg_integer().
get_clock_of_dc(Key, VectorClock) ->
  case dict:find(Key, VectorClock) of
    {ok, Value} -> Value;
    error -> 0
  end.

-spec set_clock_of_dc(any(), non_neg_integer(), vectorclock()) -> vectorclock().
set_clock_of_dc(Key, Value, VectorClock) ->
  dict:store(Key, Value, VectorClock).

-spec from_list([{any(), non_neg_integer()}]) -> vectorclock().
from_list(List) ->
    dict:from_list(List).

-spec max([vectorclock()]) -> vectorclock().
max([]) -> new();
max([V]) -> V;
max([V1, V2|T]) -> max([merge(fun erlang:max/2, V1, V2)|T]).

-spec min([vectorclock()]) -> vectorclock().
min([]) -> new();
min([V]) -> V;
min([V1, V2|T]) -> min([merge(fun erlang:min/2, V1, V2)|T]).

-spec merge(fun((non_neg_integer(), non_neg_integer()) -> non_neg_integer()), vectorclock(), vectorclock()) -> vectorclock().
merge(F, V1, V2) ->
  AllDCs = dict:fetch_keys(V1) ++ dict:fetch_keys(V2),
  Func = fun(DC) ->
    A = get_clock_of_dc(DC, V1),
    B = get_clock_of_dc(DC, V2),
    {DC, F(A, B)}
  end,
  from_list(lists:map(Func, AllDCs)).

-spec for_all_keys(fun((non_neg_integer(), non_neg_integer()) -> boolean()), vectorclock(), vectorclock()) -> boolean().
for_all_keys(F, V1, V2) ->
  %% We could but do not care about duplicate DC keys - finding duplicates is not worth the effort
  AllDCs = dict:fetch_keys(V1) ++ dict:fetch_keys(V2),
  Func = fun(DC) ->
    A = get_clock_of_dc(DC, V1),
    B = get_clock_of_dc(DC, V2),
    F(A, B)
  end,
  lists:all(Func, AllDCs).

-spec eq(vectorclock(), vectorclock()) -> boolean().
eq(V1, V2) -> for_all_keys(fun(A, B) -> A == B end, V1, V2).

-spec le(vectorclock(), vectorclock()) -> boolean().
le(V1, V2) -> for_all_keys(fun(A, B) -> A =< B end, V1, V2).

-spec ge(vectorclock(), vectorclock()) -> boolean().
ge(V1, V2) -> for_all_keys(fun(A, B) -> A >= B end, V1, V2).

-spec all_dots_smaller(vectorclock(), vectorclock()) -> boolean().
all_dots_smaller(V1, V2) -> for_all_keys(fun(A, B) -> A < B end, V1, V2).

-spec all_dots_greater(vectorclock(), vectorclock()) -> boolean().
all_dots_greater(V1, V2) -> for_all_keys(fun(A, B) -> A > B end, V1, V2).

-spec gt(vectorclock(), vectorclock()) -> boolean().
gt(V1, V2) -> ge(V1, V2) and (not eq(V1, V2)).

-spec lt(vectorclock(), vectorclock()) -> boolean().
lt(V1, V2) -> le(V1, V2) and (not eq(V1, V2)).

-ifdef(TEST).

vectorclock_test() ->
    V1 = vectorclock:from_list([{1, 5}, {2, 4}, {3, 5}, {4, 6}]),
    V2 = vectorclock:from_list([{1, 4}, {2, 3}, {3, 4}, {4, 5}]),
    V3 = vectorclock:from_list([{1, 5}, {2, 4}, {3, 4}, {4, 5}]),
    V4 = vectorclock:from_list([{1, 6}, {2, 3}, {3, 1}, {4, 7}]),
    V5 = vectorclock:from_list([{1, 6}, {2, 7}]),
    ?assertEqual(all_dots_greater(V1, V2), true),
    ?assertEqual(all_dots_smaller(V2, V1), true),
    ?assertEqual(all_dots_greater(V1, V3), false),
    ?assertEqual(gt(V1, V3), true),
    ?assertEqual(gt(V1, V1), false),
    ?assertEqual(ge(V1, V4), false),
    ?assertEqual(le(V1, V4), false),
    ?assertEqual(eq(V1, V4), false),
    ?assertEqual(ge(V1, V5), false).

vectorclock_max_test() ->
  V1 = vectorclock:from_list([{1, 5}, {2, 4}]),
  V2 = vectorclock:from_list([{1, 6}, {2, 3}]),
  V3 = vectorclock:from_list([{1, 3}, {3, 2}]),

  Expected12 = vectorclock:from_list([{1, 6}, {2, 4}]),
  Expected23 = vectorclock:from_list([{1, 6}, {2, 3}, {3, 2}]),
  Expected13 = vectorclock:from_list([{1, 5}, {2, 4}, {3, 2}]),
  Expected123 = vectorclock:from_list([{1, 6}, {2, 4}, {3, 2}]),
  Unexpected123 = vectorclock:from_list([{1, 5}, {2, 5}, {3, 5}]),

  ?assertEqual(eq(max([V1, V2]), Expected12), true),
  ?assertEqual(eq(max([V2, V3]), Expected23), true),
  ?assertEqual(eq(max([V1, V3]), Expected13), true),
  ?assertEqual(eq(max([V1, V2, V3]), Expected123), true),
  ?assertEqual(eq(max([V1, V2, V3]), Unexpected123), false).


vectorclock_min_test() ->
  V1 = vectorclock:from_list([{1, 5}, {2, 4}]),
  V2 = vectorclock:from_list([{1, 6}, {2, 3}]),
  V3 = vectorclock:from_list([{1, 3}, {3, 2}]),

  Expected12 = vectorclock:from_list([{1, 5}, {2, 3}]),
  Expected23 = vectorclock:from_list([{1, 3}]),
  Expected13 = vectorclock:from_list([{1, 3}]),
  Expected123 = vectorclock:from_list([{1, 3}]),
  Unexpected123 = vectorclock:from_list([{1, 3}, {2, 3}, {3, 2}]),

  ?assertEqual(eq(min([V1, V2]), Expected12), true),
  ?assertEqual(eq(min([V2, V3]), Expected23), true),
  ?assertEqual(eq(min([V1, V3]), Expected13), true),
  ?assertEqual(eq(min([V1, V2, V3]), Expected123), true),
  ?assertEqual(eq(min([V1, V2, V3]), Unexpected123), false).

-endif.
