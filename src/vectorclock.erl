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
  new/0,
  get_clock/1,
  get_clock_by_key/1,
  get_clock_of_dc/2,
  set_clock_of_dc/3,
  get_stable_snapshot/0,
  from_list/1,
  eq/2,
  lt/2,
  gt/2,
  keep_max/2,
  keep_min/2,
  le/2,
  ge/2,
  strict_ge/2,
  strict_le/2, max/1, min/1]).

-export_type([vectorclock/0]).

-spec new() -> vectorclock().
new() ->
    dict:new().

-spec get_clock_by_key(Key :: key()) -> {ok, vectorclock:vectorclock()} | {error, term()}.
get_clock_by_key(Key) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    Indexnode = hd(Preflist),
    try
        riak_core_vnode_master:sync_command(
          Indexnode, get_clock, vectorclock_vnode_master)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

-spec get_clock(Partition :: non_neg_integer())
               -> {ok, vectorclock()} | {error, term()}.
get_clock(Partition) ->
    Indexnode = {Partition, node()},
    try
        riak_core_vnode_master:sync_command(
           Indexnode, get_clock, vectorclock_vnode_master)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc get_stable_snapshot: Returns stable snapshot time
%% in the current DC. stable snapshot time is the snapshot available at
%% in all partitions
-spec get_stable_snapshot() -> {ok, vectorclock:vectorclock()}.
get_stable_snapshot() ->
    %% This is fine if transactions coordinators exists on the ring (i.e. they have access
    %% to riak core meta-data) otherwise will have to change this
    {ok,vectorclock_vnode:get_stable_snapshot()}.


get_clock_of_dc(Dcid, VectorClock) ->
    case dict:find(Dcid, VectorClock) of
        {ok, Value} ->
            {ok, Value};
        error ->
            {ok, 0}
    end.

set_clock_of_dc(DcId, Time, VectorClock) ->
    dict:update(DcId,
                fun(_Value) ->
                        Time
                end,
                Time,
                VectorClock).

from_list(List) ->
    dict:from_list(List).

max([]) -> dict:new();
max([V]) -> V;
max([V1,V2|T]) -> max([dict:merge(fun(_K, A, B) -> erlang:max(A, B) end, V1, V2)|T]).

min([]) -> dict:new();
min([V]) -> V;
min([V1,V2|T]) -> min([dict:merge(fun(_K, A, B) -> erlang:min(A, B) end, V1, V2)|T]).


keep_max(V1, V2) ->
    fold_all_keys(fun(A, B, DC, Acc) -> dict:store(DC, max(A, B), Acc) end, V1, V2).

keep_min(V1, V2) ->
    fold_all_keys(fun(A, B, DC, Acc) -> dict:store(DC, min(A, B), Acc) end, V1, V2).

fold_all_keys(F, V1, V2) ->
    AllDCs = dict:fetch_keys(V1) ++ dict:fetch_keys(V2),
    lists:foldl(fun(DC, Acc) ->
			{ok, A} = get_clock_of_dc(DC, V1),
			{ok, B} = get_clock_of_dc(DC, V2),
			F(A, B, DC, Acc)
		end, new(), AllDCs).

-spec for_all_keys(fun((non_neg_integer(), non_neg_integer()) -> boolean()), vectorclock(), vectorclock()) -> boolean().
for_all_keys(F, V1, V2) ->
  %% We could but do not care about duplicate DC keys - finding duplicates is not worth the effort
  AllDCs = dict:fetch_keys(V1) ++ dict:fetch_keys(V2),
  lists:all(fun(DC) -> F(get_clock_of_dc(DC, V1), get_clock_of_dc(DC, V2)) end, AllDCs).

-spec eq(vectorclock(), vectorclock()) -> boolean().
eq(V1, V2) -> for_all_keys(fun(A, B) -> A == B end, V1, V2).

-spec le(vectorclock(), vectorclock()) -> boolean().
le(V1, V2) -> for_all_keys(fun(A, B) -> A =< B end, V1, V2).

-spec ge(vectorclock(), vectorclock()) -> boolean().
ge(V1, V2) -> for_all_keys(fun(A, B) -> A >= B end, V1, V2).

-spec lt(vectorclock(), vectorclock()) -> boolean().
lt(V1, V2) -> for_all_keys(fun(A, B) -> A < B end, V1, V2).

-spec gt(vectorclock(), vectorclock()) -> boolean().
gt(V1, V2) -> for_all_keys(fun(A, B) -> A > B end, V1, V2).

-spec strict_ge(vectorclock(), vectorclock()) -> boolean().
strict_ge(V1,V2) -> ge(V1,V2) and (not eq(V1,V2)).

-spec strict_le(vectorclock(), vectorclock()) -> boolean().
strict_le(V1,V2) -> le(V1,V2) and (not eq(V1,V2)).

-ifdef(TEST).

vectorclock_test() ->
    V1 = vectorclock:from_list([{1,5},{2,4},{3,5},{4,6}]),
    V2 = vectorclock:from_list([{1,4}, {2,3}, {3,4},{4,5}]),
    V3 = vectorclock:from_list([{1,5}, {2,4}, {3,4},{4,5}]),
    V4 = vectorclock:from_list([{1,6},{2,3},{3,1},{4,7}]),
    V5 = vectorclock:from_list([{1,6},{2,7}]),
    ?assertEqual(gt(V1,V2), true),
    ?assertEqual(lt(V2,V1), true),
    ?assertEqual(gt(V1,V3), false),
    ?assertEqual(strict_ge(V1,V3), true),
    ?assertEqual(strict_ge(V1,V1), false),
    ?assertEqual(ge(V1,V4), false),
    ?assertEqual(le(V1,V4), false),
    ?assertEqual(eq(V1,V4), false),
    ?assertEqual(ge(V1,V5), false).

-endif.
