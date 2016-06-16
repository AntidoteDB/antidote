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
    get_stable_snapshot/0,
    get_scalar_stable_time/0,
    get_partition_snapshot/1,
    create_commit_vector_clock/3,
    from_list/1,
    new/0,
    eq/2,
    lt/2,
    gt/2,
    le/2,
    ge/2,
    find/2,
    strict_ge/2,
    strict_le/2,
    max/1,
    min/1,
    fold/3,
    store/3,
    fetch/2,
    erase/2,
    map/2,
    max_vc/2,
    min_vc/2]).

-export_type([vectorclock/0]).

-spec new() -> vectorclock().
new() ->
    orddict:new().

fold(F, Acc, [{Key,Val}|D]) ->
    orddict:fold(F, F(Key, Val, Acc), D);
fold(F, Acc, []) when is_function(F, 3) -> Acc.

find(Key, Orddict) ->
    orddict:find(Key, Orddict).

store(Key, Value, Orddict1) ->
    orddict:store(Key, Value, Orddict1).

fetch(Key, Orddict)->
    orddict:fetch(Key, Orddict).

erase(Key, Orddict1)->
    orddict:erase(Key, Orddict1).

map(Fun, Orddict1) ->
    orddict:map(Fun, Orddict1).

%% @doc get_stable_snapshot: Returns stable snapshot time
%% in the current DC. stable snapshot time is the snapshot available at
%% in all partitions
-spec get_stable_snapshot() -> {ok, snapshot_time()}.
get_stable_snapshot() ->
    case meta_data_sender:get_merged_data(stable) of
        undefined ->
            %% The snapshot isn't ready yet, need to wait for startup
            timer:sleep(10),
            get_stable_snapshot();
        SS ->
            case application:get_env(antidote, txn_prot) of
                {ok, Prot} when ((Prot == clocksi) orelse (Prot == physics))->
                    %% This is fine if transactions coordinators exists on the ring (i.e. they have access
                    %% to riak core meta-data) otherwise will have to change this
                    {ok, SS};
                {ok, gr} ->
                    %% For gentlerain use the same format as clocksi
                    %% But, replicate GST to all entries in the orddict
                    StableSnapshot = SS,
                    case orddict:size(StableSnapshot) of
                        0 ->
                            {ok, StableSnapshot};
                        _ ->
                            ListTime = vectorclock:fold(
                                fun(_Key, Value, Acc) ->
                                    [Value | Acc]
                                end, [], StableSnapshot),
                            GST = lists:min(ListTime),
                            {ok, orddict:map(
                                fun(_K, _V) ->
                                    GST
                                end,
                                StableSnapshot)}
                    end
            end
    end.

-spec get_partition_snapshot(partition_id()) -> snapshot_time().
get_partition_snapshot(Partition) ->
    case meta_data_sender:get_meta_dict(stable, Partition) of
        undefined ->
            %% The partition isn't ready yet, wait for startup
            timer:sleep(10),
            get_partition_snapshot(Partition);
        SS ->
            SS
    end.

%% Returns the minimum value in the stable vector snapshot time
%% Useful for gentlerain protocol.
-spec get_scalar_stable_time() -> {ok, non_neg_integer(), vectorclock()}.
get_scalar_stable_time() ->
    {ok, StableSnapshot} = get_stable_snapshot(),
    %% orddict:is_empty/1 is not available, hence using orddict:size/1
    %% to check whether it is empty
    case orddict:size(StableSnapshot) of
        0 ->
            %% This case occur when updates from remote replicas has not yet received
            %% or when there are no remote replicas
            %% Since with current setup there is no mechanism
            %% to distinguish these, we assume the second case
            Now = dc_utilities:now_microsec() - ?OLD_SS_MICROSEC,
            {ok, Now, StableSnapshot};
        _ ->
            %% This is correct only if stablesnapshot has entries for
            %% all DCs. Inorder to check that we need to configure the 
            %% number of DCs in advance, which is not possible now.
            ListTime = orddict:fold(
                fun(_Key, Value, Acc) ->
                    [Value | Acc]
                end, [], StableSnapshot),
            GST = lists:min(ListTime),
            {ok, GST, StableSnapshot}
    end.

-spec get_clock_of_dc(any(), vectorclock()) -> non_neg_integer().
get_clock_of_dc(Key, VectorClock) ->
    case orddict:find(Key, VectorClock) of
        {ok, Value} -> Value;
        error -> 0
    end.

-spec set_clock_of_dc(any(), non_neg_integer(), vectorclock()) -> vectorclock().
set_clock_of_dc(Key, Value, VectorClock) ->
    case is_atom(VectorClock) of
        true -> orddict:store(Key, Value, []);
        false -> orddict:store(Key, Value, VectorClock)
    end.

-spec create_commit_vector_clock(any(), non_neg_integer(), vectorclock()) -> vectorclock().
create_commit_vector_clock(Key, Value, VectorClock)->
set_clock_of_dc(Key, Value, VectorClock).

-spec from_list([{any(), non_neg_integer()}]) -> vectorclock().
from_list(List) ->
    orddict:from_list(List).

-spec max([vectorclock()]) -> vectorclock().
max([]) -> new();
max([V]) -> V;
max([V1, V2 | T]) -> max([merge(fun erlang:max/2, V1, V2) | T]).

-spec max_vc(vectorclock(), vectorclock()) -> vectorclock().
max_vc(V, V)-> V;
max_vc(V, ignore)-> V;
max_vc(V, undefined)-> V;
max_vc(ignore, V)-> V;
max_vc(undefined, V)-> V;
max_vc([], V)-> V;
max_vc(V, [])-> V;
max_vc(V1, V2) ->
    case ge(V1, V2) of
        true ->
            V1;
        false ->
            V2
    end.

-spec min_vc(vectorclock(), vectorclock()) -> vectorclock().
min_vc(V, ignore)-> V;
min_vc(ignore, V)-> V;
min_vc([], V)-> V;
min_vc(V, [])-> V;
min_vc(V1, V2) ->
    case ge(V1, V2) of
        true ->
            V2;
        false ->
            V1
    end.

-spec min([vectorclock()]) -> vectorclock().
min([]) -> new();
min([V]) -> V;
min([V1, V2 | T]) -> min([merge(fun erlang:min/2, V1, V2) | T]).

-spec merge(fun((non_neg_integer(), non_neg_integer()) -> non_neg_integer()), vectorclock(), vectorclock()) -> vectorclock().
merge(F, V1, V2) ->
    AllDCs = orddict:fetch_keys(V1) ++ orddict:fetch_keys(V2),
    Func = fun(DC) ->
        A = get_clock_of_dc(DC, V1),
        B = get_clock_of_dc(DC, V2),
        {DC, F(A, B)}
           end,
    from_list(lists:map(Func, AllDCs)).

-spec for_all_keys(fun((non_neg_integer(), non_neg_integer()) -> boolean()), vectorclock(), vectorclock()) -> boolean().
for_all_keys(F, V1, V2) ->
    %% We could but do not care about duplicate DC keys - finding duplicates is not worth the effort
    AllDCs = orddict:fetch_keys(V1) ++ orddict:fetch_keys(V2),
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

-spec lt(vectorclock(), vectorclock()) -> boolean().
lt(V1, V2) -> for_all_keys(fun(A, B) -> A < B end, V1, V2).

-spec gt(vectorclock(), vectorclock()) -> boolean().
gt(V1, V2) -> for_all_keys(fun(A, B) -> A > B end, V1, V2).

-spec strict_ge(vectorclock(), vectorclock()) -> boolean().
strict_ge(V1, V2) -> ge(V1, V2) and (not eq(V1, V2)).

-spec strict_le(vectorclock(), vectorclock()) -> boolean().
strict_le(V1, V2) -> le(V1, V2) and (not eq(V1, V2)).

-ifdef(TEST).

vectorclock_test() ->
    V1 = vectorclock:from_list([{1, 5}, {2, 4}, {3, 5}, {4, 6}]),
    V2 = vectorclock:from_list([{1, 4}, {2, 3}, {3, 4}, {4, 5}]),
    V3 = vectorclock:from_list([{1, 5}, {2, 4}, {3, 4}, {4, 5}]),
    V4 = vectorclock:from_list([{1, 6}, {2, 3}, {3, 1}, {4, 7}]),
    V5 = vectorclock:from_list([{1, 6}, {2, 7}]),
    ?assertEqual(gt(V1, V2), true),
    ?assertEqual(lt(V2, V1), true),
    ?assertEqual(gt(V1, V3), false),
    ?assertEqual(strict_ge(V1, V3), true),
    ?assertEqual(strict_ge(V1, V1), false),
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
