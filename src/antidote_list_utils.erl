%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
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


-module(antidote_list_utils).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-export([group_by_first/1, group_by/2, group_by/4, reduce/2, topsort/2, find_first/2, orddict_remove_keys/2, pmap/2, pmap_err/2]).


%% groups a list of key-value pairs by key
-spec group_by_first([{K, V}]) -> #{K => [V]}.
group_by_first(List) ->
    group_by(
        fun({K, _}) -> K end,
        fun({_, V}) -> [V] end,
        fun({_, V}, Xs) -> [V | Xs] end,
        List
    ).

% groups
-spec group_by(fun((E) -> K), [E]) -> #{K => [E]}.
group_by(F, List) ->
    group_by(F, fun(X) -> [X] end, fun(X, Xs) -> [X | Xs] end, List).

-spec group_by(fun((E) -> K), fun((E) -> V), fun((E, V) -> V), [E]) -> #{K => V}.
group_by(F, Init, Merge, List) ->
    lists:foldr(fun(X, M) ->
        K = F(X),
        maps:update_with(K, fun(L) -> Merge(X, L) end, Init(X), M)
    end, maps:new(), List).


-spec reduce(fun((E, E) -> E), [E]) -> E.
reduce(_, []) -> throw('cannot reduce empty list');
reduce(_, [X]) -> X;
reduce(M, [X, Y | Xs]) -> reduce(M, [M(X, Y) | Xs]).


topsort(_Cmp, []) -> [];
topsort(Cmp, Xs) ->
    % Min are all elements X from Xs, such that for all elements Y from Xs: not Y < X
    {Min, NotMin} = lists:partition(
        fun(X) ->
            lists:all(fun(Y) -> not Cmp(Y, X) end, Xs)
        end,
        Xs
    ),
    Min ++ topsort(Cmp, NotMin).


-spec find_first(fun((T) -> boolean()), [T]) -> error | {ok, T}.
find_first(Pred, []) when is_function(Pred) -> error;
find_first(Pred, [X | Xs]) ->
    case Pred(X) of
        true -> {ok, X};
        false -> find_first(Pred, Xs)
    end.

orddict_remove_keys(Dict, Keys) ->
    lists:foldl(fun(K, Acc) -> orddict:erase(K, Acc) end, Dict, Keys).

%% like lists:map, but each element is computed in its own process
-spec pmap(fun((A) -> B), [A]) -> [B].
pmap(F, List) ->
    Self = self(),
    Pids = [spawn_link(fun() -> Self ! {self(), F(X)} end) || X <- List],
    [receive {Pid, X} -> X end || Pid <- Pids].


%% like pmap, but the function may fail
%% Returns the first received error if any or a list of results.
-spec pmap_err(fun((A) -> {ok, B} | {error, Reason}), [A]) -> {ok, [B]} | {error, Reason}.
pmap_err(F, List) ->
    Self = self(),
    Pids = [spawn_link(fun() -> Self ! {self(), F(X)} end) || X <- List],
    Receive = fun
        Receive(0, Acc) -> {ok, Acc};
        Receive(N, Acc) ->
            receive
                {Pid, Msg} ->
                    case Msg of
                        {ok, Val} ->
                            Receive(N - 1, maps:put(Pid, Val, Acc));
                        {error, Reason} ->
                            {error, Reason};
                        Other ->
                            {error, {unhandled_message, Other}}
                    end
            end
    end,
    case Receive(length(Pids), #{}) of
        {error, Reason} ->
            {error, Reason};
        {ok, Acc} ->
            {ok, [maps:get(Pid, Acc) || Pid <- Pids]}
    end.


-ifdef(TEST).
group_by_first_test() ->
    M = group_by_first([{a, 1}, {b, 2}, {a, 3}, {a, 4}]),
    ?assertEqual(#{a => [1, 3, 4], b => [2]}, M).


pmap_test() ->
    ?assertEqual([1, 4, 9, 16], pmap(fun(X) -> X * X end, [1, 2, 3, 4])).


pmap_err1_test() ->
    ?assertEqual({ok, [1, 4, 9, 16]}, pmap_err(fun(X) -> {ok, X * X} end, [1, 2, 3, 4])).

pmap_err2_test() ->
    ?assertEqual({error, blub}, pmap_err(fun(3) -> {error, blub}; (X) -> {ok, X * X} end, [1, 2, 3, 4])).

-endif.
