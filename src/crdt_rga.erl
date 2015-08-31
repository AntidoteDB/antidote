%% @doc
%%
%% An operation-based Replicated Growable Array CRDT.
%%
%% As the data structure is operation-based, to issue an operation, one should
%% first call `generate_downstream/3', to get the downstream version of the
%% operation, and then call `update/2'.
%%
%% It provides two operations: insert, which adds an element to the RGA, and delete,
%% which removes an element from the RGA.
%%
%% This implementation is based on the paper cited below.
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end
-module(crdt_rga).

-export([new/0, update/2, purge_tombstones/1]).

%% -ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% -endif.

-type vertex() :: {any(), any()}.

-type rga_op() :: {addRight, vertex(), atom()} | {remove, vertex()}.

-type rga_result() :: {ok, vertex(), rga()} | rga() .

-type rga() :: list().

-spec new() -> rga().
new() ->
    [].

-spec update(rga_op(), rga()) -> rga_result().
update({addRight, Vertex, Value}, Rga) ->
    recursive_insert(Vertex, Value, Rga, []);
update({remove, Vertex}, Rga) ->
    recursive_remove(Vertex, Rga, []).

recursive_insert(_, Value, [], []) ->
    Inserted = {Value, os:timestamp()},
    {ok, Inserted, [Inserted]};
recursive_insert(Vertex, Value, [Vertex | T], L) ->
    add_element({Value, os:timestamp()}, T, L ++ [Vertex]);
recursive_insert(Vertex, Value, [H | T], L) ->
    recursive_insert(Vertex, Value, T, L ++ [H]).

add_element({Value, TimeStamp}, [{Value1, TimeStamp1} | T], L) ->
    case TimeStamp >= TimeStamp1 of
        true ->
            {ok, {Value, TimeStamp}, L ++ [{Value, TimeStamp}] ++ [{Value1, TimeStamp1} | T]};
        _ ->
            add_element({Value, TimeStamp}, T, L ++ [{Value1, TimeStamp1}])
    end;
add_element(Insert, [], L) ->
    {ok, Insert, L ++ [Insert]}.

recursive_remove(_, [], L) ->
    L;
recursive_remove({Value, TimeStamp}, [{Value, TimeStamp} | T], L) ->
    L ++ [{deleted, TimeStamp}] ++ T;
recursive_remove(Vertex, [H | T], L) ->
    recursive_remove(Vertex, T, L ++ [H]).

purge_tombstones(Rga) ->
    lists:foldl(fun({Value, TimeStamp}, L) ->
        case Value == deleted of
            true    -> L;
            _       -> L ++ [{Value, TimeStamp}]
        end
    end, [], Rga).


-ifdef(TEST).
new_test() ->
    ?assertEqual([], new()).

insert_returns_same_element_test() ->
    L = new(),
    {ok, V1, L1} = update({addRight, {0, 0}, 1}, L),
    ?assertEqual([V1], L1).

consecutive_inserts_test() ->
    L = new(),
    {ok, V1, L1} = update({addRight, {0, 0}, 1}, L),
    {ok, V2, L2} = update({addRight, V1, 2}, L1),
    {ok, _, L3} = update({addRight, V2, 3}, L2),
    ?assertMatch([{1, _}, {2, _}, {3, _}], L3).

insert_in_the_middle_test() ->
    L = new(),
    {ok, V1, L1} = update({addRight, {0, 0}, 1}, L),
    {ok, _, L2} = update({addRight, V1, 2}, L1),
    {ok, _, L3} = update({addRight, V1, 3}, L2),
    ?assertMatch([{1, _}, {3, _}, {2, _}], L3).

remove_first_element_test() ->
    L = new(),
    {ok, V1, L1} = update({addRight, {0, 0}, 1}, L),
    {ok, V2, L2} = update({addRight, V1, 2}, L1),
    {ok, _, L3} = update({addRight, V2, 3}, L2),
    L4 = update({remove, V1}, L3),
    ?assertMatch([{deleted, _}, {2, _}, {3, _}], L4).

remove_middle_element_test() ->
    L = new(),
    {ok, V1, L1} = update({addRight, {0, 0}, 1}, L),
    {ok, V2, L2} = update({addRight, V1, 2}, L1),
    {ok, _, L3} = update({addRight, V2, 3}, L2),
    L4 = update({remove, V2}, L3),
    ?assertMatch([{1, _}, {deleted, _}, {3, _}], L4).

remove_last_element_test() ->
    L = new(),
    {ok, V1, L1} = update({addRight, {0, 0}, 1}, L),
    {ok, V2, L2} = update({addRight, V1, 2}, L1),
    {ok, V3, L3} = update({addRight, V2, 3}, L2),
    L4 = update({remove, V3}, L3),
    ?assertMatch([{1, _}, {2, _}, {deleted, _}], L4).
-endif.



