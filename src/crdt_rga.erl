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

-export([new/0, update/2, purge_tombstones/1, generate_downstream/3]).

-export_type([rga/0, rga_op/0, rga_downstream_op/0]).

%% -ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% -endif.

-type vertex() :: {atom(), any(), number()}.

-type rga_downstream_op() :: {addRight, vertex(), vertex()} | {remove, vertex()}.

-type rga_op() :: {addRight, any(), number()} | {remove, number()}.

-type rga_result() :: {ok, rga}.

-type rga() :: list().

-type actor() :: riak_dt:actor().

-spec new() -> rga().
new() ->
    [].

%% @doc generate downstream operations.
%% If the operation is addRight, generates a unique token for the new element.
%% If the operation is remove, fetches the vertex of the element to be removed.
-spec generate_downstream(rga_op(), actor(), rga()) -> {ok, rga_downstream_op()}.
generate_downstream({addRight, Elem, Position}, _Actor, Rga) ->
    case length(Rga) of
        0 -> {ok, {addRight, {ok, 0, 0}, {ok, Elem, unique()}}};
        _ -> {ok, {addRight, lists:nth(Position, Rga), {ok, Elem, unique()}}}
    end;
generate_downstream({remove, Position}, _Actor, Rga) ->
    {ok, {remove, lists:nth(Position, Rga)}}.

%% @doc This method takes in an rga operation and an rga to perform the operation.
%% It returns either the added Vertex and the new rga in case of an insert, and the new rga in the case of a remove.
-spec update(rga_downstream_op(), rga()) -> rga_result().
update({addRight, RightVertex, NewVertex}, Rga) ->
    recursive_insert(RightVertex, NewVertex, Rga, []);
update({remove, Vertex}, Rga) ->
    recursive_remove(Vertex, Rga, []).

%% Private
%% @doc recursively looks for the Vertex where the new element should be put to the right of.
recursive_insert(_, NewVertex, [], []) ->
    {ok, [NewVertex]};
recursive_insert(RightVertex, NewVertex, [RightVertex | T], L) ->
    add_element(NewVertex, T, [RightVertex | L]);
recursive_insert(RightVertex, NewVertex, [H | T], L) ->
    recursive_insert(RightVertex, NewVertex, T, [H | L]).

%% Private
%% @doc the place for the insertion has been found, so now the UIDs are compared to see where to insert.
add_element({Status, Value, UID}, [{Status1, Value1, UID1} | T], L) ->
    case UID >= UID1 of
        true ->
            {ok, lists:reverse(L) ++ [{Status, Value, UID}] ++ [{Status1, Value1, UID1} | T]};
        _ ->
            add_element({Status, Value, UID}, T, [{Status1, Value1, UID1} | L])
    end;
add_element(Insert, [], L) ->
    {ok, lists:reverse([Insert | L])}.

%% Private
%% @doc recursively looks for the Vertex to be removed. Once it is found, it is marked as "deleted", erasing its value.
recursive_remove(_, [], L) ->
    {ok, L};
recursive_remove({_, Value, UID}, [{_, Value, UID} | T], L) ->
    {ok, lists:reverse(L) ++ [{deleted, Value, UID}] ++ T};
recursive_remove(Vertex, [H | T], L) ->
    recursive_remove(Vertex, T, [H | L]).

%% @doc given an rga, this mehtod looks for all tombstones and removes them, returning the tombstone free rga.
purge_tombstones(Rga) ->
    L = lists:foldl(fun({Status, Value, UID}, L) ->
        case Status == deleted of
            true -> L;
            _ -> [{Status, Value, UID} | L]
        end
    end, [], Rga),
    lists:reverse(L).

%% @doc generate a unique identifier (best-effort).
unique() ->
    crypto:strong_rand_bytes(20).


-ifdef(TEST).

new_test() ->
    ?assertEqual([], new()).

generate_downstream_empty_rga_test() ->
    L = new(),
    {ok, DownstreamOp} = generate_downstream({addRight, 4, 1}, 1, L),
    ?assertMatch({addRight, {ok, 0, 0}, {ok, 4, _}}, DownstreamOp).

generate_downstream_non_empty_rga_test() ->
    L = new(),
    {ok, DownstreamOp} = generate_downstream({addRight, 4, 1}, 1, L),
    {ok, L1} = update(DownstreamOp, L),
    {ok, DownstreamOp1} = generate_downstream({addRight, 3, 1}, 1, L1),
    ?assertMatch({addRight, {ok, 4, _}, {ok, 3, _}}, DownstreamOp1).

add_right_in_empty_rga_test() ->
    L = new(),
    {ok, DownstreamOp} = generate_downstream({addRight, 1, 1}, 1, L),
    {ok, L1} = update(DownstreamOp, L),
    ?assertMatch([{ok, 1, _}], L1).

add_right_in_non_empty_rga_test() ->
    L = new(),
    {ok, DownstreamOp} = generate_downstream({addRight, 1, 1}, 1, L),
    {ok, L1} = update(DownstreamOp, L),
    {ok, DownstreamOp1} = generate_downstream({addRight, 2, 1}, 1, L1),
    {ok, L2} = update(DownstreamOp1, L1),
    ?assertMatch([{ok, 1, _}, {ok, 2, _}], L2).

%% In the tests that follow, the values of generate_downstream are placed by hand so
%% the intended scenarios can be correctly tested, not depending on the unique() function.

insert_in_the_middle_test() ->
    L = new(),
    DownstreamOp1 = {addRight, {ok, 0, 0}, {ok, 1, 1}},
    DownstreamOp2 = {addRight, {ok, 1, 1}, {ok, 3, 2}},
    DownstreamOp3 = {addRight, {ok, 1, 1}, {ok, 2, 3}},
    {ok, L1} = update(DownstreamOp1, L),
    {ok, L2} = update(DownstreamOp2, L1),
    {ok, L3} = update(DownstreamOp3, L2),
    ?assertMatch([{ok, 1, _}, {ok, 2, _}, {ok, 3, _}], L3).

remove_first_element_test() ->
    L = new(),
    DownstreamOp1 = {addRight, {ok, 0, 0}, {ok, 1, 1}},
    DownstreamOp2 = {addRight, {ok, 1, 1}, {ok, 3, 2}},
    DownstreamOp3 = {addRight, {ok, 1, 1}, {ok, 2, 3}},
    {ok, L1} = update(DownstreamOp1, L),
    {ok, L2} = update(DownstreamOp2, L1),
    {ok, L3} = update(DownstreamOp3, L2),
    {ok, DownstreamOp4} = generate_downstream({remove, 1}, 1, L3),
    {ok, L4} = update(DownstreamOp4, L3),
    ?assertMatch([{deleted, 1, _}, {ok, 2, _}, {ok, 3, _}], L4).

remove_middle_element_test() ->
    L = new(),
    DownstreamOp1 = {addRight, {ok, 0, 0}, {ok, 1, 1}},
    DownstreamOp2 = {addRight, {ok, 1, 1}, {ok, 3, 2}},
    DownstreamOp3 = {addRight, {ok, 1, 1}, {ok, 2, 3}},
    {ok, L1} = update(DownstreamOp1, L),
    {ok, L2} = update(DownstreamOp2, L1),
    {ok, L3} = update(DownstreamOp3, L2),
    {ok, DownstreamOp4} = generate_downstream({remove, 2}, 1, L3),
    {ok, L4} = update(DownstreamOp4, L3),
    ?assertMatch([{ok, 1, _}, {deleted, 2, _}, {ok, 3, _}], L4).

remove_last_element_test() ->
    L = new(),
    DownstreamOp1 = {addRight, {ok, 0, 0}, {ok, 1, 1}},
    DownstreamOp2 = {addRight, {ok, 1, 1}, {ok, 3, 2}},
    DownstreamOp3 = {addRight, {ok, 1, 1}, {ok, 2, 3}},
    {ok, L1} = update(DownstreamOp1, L),
    {ok, L2} = update(DownstreamOp2, L1),
    {ok, L3} = update(DownstreamOp3, L2),
    {ok, DownstreamOp4} = generate_downstream({remove, 3}, 1, L3),
    {ok, L4} = update(DownstreamOp4, L3),
    ?assertMatch([{ok, 1, _}, {ok, 2, _}, {deleted, 3, _}], L4).

insert_right_of_a_remove_test() ->
    L = new(),
    DownstreamOp1 = {addRight, {ok, 0, 0}, {ok, 1, 1}},
    DownstreamOp2 = {addRight, {ok, 1, 1}, {ok, 3, 2}},
    DownstreamOp3 = {addRight, {ok, 1, 1}, {ok, 2, 4}},
    {ok, L1} = update(DownstreamOp1, L),
    {ok, L2} = update(DownstreamOp2, L1),
    {ok, L3} = update(DownstreamOp3, L2),
    {ok, DownstreamOp4} = generate_downstream({remove, 2}, 1, L3),
    {ok, L4} = update(DownstreamOp4, L3),
    DownstreamOp5 = {addRight, {deleted, 2, 4}, {ok, 4, 3}},
    {ok, L5} = update(DownstreamOp5, L4),
    ?assertMatch([{ok, 1, _}, {deleted, 2, _}, {ok, 4, _}, {ok, 3, _}], L5).

pruge_tombstones_test() ->
    L = new(),
    DownstreamOp1 = {addRight, {ok, 0, 0}, {ok, 1, 1}},
    DownstreamOp2 = {addRight, {ok, 1, 1}, {ok, 3, 2}},
    DownstreamOp3 = {addRight, {ok, 1, 1}, {ok, 2, 3}},
    {ok, L1} = update(DownstreamOp1, L),
    {ok, L2} = update(DownstreamOp2, L1),
    {ok, L3} = update(DownstreamOp3, L2),
    {ok, DownstreamOp4} = generate_downstream({remove, 2}, 1, L3),
    {ok, L4} = update(DownstreamOp4, L3),
    L5 = purge_tombstones(L4),
    ?assertMatch([{ok, 1, _}, {ok, 3, _}], L5).

-endif.



