%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 SyncFree Consortium.  All Rights Reserved.
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
%%
%% An operation-based Replicated Growable Array CRDT.
%%
%% As the data structure is operation-based, to issue an operation, one should
%% first call `generate_downstream/3', to get the downstream version of the
%% operation, and then call `update/2'.
%%
%% It provides two operations: addRight, which adds an element to the RGA, and remove,
%% which removes an element from the RGA.
%%
%% This implementation is based on the paper cited below.
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end
-module(antidote_crdt_rga).

-behaviour(antidote_crdt).

%% Call backs
-export([ new/0,
          value/1,
          downstream/2,
          update/2,
          equal/2,
          to_binary/1,
          from_binary/1,
          is_operation/1,
          require_state_downstream/1
        ]).

-export([purge_tombstones/1]).

-export_type([rga/0, rga_op/0, rga_downstream_op/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type vertex() :: {ok | deleted, any(), number()}.

-type rga_downstream_op() :: {addRight, vertex(), vertex()} | {remove, {ok, any(), number()}}.

-type rga_op() :: {addRight, any(), non_neg_integer()} | {remove, non_neg_integer()}.

-type rga_result() :: {ok, rga()}.

-type rga() :: [vertex()].

-spec new() -> [].
new() ->
    [].

%% @doc generate downstream operations.
%% If the operation is addRight, generates a unique token for the new element.
%% If the operation is remove, fetches the vertex of the element to be removed.
-spec downstream(rga_op(), rga()) -> {ok, rga_downstream_op()} | {error, {invalid_position, number()}}.
downstream({addRight, Elem, Position}, Rga) ->
    case (Position < 0) or (Position > length(Rga)) of
        true -> {error, {invalid_position, Position}};
        false -> case (Rga == []) or (Position == 0) of
                     true -> {ok, {addRight, {ok, 0, 0}, {ok, Elem, unique()}}};
                     false -> {ok, {addRight, lists:nth(Position, Rga), {ok, Elem, unique()}}}
                 end
    end;
downstream({remove, Position}, Rga) ->
    {ok, {remove, lists:nth(Position, Rga)}}.

%% @doc given an RGA, returns the same RGA, as it represents its own value.
-spec value(rga()) -> rga().
value(Rga) ->
    Rga.

%% @doc This method takes in an rga operation and an rga to perform the operation.
%% It returns either the added Vertex and the new rga in case of an addRight, and the new rga in the case of a remove.
-spec update(rga_downstream_op(), rga()) -> rga_result().
update({addRight, RightVertex, NewVertex}, Rga) ->
    recursive_insert(RightVertex, NewVertex, Rga, []);
update({remove, Vertex}, Rga) ->
    remove_vertex(Vertex, Rga).

%% Private
%% @doc recursively looks for the Vertex where the new element should be put to the right of.
-spec recursive_insert(vertex(), vertex(), rga(), list()) -> rga_result().
recursive_insert(_, NewVertex, [], []) ->
    {ok, [NewVertex]};
recursive_insert({ok, 0, 0}, NewVertex, L, []) ->
    add_element(NewVertex, L, []);
recursive_insert(RightVertex, NewVertex, [RightVertex | T], L) ->
    add_element(NewVertex, T, [RightVertex | L]);
recursive_insert(RightVertex, NewVertex, [H | T], L) ->
    recursive_insert(RightVertex, NewVertex, T, [H | L]).

%% Private
%% @doc the place for the insertion has been found, so now the UIDs are compared to see where to insert.
-spec add_element(vertex(), rga(), list()) -> rga_result().
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
%% @doc looks for the Vertex to be removed. Once it's found, it's marked as "deleted".
%% The Vertex is not removed from the list, to allow adding elements to its right.
-spec remove_vertex(vertex(), rga()) -> rga_result().
remove_vertex({ok, Value, UID}, Rga) ->
    {ok, lists:keyreplace(UID, 3, Rga, {deleted, Value, UID})}.

%% @doc given an rga, this mehtod looks for all tombstones and removes them, returning the tombstone free rga.
-spec purge_tombstones(rga()) -> rga_result().
purge_tombstones(Rga) ->
    L = lists:filter(fun({Status, _, _}) -> Status == ok end, Rga),
    {ok, L}.

%% @doc generates a unique identifier based on the node and a unique reference.
unique() ->
    {node(), make_ref()}.

%% @doc The following operation verifies that Operation is supported by this particular CRDT.
-spec is_operation(term()) -> boolean().
is_operation({addRight, _, Position}) ->
    (is_integer(Position) and (Position >= 0));
is_operation({remove, Position})->
            (is_integer(Position) and (Position >= 0));
is_operation(_) ->
            false.

require_state_downstream(_) ->
     true.

equal(Rga1, Rga2) ->
    Rga1 == Rga2.

to_binary(Rga1) ->
    erlang:term_to_binary(Rga1).

from_binary(Bin) ->
    {ok, erlang:binary_to_term(Bin)}.

-ifdef(TEST).

new_test() ->
    ?assertEqual([], new()).

generate_downstream_invalid_position_test() ->
    L = new(),
    Result1 = downstream({addRight, 1, 1}, L),
    ?assertMatch({error, {invalid_position, 1}}, Result1),
    Result2 = downstream({addRight, 1, -1}, L),
    ?assertMatch({error, {invalid_position, -1}}, Result2).

generate_downstream_empty_rga_test() ->
    L = new(),
    {ok, DownstreamOp} = downstream({addRight, 4, 0}, L),
    ?assertMatch({addRight, {ok, 0, 0}, {ok, 4, _}}, DownstreamOp).

generate_downstream_non_empty_rga_test() ->
    L = new(),
    {ok, DownstreamOp} = downstream({addRight, 4, 0}, L),
    {ok, L1} = update(DownstreamOp, L),
    {ok, DownstreamOp1} = downstream({addRight, 3, 1}, L1),
    ?assertMatch({addRight, {ok, 4, _}, {ok, 3, _}}, DownstreamOp1).

add_right_in_empty_rga_test() ->
    L = new(),
    {ok, DownstreamOp} = downstream({addRight, 1, 0}, L),
    {ok, L1} = update(DownstreamOp, L),
    ?assertMatch([{ok, 1, _}], L1).

add_right_in_non_empty_rga_test() ->
    L = new(),
    {ok, DownstreamOp} = downstream({addRight, 1, 0}, L),
    {ok, L1} = update(DownstreamOp, L),
    {ok, DownstreamOp1} = downstream({addRight, 2, 1}, L1),
    {ok, L2} = update(DownstreamOp1, L1),
    ?assertMatch([{ok, 1, _}, {ok, 2, _}], L2).

%% In the tests that follow, the values of generate_downstream are placed by hand ,so
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
    {ok, DownstreamOp4} = downstream({remove, 1}, L3),
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
    {ok, DownstreamOp4} = downstream({remove, 2}, L3),
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
    {ok, DownstreamOp4} = downstream({remove, 3}, L3),
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
    {ok, DownstreamOp4} = downstream({remove, 2}, L3),
    {ok, L4} = update(DownstreamOp4, L3),
    DownstreamOp5 = {addRight, {deleted, 2, 4}, {ok, 4, 3}},
    {ok, L5} = update(DownstreamOp5, L4),
    ?assertMatch([{ok, 1, _}, {deleted, 2, _}, {ok, 4, _}, {ok, 3, _}], L5).

purge_tombstones_test() ->
    L = new(),
    DownstreamOp1 = {addRight, {ok, 0, 0}, {ok, 1, 1}},
    DownstreamOp2 = {addRight, {ok, 1, 1}, {ok, 3, 2}},
    DownstreamOp3 = {addRight, {ok, 1, 1}, {ok, 2, 3}},
    {ok, L1} = update(DownstreamOp1, L),
    {ok, L2} = update(DownstreamOp2, L1),
    {ok, L3} = update(DownstreamOp3, L2),
    {ok, DownstreamOp4} = downstream({remove, 2}, L3),
    {ok, L4} = update(DownstreamOp4, L3),
    {ok, L5} = purge_tombstones(L4),
    ?assertMatch([{ok, 1, _}, {ok, 3, _}], L5).

%% This test creates two RGAs and performs updates, checking they reach
%% the same final state after the four updates are aplied in both replicas.
concurrent_updates_in_two_replicas_test() ->
    R1_0 = new(),
    R2_0 = new(),
    {ok, DownstreamOp1} = downstream({addRight, 1, 0},  R1_0),
    {ok, DownstreamOp2} = downstream({addRight, 2, 0},  R2_0),
    {ok, R1_1} = update(DownstreamOp1, R1_0),
    {ok, R2_1} = update(DownstreamOp2, R2_0),
    {ok, R1_2} = update(DownstreamOp2, R1_1),
    {ok, R2_2} = update(DownstreamOp1, R2_1),
    ?assertEqual(R1_2, R2_2),
    {ok, DownstreamOp3} = downstream({addRight, 3, 2}, R1_2),
    {ok, DownstreamOp4} = downstream({addRight, 4, 2}, R2_2),
    {ok, R1_3} = update(DownstreamOp3, R1_2),
    {ok, R2_3} = update(DownstreamOp4, R2_2),
    {ok, R1_4} = update(DownstreamOp4, R1_3),
    {ok, R2_4} = update(DownstreamOp3, R2_3),
    ?assertEqual(R1_4, R2_4).

is_operation_test() ->
    %% addRight checks
    ?assertEqual(false, is_operation({addRight, value, -1})),
    ?assertEqual(true, is_operation({addRight, value, 0})),
    ?assertEqual(true, is_operation({addRight, value, 1})),

    %% remove checks
    ?assertEqual(false, is_operation({remove, -1})),
    ?assertEqual(true, is_operation({remove, 0})),
    ?assertEqual(true, is_operation({remove, 1})),

    %% invalid operation checks
    ?assertEqual(false, is_operation({anything, 1})),
    ?assertEqual(false, is_operation({add, 1, 4})).

-endif.
