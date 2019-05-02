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
-module(antidotec_set).

-include_lib("antidote_pb_codec/include/antidote_pb.hrl").

-behaviour(antidotec_datatype).

-export([new/0,
         new/1,
         value/1,
         dirty_value/1,
         to_ops/2,
         is_type/1,
         type/0
        ]).

-export([add/2,
         remove/2,
         contains/2
        ]).

-record(antidote_set, {
          set :: sets:set(),
          adds :: sets:set(),
          rems :: sets:set()
         }).

-export_type([antidote_set/0]).
-opaque antidote_set() :: #antidote_set{}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-spec new() -> antidote_set().
new() ->
    #antidote_set{set=sets:new(), adds=sets:new(), rems=sets:new()}.

-spec new(list() | sets:set()) -> antidote_set().
new(List) when is_list(List) ->
    Set = sets:from_list(List),
    #antidote_set{set=Set, adds=sets:new(), rems=sets:new()};

new(Set) ->
    #antidote_set{set=Set, adds=sets:new(), rems=sets:new()}.

-spec value(antidote_set()) -> [term()].
value(#antidote_set{set=Set}) -> sets:to_list(Set).

-spec dirty_value(antidote_set()) -> [term()].
dirty_value(#antidote_set{set=Set, adds = Adds, rems=Rems}) ->
  sets:to_list(sets:subtract(sets:union(Set, Adds), Rems)).

%% @doc Adds an element to the local set container.
-spec add(term(), antidote_set()) -> antidote_set().
add(Elem, #antidote_set{adds=Adds}=Fset) ->
    Fset#antidote_set{adds=sets:add_element(Elem, Adds)}.

-spec remove(term(), antidote_set()) -> antidote_set().
remove(Elem, #antidote_set{rems=Rems}=Fset) ->
    Fset#antidote_set{rems=sets:add_element(Elem, Rems)}.

-spec contains(term(), antidote_set()) -> boolean().
contains(Elem, #antidote_set{set=Set}) ->
    sets:is_element(Elem, Set).

%% @doc Determines whether the passed term is a set container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, antidote_set).

%% @doc Returns the symbolic name of this container.
-spec type() -> set.
type() -> set.

to_ops(BoundObject, #antidote_set{adds=Adds, rems=Rems}) ->
    R = case sets:size(Rems) > 0 of
            true -> [{BoundObject, remove_all, sets:to_list(Rems)}];
            false -> []
        end,
    case sets:size(Adds) > 0 of
        true -> [{BoundObject, add_all, sets:to_list(Adds)} | R];
        false -> R
    end.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
add_op_test() ->
    New = antidotec_set:new([]),
    Set1 = antidotec_set:dirty_value(New),
    ?assertEqual([], Set1),
    OneElement = antidotec_set:add(<<"value1">>, New),
    Set2 = antidotec_set:dirty_value(OneElement),
    ?assertEqual([<<"value1">>], Set2).

add_op_existing_set_test() ->
    New = antidotec_set:new([<<"elem1">>, <<"elem2">>, <<"elem3">>]),
    ThreeElemSet = antidotec_set:dirty_value(New),
    ?assertEqual([<<"elem1">>, <<"elem2">>, <<"elem3">>], lists:sort(ThreeElemSet)),
    AddElem = antidotec_set:add(<<"elem4">>, New),
    S1 = antidotec_set:remove(<<"elem4">>, AddElem),
    S2 = antidotec_set:remove(<<"elem2">>, S1),
    TwoElemSet = antidotec_set:dirty_value(S2),
    ?assertEqual([<<"elem1">>, <<"elem3">>], lists:sort(TwoElemSet)).

-endif.
