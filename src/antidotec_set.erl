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

-include_lib("riak_pb/include/antidote_pb.hrl").

-behaviour(antidotec_datatype).

-export([new/1,
         new/2,
         message_for_get/1,
         value/1,
         to_ops/1,
         is_type/1,
         type/0,
         dirty_value/1
        ]).

-export([add/2,
         remove/2,
         contains/2
        ]).

-record(antidote_set, {
          key :: term(),
          set :: set(),
          adds :: set(),
          rems :: set()
         }).

-export_type([antidote_set/0]).
-opaque antidote_set() :: #antidote_set{}.

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.


-spec new(term()) -> antidote_set().
new(Key) ->
    #antidote_set{key=Key, set=sets:new(), adds=sets:new(), rems=sets:new()}.

-spec new(term(), list()) -> antidote_set().
new(Key,[]) ->
    #antidote_set{key=Key, set=sets:new(), adds=sets:new(), rems=sets:new()};

new(Key, [_H | _] = List) ->
    Set = lists:foldl(fun(E,S) ->
                        sets:add_element(E,S)
                end,sets:new(),List),
    #antidote_set{key=Key, set=Set, adds=sets:new(), rems=sets:new()};


new(Key, Set) ->
    #antidote_set{key=Key, set=Set, adds=sets:new(), rems=sets:new()}.

-spec value(antidote_set()) -> set().
value(#antidote_set{set=Set}) -> Set.

-spec dirty_value(antidote_set()) -> set().
dirty_value(#antidote_set{set=Set, adds=Adds}) ->
    sets:union(Set,Adds).

%% @doc Adds an element to the local set container.
-spec add(term(), antidote_set()) -> antidote_set().
add(Elem, #antidote_set{set=Set, adds=Adds}=Fset) ->
     case sets:is_element(Elem, Set) of
        false -> Fset#antidote_set{adds=sets:add_element(Elem,Adds)};
        true -> Fset
     end.
-spec remove(term(), antidote_set()) -> antidote_set().
remove(Elem, #antidote_set{set=Set, adds=Adds, rems=Rems}=Fset) ->
    case sets:is_element(Elem, Adds) of
        true ->  Fset#antidote_set{adds=sets:del_element(Elem,Adds)};
        false -> 
            case sets:is_element(Elem, Set) of
                true -> Fset#antidote_set{rems=sets:add_element(Elem,Rems)};
                false -> Fset
            end
    end.

-spec contains(term(), antidote_set()) -> boolean().
contains(Elem, #antidote_set{set=Set, adds=Adds, rems=Rems}) ->
    case sets:is_element(Elem, Adds) of
        true -> true;
        false ->
            case sets:is_element(Elem, Set) of
                true -> not sets:is_element(Elem, Rems);
                false -> false
            end
    end.

%% @doc Determines whether the passed term is a set container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, antidote_set).


%% @doc Returns the symbolic name of this container.
-spec type() -> riak_dt_orset.
type() -> riak_dt_orset.

to_ops(#antidote_set{key=Key, adds=Adds, rems=Rems}) -> 
    case sets:size(Adds) =:= 0 andalso sets:size(Rems) =:= 0 of
        true -> undefined;
        false ->
            AddsAsBin = lists:map(fun(X) ->
                                          erlang:term_to_binary(X)
                                  end,sets:to_list(Adds)),
            RemsAsBin = lists:map(fun(X) ->
                                          erlang:term_to_binary(X)
                                  end,sets:to_list(Rems)),
            [#fpbsetupdatereq{key=Key, adds=AddsAsBin, rems=RemsAsBin}]
    end.

message_for_get(Key) -> #fpbgetsetreq{key=Key}.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
add_op_test() ->
    New = antidotec_set:new(dumb_key),
    EmptySet = sets:size(antidotec_set:dirty_value(New)),
    OneElement = antidotec_set:add(atom1,New),
    Size1Set = sets:size(antidotec_set:dirty_value(OneElement)),
    [?_assert(EmptySet =:= 0),
     ?_assert(Size1Set =:= 1)].

add_op_existing_set_test() ->
    New = antidotec_set:new(dumb_key,[elem1,elem2,elem3]),
    ThreeElemSet = sets:size(antidotec_set:dirty_value(New)),
    AddElem = antidotec_set:add(elem4,New),
    S1 = antidotec_set:remove(elem4,AddElem),
    S2 = antidotec_set:remove(elem2,S1),
    TwoElemSet = sets:size(antidotec_set:dirty_value(S2)),
    [?_assert(ThreeElemSet =:= 3),
     ?_assert(TwoElemSet =:= 2)].
-endif.

