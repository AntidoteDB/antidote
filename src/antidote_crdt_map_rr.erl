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

%% @doc module antidote_crdt_map_rr - A CRDT map datatype with a reset functionality
%%
%% Inserting a new element in the map:
%%  if element already there -> do nothing
%%  if not create a map entry where value is initial state of the embedded
%%    data type (a call to the create function of the embedded data type)
%%
%% Update operations on entries(embedded CRDTs) are calls to the update fucntions of the entries.
%%
%% Deleting an entry in the map:
%%  1- calls the reset function of this entry (tries to reset entry to its initial state)
%%    As reset only affects operations that are locally (where reset was invoked) seen
%%    i.e. operations on the same entry that are concurrent to the reset operation are
%%    not affected and their effect should be observable once delivered.
%%
%%  if there were no operations concurrent to the reset (all operations where in the causal past of the reset),
%%  then the state of the entry is bottom (the initial state of the entry)
%%
%%  2- checks if the state of the entry after the reset is bottom (its initial state)
%%    if bottom, delete the entry from the map
%%    if not bottom, keep the entry
%%  
%%
%% An entry exists in a map, if there is at least one update (inserts included) on the key, which is not followed by a remove
%%
%% Resetting the map means removing all the current entries
%%

-module(antidote_crdt_map_rr).

-behaviour(antidote_crdt).

%% API
-export([new/0, value/1, update/2, equal/2, get/2,
  to_binary/1, from_binary/1, is_operation/1, downstream/2, require_state_downstream/1, is_bottom/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-type typedKey() :: {Key::term(), Type::atom()}.
-type state() :: dict:dict(typedKey(), {NestedState::term()}).
-type op() ::
    {update, nested_op()}
  | {update, [nested_op()]}
  | {remove, typedKey()}
  | {remove, [typedKey()]}
  | {batch, {Updates::[nested_op()], Removes::[typedKey()]}}
  | {reset, {}}.
-type nested_op() :: {typedKey(), Op::term()}.
-type effect() ::
     {Adds::[nested_downstream()], Removed::[nested_downstream()]}.
-type nested_downstream() :: {typedKey(), none | {ok, Effect::term()}}.
-type value() :: orddict:orddict(typedKey(), term()).

-spec new() -> state().
new() ->
    dict:new().

-spec value(state()) -> value().
value(Map) ->
  lists:sort([{{Key, Type}, Type:value(Value)} || {{Key, Type}, Value} <- dict:to_list(Map)]).

% get a value from the map
% returns empty value if the key is not present in the map
-spec get(typedKey(), value()) -> term().
get({_K, Type}=Key, Map) ->
  case orddict:find(Key, Map) of
    {ok, Val} -> Val;
    error -> Type:value(Type:new())
  end.


-spec require_state_downstream(op()) -> boolean().
require_state_downstream(_Op) ->
  true.

-spec downstream(op(), state()) -> {ok, effect()}.
downstream({update, {{Key, Type}, Op}}, CurrentMap) ->
  downstream({update, [{{Key, Type}, Op}]}, CurrentMap);
downstream({update, NestedOps}, CurrentMap) ->
  downstream({batch, {NestedOps, []}}, CurrentMap);
downstream({remove, {Key, Type}}, CurrentMap) ->
  downstream({remove, [{Key, Type}]}, CurrentMap);
downstream({remove, Keys}, CurrentMap) ->
  downstream({batch, {[], Keys}}, CurrentMap);
downstream({batch, {Updates, Removes}}, CurrentMap) ->
  UpdateEffects = [generate_downstream_update(Op, CurrentMap) || Op <- Updates],
  RemoveEffects = [generate_downstream_remove(Key, CurrentMap) || Key <- Removes],
  {ok, {UpdateEffects, RemoveEffects}};
downstream({reset, {}}, CurrentMap) ->
  % reset removes all keys
  AllKeys = [Key || {Key, _Val} <- value(CurrentMap)],
  downstream({remove, AllKeys}, CurrentMap).


-spec generate_downstream_update({typedKey(), Op::term()}, state()) -> nested_downstream().
generate_downstream_update({{Key, Type}, Op}, CurrentMap) ->
  CurrentState =
    case dict:is_key({Key, Type}, CurrentMap) of
      true -> dict:fetch({Key, Type}, CurrentMap);
      false -> Type:new()
    end,
  {ok, DownstreamEffect} = Type:downstream(Op, CurrentState),
  {{Key, Type}, {ok, DownstreamEffect}}.


-spec generate_downstream_remove(typedKey(), state()) -> nested_downstream().
generate_downstream_remove({Key, Type}, CurrentMap) ->
  CurrentState =
    case dict:is_key({Key, Type}, CurrentMap) of
      true -> dict:fetch({Key, Type}, CurrentMap);
      false -> Type:new()
    end,
  DownstreamEffect =
    case Type:is_operation({reset, {}}) of
      true ->
        {ok, _} = Type:downstream({reset, {}}, CurrentState);
      false ->
        none
    end,
  {{Key, Type}, DownstreamEffect}.


-spec update(effect(), state()) -> {ok, state()}.
update({Updates, Removes}, State) ->
  State2 = lists:foldl(fun(E, S) -> update_entry(E, S)  end, State, Updates),
  State3 = dict:fold(fun(K, V, S) -> remove_obsolete(K, V, S)  end, new(), State2),
  State4 = lists:foldl(fun(E, S) -> remove_entry(E, S)  end, State3, Removes),
  {ok, State4}.

update_entry({{Key, Type}, {ok, Op}}, Map) ->
  case dict:find({Key, Type}, Map) of
    {ok, State} ->
      {ok, UpdatedState} = Type:update(Op, State),
      dict:store({Key, Type}, UpdatedState, Map);
    error ->
      NewValue = Type:new(),
      {ok, NewValueUpdated} = Type:update(Op, NewValue),
      dict:store({Key, Type}, NewValueUpdated, Map)
  end.

remove_entry({{Key, Type}, {ok, Op}}, Map) ->
  case dict:find({Key, Type}, Map) of
    {ok, State} ->
      {ok, UpdatedState} = Type:update(Op, State),
      case is_bottom(Type, UpdatedState) of
        true ->
          dict:erase({Key, Type}, Map);
        false ->
          dict:store({Key, Type}, UpdatedState, Map)
      end;
    error ->
      Map
  end;
remove_entry({{_Key, _Type}, none}, Map) ->
  Map.

remove_obsolete({Key, Type}, Val, Map) ->
  case is_bottom(Type, Val) of
    false ->
      dict:store({Key, Type}, Val, Map);
    true ->
      Map
  end.

is_bottom(Type, State) ->
  erlang:function_exported(Type, is_bottom, 1) andalso Type:is_bottom(State).

equal(Map1, Map2) ->
    Map1 == Map2. % TODO better implementation (recursive equals)


-define(TAG, 101).
-define(V1_VERS, 1).

to_binary(Policy) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Policy))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
  {ok, binary_to_term(Bin)}.

is_operation(Operation) ->
  case Operation of
    {update, {{_Key, Type}, Op}} ->
      antidote_crdt:is_type(Type)
        andalso Type:is_operation(Op);
    {update, Ops} when is_list(Ops) ->
      distinct([Key || {Key, _} <- Ops])
      andalso lists:all(fun(Op) -> is_operation({update, Op}) end, Ops);
    {remove, {_Key, Type}} ->
      antidote_crdt:is_type(Type);
    {remove, Keys} when is_list(Keys) ->
      distinct(Keys)
        andalso lists:all(fun(Key) -> is_operation({remove, Key}) end, Keys);
    {batch, {Updates, Removes}} ->
      is_list(Updates)
        andalso is_list(Removes)
        andalso distinct(Removes ++ [Key || {Key, _} <- Updates])
        andalso lists:all(fun(Key) -> is_operation({remove, Key}) end, Removes)
        andalso lists:all(fun(Op) -> is_operation({update, Op}) end, Updates);
    {reset, {}} -> true;
    is_bottom -> true;
    _ ->
      false
  end.

distinct([]) -> true;
distinct([X|Xs]) ->
  not lists:member(X, Xs) andalso distinct(Xs).

is_bottom(Map) ->
  dict:is_empty(Map).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

reset1_test() ->
  Map0 = new(),
  % DC1: a.incr
  {ok, Incr1} = downstream({update, {{a, antidote_crdt_fat_counter}, {increment, 1}}}, Map0),
  {ok, Map1a} = update(Incr1, Map0),
  % DC1 reset
  {ok, Reset1} = downstream({reset, {}}, Map1a),
  {ok, Map1b} = update(Reset1, Map1a),
  % DC2 a.remove
  {ok, Remove1} = downstream({remove, {a, antidote_crdt_fat_counter}}, Map0),
  {ok, Map2a} = update(Remove1, Map0),
  % DC2 --> DC1
  {ok, Map1c} = update(Remove1, Map1b),
  % DC1 reset
  {ok, Reset2} = downstream({reset, {}}, Map1c),
  {ok, Map1d} = update(Reset2, Map1c),
  % DC1: a.incr
  {ok, Incr2} = downstream({update, {{a, antidote_crdt_fat_counter}, {increment, 2}}}, Map1d),
  {ok, Map1e} = update(Incr2, Map1d),

  io:format("Map0 = ~p~n", [Map0]),
  io:format("Incr1 = ~p~n", [Incr1]),
  io:format("Map1a = ~p~n", [Map1a]),
  io:format("Reset1 = ~p~n", [Reset1]),
  io:format("Map1b = ~p~n", [Map1b]),
  io:format("Remove1 = ~p~n", [Remove1]),
  io:format("Map2a = ~p~n", [Map2a]),
  io:format("Map1c = ~p~n", [Map1c]),
  io:format("Reset2 = ~p~n", [Reset2]),
  io:format("Map1d = ~p~n", [Map1d]),
  io:format("Incr2 = ~p~n", [Incr2]),
  io:format("Map1e = ~p~n", [Map1e]),

  ?assertEqual([], value(Map0)),
  ?assertEqual([{{a, antidote_crdt_fat_counter}, 1}], value(Map1a)),
  ?assertEqual([], value(Map1b)),
  ?assertEqual([], value(Map2a)),
  ?assertEqual([], value(Map1c)),
  ?assertEqual([], value(Map1d)),
  ?assertEqual([{{a, antidote_crdt_fat_counter}, 2}], value(Map1e)).


reset2_test() ->
  Map0 = new(),
  % DC1: s.add
  {ok, Add1} = downstream({update, {{s, antidote_crdt_set_rw}, {add, a}}}, Map0),
  {ok, Map1a} = update(Add1, Map0),
  % DC1 reset
  {ok, Reset1} = downstream({reset, {}}, Map1a),
  {ok, Map1b} = update(Reset1, Map1a),
  % DC2 s.remove
  {ok, Remove1} = downstream({remove, {s, antidote_crdt_set_rw}}, Map0),
  {ok, Map2a} = update(Remove1, Map0),
  % DC2 --> DC1
  {ok, Map1c} = update(Remove1, Map1b),
  % DC1 reset
  {ok, Reset2} = downstream({reset, {}}, Map1c),
  {ok, Map1d} = update(Reset2, Map1c),
  % DC1: s.add
  {ok, Add2} = downstream({update, {{s, antidote_crdt_set_rw}, {add, b}}}, Map1d),
  {ok, Map1e} = update(Add2, Map1d),

  io:format("Map0 = ~p~n"   , [value(Map0)]),
  io:format("Add1 = ~p~n"   , [Add1]),
  io:format("Map1a = ~p~n"  , [value(Map1a)]),
  io:format("Reset1 = ~p~n" , [Reset1]),
  io:format("Map1b = ~p~n"  , [value(Map1b)]),
  io:format("Remove1 = ~p~n", [Remove1]),
  io:format("Map2a = ~p~n"  , [value(Map2a)]),
  io:format("Map1c = ~p~n"  , [value(Map1c)]),
  io:format("Reset2 = ~p~n" , [Reset2]),
  io:format("Map1d = ~p~n"  , [value(Map1d)]),
  io:format("Add2 = ~p~n"   , [Add2]),
  io:format("Map1e = ~p~n"  , [value(Map1e)]),

  ?assertEqual([], value(Map0)),
  ?assertEqual([{{s, antidote_crdt_set_rw}, [a]}], value(Map1a)),
  ?assertEqual([], value(Map1b)),
  ?assertEqual([], value(Map2a)),
  ?assertEqual([], value(Map1c)),
  ?assertEqual([], value(Map1d)),
  ?assertEqual([{{s, antidote_crdt_set_rw}, [b]}], value(Map1e)).

prop1_test() ->
  Map0 = new(),
  % DC1: s.add
  {ok, Add1} = downstream({update, {{a, antidote_crdt_map_rr}, {update, {{a, antidote_crdt_set_rw}, {add, a}}}}}, Map0),
  {ok, Map1a} = update(Add1, Map0),

  % DC1 reset
  {ok, Reset1} = downstream({remove, {a, antidote_crdt_map_rr}}, Map1a),
  {ok, Map1b} = update(Reset1, Map1a),

  io:format("Map0 = ~p~n", [Map0]),
  io:format("Add1 = ~p~n", [Add1]),
  io:format("Map1a = ~p~n", [Map1a]),
  io:format("Reset1 = ~p~n", [Reset1]),
  io:format("Map1b = ~p~n", [Map1b]),

  ?assertEqual([], value(Map0)),
  ?assertEqual([{{a, antidote_crdt_map_rr}, [{{a, antidote_crdt_set_rw}, [a]}]}], value(Map1a)),
  ?assertEqual([], value(Map1b)).

prop2_test() ->
  Map0 = new(),
  % DC1: update remove
  {ok, Add1} = downstream({update, [{{b, antidote_crdt_map_rr}, {remove, {a, antidote_crdt_set_rw}}}]}, Map0),
  {ok, Map1a} = update(Add1, Map0),

  % DC2 remove
  {ok, Remove2} = downstream({remove, {b, antidote_crdt_map_rr}}, Map0),
  {ok, Map2a} = update(Remove2, Map0),

  % pull DC2 -> DC1
  {ok, Map1b} = update(Remove2, Map1a),

  io:format("Map0 = ~p~n", [Map0]),
  io:format("Add1 = ~p~n", [Add1]),
  io:format("Map1a = ~p~n", [Map1a]),
  io:format("Remove2 = ~p~n", [Remove2]),
  io:format("Map1b = ~p~n", [Map1b]),

  ?assertEqual([], value(Map0)),
  ?assertEqual([], value(Map1a)),
  ?assertEqual([], value(Map2a)),
  ?assertEqual([], value(Map1b)).

upd(Update, State) ->
    {ok, Downstream} = downstream(Update, State),
    {ok, Res} = update(Downstream, State),
    Res.

remove_test() ->
  M1 = new(),
  ?assertEqual([], value(M1)),
  ?assertEqual(true, is_bottom(M1)),
  M2 = upd({update, [
      {{<<"a">>, antidote_crdt_orset}, {add, <<"1">>}},
      {{<<"b">>, antidote_crdt_mvreg}, {assign, <<"2">>}},
      {{<<"c">>, antidote_crdt_fat_counter}, {increment, 1}}
    ]}, M1),
  ?assertEqual([
      {{<<"a">>, antidote_crdt_orset}, [<<"1">>]},
      {{<<"b">>, antidote_crdt_mvreg}, [<<"2">>]},
      {{<<"c">>, antidote_crdt_fat_counter}, 1}
  ], value(M2)),
  ?assertEqual(false, is_bottom(M2)),
  M3 = upd({reset, {}}, M2),
  io:format("M3 state = ~p~n", [dict:to_list(M3)]),
  ?assertEqual([], value(M3)),
  ?assertEqual(true, is_bottom(M3)),
  ok.

-endif.
