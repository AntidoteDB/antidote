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

%% @doc module antidote_crdt_map - A add-wins map
%%
%% This map forwards all operations to the embedded CRDTs.
%% Deleting a key tries to reset the entry to its initial state
%%
%% An element exists in a map, if there is at least one update on the key, which is not followed by a remove
%%
%% Resetting the map means removing all the current entries
%%
%% Implementation note:
%% The implementation of Add-wins semantic is the same as in orset:
%% Each element has a set of add-tokens.
%% An entry is in the map, if the set of add-tokens is not empty.
%% When an entry is removed, the value is still kept in the state, so that
%% concurrent updates can be reconciled.
%% This could be optimized for certain types

-module(antidote_crdt_map_aw).

-behaviour(antidote_crdt).

%% API
-export([new/0, value/1, update/2, equal/2,
  to_binary/1, from_binary/1, is_operation/1, downstream/2, require_state_downstream/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-type typedKey() :: {Key::term(), Type::atom()}.
-type token() :: term().
-type state() :: dict:dict(typedKey(), {[token()], NestedState::term()}).
-type op() ::
    {update, nested_op()}
  | {update, [nested_op()]}
  | {remove, typedKey()}
  | {remove, [typedKey()]}
  | {batch, {Updates::[nested_op()], Removes::[typedKey()]}}
  | {reset, {}}.
-type nested_op() :: {typedKey(), Op::term()}.
-type effect() ::
     {Adds::[nested_downstream()], Removed::[nested_downstream()], AddedToken::token()}.
-type nested_downstream() :: {typedKey(), none | {ok, Effect::term()}, RemovedTokens::[token()]}.

-spec new() -> state().
new() ->
    dict:new().

-spec value(state()) -> [{typedKey(), Value::term()}].
value(Map) ->
  lists:sort([{{Key, Type}, Type:value(Value)} || {{Key, Type}, {Tokens, Value}} <- dict:to_list(Map), Tokens =/= []]).

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
  Token =
    case UpdateEffects of
      [] -> <<>>; % no token required
      _ -> unique()
    end,
  {ok, {UpdateEffects, RemoveEffects, Token}};
downstream({reset, {}}, CurrentMap) ->
  % reset removes all keys
  AllKeys = [Key || {Key, _Val} <- value(CurrentMap)],
  downstream({remove, AllKeys}, CurrentMap).


-spec generate_downstream_update({typedKey(), Op::term()}, state()) -> nested_downstream().
generate_downstream_update({{Key, Type}, Op}, CurrentMap) ->
  {CurrentTokens, CurrentState} =
    case dict:is_key({Key, Type}, CurrentMap) of
      true -> dict:fetch({Key, Type}, CurrentMap);
      false -> {[], Type:new()}
    end,
  {ok, DownstreamEffect} = Type:downstream(Op, CurrentState),
  {{Key, Type}, {ok, DownstreamEffect}, CurrentTokens}.


-spec generate_downstream_remove(typedKey(), state()) -> nested_downstream().
generate_downstream_remove({Key, Type}, CurrentMap) ->
  {CurrentTokens, CurrentState} =
    case dict:is_key({Key, Type}, CurrentMap) of
      true -> dict:fetch({Key, Type}, CurrentMap);
      false -> {[], Type:new()}
    end,
  DownstreamEffect =
    case Type:is_operation({reset, {}}) of
      true ->
        {ok, _} = Type:downstream({reset, {}}, CurrentState);
      false ->
        none
    end,
  {{Key, Type}, DownstreamEffect, CurrentTokens}.


unique() ->
    crypto:strong_rand_bytes(20).

-spec update(effect(), state()) -> {ok, state()}.
update({Updates, Removes, AddedToken}, State) ->
  State2 = lists:foldl(fun(E, S) -> update(E, [AddedToken], S)  end, State, Updates),
  State3 = lists:foldl(fun(E, S) -> update(E, [], S)  end, State2, Removes),
  {ok, State3}.

update({{Key, Type}, {ok, Op}, RemovedTokens}, NewTokens, Map) ->
  case dict:find({Key, Type}, Map) of
    {ok, {Tokens, State}} ->
      UpdatedTokens = NewTokens ++ (Tokens -- RemovedTokens),
      {ok, UpdatedState} = Type:update(Op, State),
      dict:store({Key, Type}, {UpdatedTokens, UpdatedState}, Map);
    error ->
      NewValue = Type:new(),
      {ok, NewValueUpdated} = Type:update(Op, NewValue),
      dict:store({Key, Type}, {NewTokens, NewValueUpdated}, Map)
  end;
update({{Key, Type}, none, RemovedTokens}, NewTokens, Map) ->
  case dict:find({Key, Type}, Map) of
    {ok, {Tokens, State}} ->
      UpdatedTokens = NewTokens ++ (Tokens -- RemovedTokens),
      dict:store({Key, Type}, {UpdatedTokens, State}, Map);
    error ->
      NewValue = Type:new(),
      dict:store({Key, Type}, {NewTokens, NewValue}, Map)
  end.



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
    _ ->
      false
  end.

distinct([]) -> true;
distinct([X|Xs]) ->
  not lists:member(X, Xs) andalso distinct(Xs).




%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

reset1_test() ->
  Set0 = new(),
  % DC1: a.incr
  {ok, Incr1} = downstream({update, {{a, antidote_crdt_integer}, {increment, 1}}}, Set0),
  {ok, Set1a} = update(Incr1, Set0),
  % DC1 reset
  {ok, Reset1} = downstream({reset, {}}, Set1a),
  {ok, Set1b} = update(Reset1, Set1a),
  % DC2 a.remove
  {ok, Remove1} = downstream({remove, {a, antidote_crdt_integer}}, Set0),
  {ok, Set2a} = update(Remove1, Set0),
  % DC2 --> DC1
  {ok, Set1c} = update(Remove1, Set1b),
  % DC1 reset
  {ok, Reset2} = downstream({reset, {}}, Set1c),
  {ok, Set1d} = update(Reset2, Set1c),
  % DC1: a.incr
  {ok, Incr2} = downstream({update, {{a, antidote_crdt_integer}, {increment, 0}}}, Set1d),
  {ok, Set1e} = update(Incr2, Set1d),

  ?assertEqual([], value(Set2a)),
  ?assertEqual([], value(Set1d)),
  ?assertEqual([{{a, antidote_crdt_integer}, 1}], value(Set1e)).





-endif.



