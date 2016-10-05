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

%% @doc module antidote_crdt_gmap - A grow-only map
%%
%% This map simply forwards all operations to the embedded CRDTs.
%% There is no real remove-operation.
%%

-module(antidote_crdt_gmap).

-behaviour(antidote_crdt).

%% API
-export([new/0, value/1, update/2, equal/2,
  to_binary/1, from_binary/1, is_operation/1, downstream/2, require_state_downstream/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-type gmap() :: dict:dict({Key::term(), Type::atom()}, NestedState::term()).
-type gmap_op() ::
    {update, nested_op()}
  | {update, [nested_op()]}
  | reset.
-type nested_op() :: {{Key::term(), Type::atom() }, Op::term()}.
-type gmap_effect() ::
    {update, nested_downstream()}
  | {update, [nested_downstream()]}.
-type nested_downstream() :: {{Key::term(), Type::atom() }, Op::term()}.

-spec new() -> gmap().
new() ->
    dict:new().

-spec value(gmap()) -> [{{Key::term(), Type::atom()}, Value::term()}].
value(Map) ->
  lists:sort([{{Key,Type}, Type:value(Value)} || {{Key, Type}, Value} <- dict:to_list(Map)]).

-spec require_state_downstream(gmap_op()) -> boolean().
require_state_downstream(_Op) ->
  true.


-spec downstream(gmap_op(), gmap()) -> {ok, gmap_effect()}.
downstream({update, {{Key, Type}, Op}}, CurrentMap) ->
    % TODO could be optimized for some types
    CurrentValue = case dict:is_key({Key, Type}, CurrentMap) of
      true -> dict:fetch({Key, Type}, CurrentMap);
      false -> Type:new()
    end,
    {ok, DownstreamOp} = Type:downstream(Op, CurrentValue),
    {ok, {update, {{Key, Type}, DownstreamOp}}};
downstream({update, Ops}, CurrentMap) when is_list(Ops) ->
    {ok, {update, lists:map(fun(Op) -> {ok, DSOp} = downstream({update, Op}, CurrentMap), DSOp end, Ops)}};
downstream(reset, CurrentMap) ->
  % calls reset on all embedded keys which support reset
  Reset =
    fun({{Key, Type}, State}) ->
      case Type:is_operation(reset) of
        true ->
          {ok, EmbeddedEffect} = Type:downstream(reset, State),
          [{update, {{Key, Type}, EmbeddedEffect}}];
        false -> []
      end
    end,
  DownstreamResets = lists:flatmap(Reset, dict:to_list(CurrentMap)),
  {ok, {update, DownstreamResets}}.

-spec update(gmap_effect(), gmap()) -> {ok, gmap()}.
update({update, {{Key, Type}, Op}}, Map) ->
    case dict:is_key({Key, Type}, Map) of
      true -> {ok, dict:update({Key, Type}, fun(V) -> {ok, Value} = Type:update(Op, V), Value end, Map)};
      false -> NewValue = Type:new(),
               {ok, NewValueUpdated} = Type:update(Op, NewValue),
               {ok, dict:store({Key, Type}, NewValueUpdated, Map)}
    end;
update({update, Ops}, Map) ->
    apply_ops(Ops, Map).

apply_ops([], Map) ->
    {ok, Map};
apply_ops([Op | Rest], Map) ->
  {ok, ORDict1} = update(Op, Map),
  apply_ops(Rest, ORDict1).



equal(Map1, Map2) ->
    Map1 == Map2. % TODO better implementation (recursive equals)


-define(TAG, 101).
-define(V1_VERS, 1).

to_binary(Policy) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Policy))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
  {ok, binary_to_term(Bin)}.

is_operation(Operation) ->
  case Operation of
    {update, {{_Key, Type}, Op}} ->
      antidote_crdt:is_type(Type)
        andalso Type:is_operation(Op);
    {update, Ops} when is_list(Ops) ->
      distinct([Key || {Key, _} <- Ops])
      andalso lists:all(fun(Op) -> is_operation({update, Op}) end, Ops);
    reset -> true;
    _ ->
      false
  end.

distinct([]) -> true;
distinct([X|Xs]) ->
  not lists:member(X, Xs) andalso distinct(Xs).


%% ===================================================================
%% EUnit tests
%% ===================================================================
% TODO
-ifdef(TEST).
new_test() ->
    ?assertEqual(dict:new(), new()).

update_test() ->
    Map1 = new(),
    {ok, DownstreamOp} = downstream({update, {{key1, antidote_crdt_lwwreg}, {assign, <<"test">>}}}, Map1),
    ?assertMatch({update, {{key1, antidote_crdt_lwwreg}, {_TS, <<"test">>}}}, DownstreamOp),
    {ok, Map2} = update(DownstreamOp, Map1),
    ?assertEqual([{{key1, antidote_crdt_lwwreg}, <<"test">>}], value(Map2)).

update2_test() ->
  Map1 = new(),
  {ok, Effect1} = downstream({update,[{{a,antidote_crdt_orset},{add,a}}]}, Map1),
  {ok, Map2} = update(Effect1, Map1),
  {ok, Effect2} = downstream(reset, Map2),
  {ok, Map3} = update(Effect2, Map2),
  ?assertEqual([{{a, antidote_crdt_orset}, []}], value(Map3)).

-endif.


