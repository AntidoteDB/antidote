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
-module(antidotec_map).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("antidote_pb_codec/include/antidote_pb.hrl").

-behaviour(antidotec_datatype).

-export([
         new/0,
         new/1,
         value/1,
         dirty_value/1,
         add_or_update/3,
         remove/2,
         to_ops/2,
         is_type/1,
         type/0
        ]).

-record(antidote_map, {
          values = #{} :: map(),
          new_values = #{} :: map(),
          removes = [] :: list()
         }).

-export_type([antidote_map/0]).
-opaque antidote_map() :: #antidote_map{}.

-spec new() -> antidote_map().
new() ->
  #antidote_map{}.

-spec new(list()) -> antidote_map().
new(Values) ->
  TranslatedValues = lists:map(fun({{Key, Type}, Value}) ->
              Mod = antidotec_datatype:module_for_crdt_type(Type),
              {{Key, Type}, Mod:new(Value)}
            end, Values),
  #antidote_map{values=maps:from_list(TranslatedValues)}.

-spec value(antidote_map()) -> map().
value(#antidote_map{values=Values}) ->
  maps:map(fun({_Key, Type}, Value) ->
              Mod = antidotec_datatype:module_for_crdt_type(Type),
              Mod:value(Value)
           end, Values).

-spec dirty_value(antidote_map()) -> map().
dirty_value(#antidote_map{values=Values, new_values=NewValues, removes=Removes}) ->
  MergedMap = maps:merge(Values, NewValues),
  MergedMapWithDrops = maps:without(Removes, MergedMap),
  maps:map(fun({_Key, Type}, Value) ->
              Mod = antidotec_datatype:module_for_crdt_type(Type),
              Mod:dirty_value(Value)
           end, MergedMapWithDrops).

-spec add_or_update(antidote_map(), any(), any()) -> antidote_map().
add_or_update(#antidote_map{values=Values, new_values=NewValues} = Map, Key, Value) ->
  case maps:is_key(Key, Values) of
    true -> Map#antidote_map{values=maps:put(Key, Value, Values)};
    false -> Map#antidote_map{new_values=maps:put(Key, Value, NewValues)}
  end.

-spec remove(antidote_map(), any()) -> antidote_map().
remove(#antidote_map{removes=Removes} = Map, Key) -> Map#antidote_map{removes=[Key|Removes]}.

-spec to_ops(any(), antidote_map()) -> list().
to_ops(BoundObject, #antidote_map{values=Values, new_values=NewValues, removes=Removes}) ->
  AddOps = update_ops(BoundObject, NewValues),
  UpdateOps = update_ops(BoundObject, Values),
  RemoveOps = lists:map(fun(K) -> {BoundObject, remove, K} end, Removes),
  lists:concat([AddOps, UpdateOps, RemoveOps]).

-spec update_ops(any(), map()) -> list().
update_ops(BoundObject, Map) ->
  List = maps:to_list(Map),
  lists:map(fun({{Key, Type}, Value}) ->
              Mod = antidotec_datatype:module_for_crdt_type(Type),
              %% {_, b, c} -> {Key, {b, c}} for arbitrarily sized tuples
              Ops = lists:map(fun(Op) -> {{Key, Type}, erlang:delete_element(1, Op)} end,
                              Mod:to_ops({Key, Type}, Value)),
              {BoundObject, update, Ops}
            end,
            List).

is_type(T) -> is_record(T, antidote_map).
type() -> map.
