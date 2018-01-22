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

-module(prop_map_rr).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_map_rr_spec/0]).

prop_map_rr_spec() ->
 crdt_properties:crdt_satisfies_partial_spec(map_rr, fun op/0, fun spec/2).


spec(Operations1, Value) ->
  Operations = normalize(Operations1),
  % Collect all keys from all all updates
  Keys = allKeys(Operations),
  GroupedByKey = [{Key, nestedOps(Operations, Key)}  || Key <- Keys],
  KeyCheck =
    fun({{Key,Type},Ops}) ->
      ?WHENFAIL(
        begin
          io:format("~n~nOperations1 = ~p~n", [Operations1]),
          io:format("Operations = ~p~n", [Operations]),
          io:format("GroupedByKey = ~p~n", [GroupedByKey])
        end,
        nestedSpec(Type, Ops, map_rr:get({Key,Type}, Value))
      )
    end,
  conjunction(
    [{Key, KeyCheck({{Key,Type}, Ops})}
     || {{Key,Type}, Ops} <- GroupedByKey]).

normalize(Operations) ->
   lists:flatmap(fun(Op) -> normalizeOp(Op, Operations) end, Operations).

allKeys(Operations) ->
  lists:usort([Key || {_AddClock, {update, {Key, _}}} <- Operations]).

nestedOps(Operations, {_,Type}=Key) ->
  Resets =
    case Type:is_operation({reset, {}}) of
      true ->
        [{Clock, {reset, {}}} || {Clock, {remove, Key2}} <- Operations, Key == Key2];
      false -> []
    end,
  Resets ++ [{Clock, NestedOp} || {Clock, {update, {Key2, NestedOp}}} <- Operations, Key == Key2].

nestedSpec(map_rr, Ops, Value) -> spec(Ops, Value);
% nestedSpec(set_aw, Ops) -> prop_set_aw:spec(Ops);
% nestedSpec(counter_fat, Ops) -> prop_counter_fat:spec(Ops);
nestedSpec(set_rw, Ops, Value) ->
  (crdt_properties:spec_to_partial(fun prop_set_rw:spec/1))(Ops, Value).

% normalizes operations (update-lists into single update-operations)
normalizeOp({Clock, {update, List}}, _) when is_list(List) ->
  [{Clock, {update, X}} || X <- List];
normalizeOp({Clock, {remove, List}}, _) when is_list(List) ->
  [{Clock, {remove, X}} || X <- List];
normalizeOp({Clock, {batch, {Updates, Removes}}}, _) ->
  [{Clock, {update, X}} || X <- Updates]
   ++ [{Clock, {remove, X}} || X <- Removes];
normalizeOp({Clock, {reset, {}}}, Operations) ->
  % reset is like remove on all keys
  Keys = allKeys(normalize(crdt_properties:subcontext(Clock, Operations))),
  [{Clock, {remove, X}} || X <- Keys];
normalizeOp(X, _) -> [X].


% generates a random operation
op() -> ?SIZED(Size, op(Size)).
op(Size) ->
  oneof([
    {update, nestedOp(Size)},
    {update, ?LET(L, list(nestedOp(Size div 2)), removeDuplicateKeys(L, []))},
    {remove, typed_key()},
    {remove, ?LET(L, list(typed_key()), lists:usort(L))},
    ?LET({Updates,Removes},
      {list(nestedOp(Size div 2)),list(typed_key())},
      begin
        Removes2 = lists:usort(Removes),
        Updates2 = removeDuplicateKeys(Updates, Removes2),
        {batch, {Updates2, Removes2}}
      end),
    {reset, {}}
  ]).

removeDuplicateKeys([], _) -> [];
removeDuplicateKeys([{Key,Op}|Rest], Keys) ->
  case lists:member(Key, Keys) of
    true -> removeDuplicateKeys(Rest, Keys);
    false -> [{Key, Op}|removeDuplicateKeys(Rest, [Key|Keys])]
  end.

nestedOp(Size) ->
  oneof(
    [
      % {{key(), counter_fat}, prop_counter_fat:op()},
      % {{key(), set_aw}, prop_set_aw:op()},
      {{key(), set_rw}, prop_set_rw:op()}
    ]
    ++
    if
      Size > 1 ->
        [{{key(), map_rr}, ?LAZY(op(Size div 2))}];
      true -> []
    end
    ).

typed_key() -> {key(), crdt_type()}.

crdt_type() ->
  oneof([set_rw, map_rr]).

key() ->
  oneof([key1,key2,key3,key4]).





