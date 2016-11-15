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

-module(prop_crdt_map_x).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_map_spec/0]).


prop_map_spec() ->
 crdt_properties:crdt_satisfies_spec(antidote_crdt_map_x, fun op/0, fun spec/1).


spec(Operations1) ->
  Operations = lists:flatmap(fun(Op) -> normalizeOp(Op, Operations1) end, Operations1),
  % the keys in the map are the ones that were updated and not deleted yet
  Keys = lists:usort([Key ||
    % has an update
    {AddClock, {update, {Key, _}}} <- Operations,
    % no remove after the update:
    [] == [Y || {RemoveClock, {remove, Y}} <- Operations, Key == Y, crdt_properties:clock_le(AddClock, RemoveClock)]
  ]),
  GroupedByKey = [{Key, nestedOps(Operations, Key)}  || Key <- Keys],
  NestedSpec = [{{Key,Type}, nestedSpec(Type, Ops)} || {{Key,Type}, Ops} <- GroupedByKey],
  %% TODO add reset operations
  lists:sort(NestedSpec).

nestedOps(Operations, {_,Type}=Key) ->
  Resets =
    case Type:is_operation({reset, {}}) of
      true ->
        [{Clock, {reset, {}}} || {Clock, {remove, Key2}} <- Operations, Key == Key2];
      false -> []
    end,
  Resets ++ [{Clock, NestedOp} || {Clock, {update, {Key2, NestedOp}}} <- Operations, Key == Key2].

nestedSpec(antidote_crdt_map_x, Ops) -> spec(Ops);
% nestedSpec(antidote_crdt_orset, Ops) -> prop_crdt_orset:add_wins_set_spec(Ops);
% nestedSpec(antidote_crdt_big_counter, Ops) -> prop_crdt_big_counter:big_counter_spec(Ops);
nestedSpec(antidote_crdt_set_rw, Ops) -> prop_crdt_set_rw:rem_wins_set_spec(Ops).

% normalizes operations (update-lists into single update-operations)
normalizeOp({Clock, {update, List}}, _) when is_list(List) ->
  [{Clock, {update, X}} || X <- List];
normalizeOp({Clock, {remove, List}}, _) when is_list(List) ->
  [{Clock, {remove, X}} || X <- List];
normalizeOp({Clock, {batch, {Updates, Removes}}}, _) ->
  [{Clock, {update, X}} || X <- Updates]
   ++ [{Clock, {remove, X}} || X <- Removes];
normalizeOp({Clock, {reset, {}}}, Operations) ->
  % reset is like removing all current keys
  Map = spec(crdt_properties:subcontext(Clock, Operations)),
  Keys = [Key || {Key, _Val} <- Map],
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
      % {{key(), prop_crdt_big_counter}, prop_crdt_big_counter:big_counter_op()},
      % {{key(), antidote_crdt_orset}, prop_crdt_orset:set_op()},
      {{key(), antidote_crdt_set_rw}, prop_crdt_set_rw:set_op()}
    ]
    ++
    if
      Size > 1 ->
        [{{key(), antidote_crdt_map_x}, ?LAZY(op(Size div 2))}];
      true -> []
    end
    ).

typed_key() -> {key(), crdt_type()}.

crdt_type() ->
  oneof([antidote_crdt_set_rw, antidote_crdt_map_x]).

key() ->
  oneof([a,b,c,d]).





