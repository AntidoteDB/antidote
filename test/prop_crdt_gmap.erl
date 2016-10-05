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

-module(prop_crdt_gmap).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_gmap_spec/0]).


prop_gmap_spec() ->
 crdt_properties:crdt_satisfies_spec(antidote_crdt_gmap, fun op/0, fun spec/1).


spec(Operations1) ->
  Operations = lists:flatmap(fun normalizeOp/1, Operations1),
  Keys = lists:usort([Key || {_, {update, {Key, _}}} <- Operations]),
  GroupedByKey = [{Key, nestedOps(Operations, Key)}  || Key <- Keys],
  NestedSpec = [{{Key,Type}, nestedSpec(Type, Ops)} || {{Key,Type}, Ops} <- GroupedByKey],
  lists:sort(NestedSpec).

nestedOps(Operations, {_,Type}=Key) ->
  Resets =
      case Type:is_operation(reset) of
        true ->
          [{Clock, reset} || {Clock, reset} <- Operations];
        false -> []
      end,
  Resets ++ [{Clock, NestedOp} || {Clock, {update, {Key2, NestedOp}}} <- Operations, Key == Key2].

nestedSpec(antidote_crdt_gmap, Ops) -> spec(Ops);
nestedSpec(antidote_crdt_orset, Ops) -> prop_crdt_orset:add_wins_set_spec(Ops);
nestedSpec(antidote_crdt_counter, Ops) -> prop_crdt_counter:counter_spec(Ops).


normalizeOp({Clock, {update, List}}) when is_list(List) ->
  [{Clock, {update, X}} || X <- List];
normalizeOp(X) -> [X].


% generates a random operation
op() -> ?SIZED(Size, op(Size)).
op(Size) ->
  frequency([
    % nested updates
    {10, {update, oneof([
        nestedOp(Size),
        ?LET(L, list(nestedOp(Size div 2)), removeDuplicateKeys(L, []))
      ])}},
    {1, reset}
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
      % TODO add other type (orset) and recursive maps
      % TODO make sure that keys are unique here and in the is_operation check
      {{key(), antidote_crdt_orset}, prop_crdt_orset:set_op()},
      {{key(), antidote_crdt_counter}, prop_crdt_counter:counter_op()}
    ]
    ++
    if
      Size > 1 ->
        [{{key(), antidote_crdt_gmap}, ?LAZY(op(Size div 2))}];
      true -> []
    end
    ).


key() ->
  oneof([a,b,c,d]).





