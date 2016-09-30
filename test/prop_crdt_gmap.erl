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
  GroupedByKey = [{Key, [{Clock, NestedOp} || {Clock, {update, {Key2, NestedOp}}} <- Operations, Key == Key2 ]}  || Key <- Keys],
  NestedSpec = [{{Key,Type}, nestedSpec(Type, Ops)} || {{Key,Type}, Ops} <- GroupedByKey],
  lists:sort(NestedSpec).

nestedSpec(antidote_crdt_gmap, Ops) -> spec(Ops);
nestedSpec(antidote_crdt_counter, Ops) -> prop_crdt_counter:counter_spec(Ops).


normalizeOp({Clock, {update, List}}) when is_list(List) ->
  [{Clock, {update, X}} || X <- List];
normalizeOp(X) -> [X].


% generates a random counter operation
op() ->
  {update, oneof([nestedOp(), list(nestedOp())])}.

nestedOp() ->
  oneof([
%%    {{key(), antidote_crdt_gmap}, op()},
    % TODO add other type (orset) and recursive maps
    {{key(), antidote_crdt_counter}, prop_crdt_counter:counter_op()}
  ]).


key() ->
  oneof([a,b,c,d]).



