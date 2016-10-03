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

-module(prop_crdt_set_rw).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_orset_spec/0, set_op/0, rem_wins_set_spec/1]).


prop_orset_spec() ->
 crdt_properties:crdt_satisfies_spec(antidote_crdt_set_rw, fun set_op/0, fun rem_wins_set_spec/1).


rem_wins_set_spec(Operations1) ->
  Operations = lists:flatmap(fun normalizeOperation/1, Operations1),
  Removed =
    [X ||
      % such that there is a remove operation for X
      {RemClock, {remove, X}} <- Operations,
      % and there is no add operation after the remove
      [] == [Y || {AddClock, {add, Y}} <- Operations, X == Y, crdt_properties:clock_le(RemClock, AddClock)]
    ],
  Added = lists:usort([X || {_, {add, X}} <- Operations]),
  Added -- Removed.

% transforms add_all and remove_all into single operations
normalizeOperation({Clock, {add_all, Elems}}) ->
  [{Clock, {add, Elem}} || Elem <- Elems];
normalizeOperation({Clock, {remove_all, Elems}}) ->
  [{Clock, {remove, Elem}} || Elem <- Elems];
normalizeOperation(X) ->
  [X].

% generates a random counter operation
set_op() ->
  oneof([
    {add, set_element()},
    {add_all, list(set_element())},
    {remove, set_element()},
    {remove_all, list(set_element())}
  ]).

set_element() ->
  oneof([a,b]).

