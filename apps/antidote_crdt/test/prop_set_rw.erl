%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(prop_set_rw).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_set_rw_spec/0, op/0, spec/1]).


prop_set_rw_spec() ->
 crdt_properties:crdt_satisfies_spec(antidote_crdt_set_rw, fun op/0, fun spec/1).


spec(Operations1) ->
  Operations2 = crdt_properties:filter_resets(Operations1),
  Operations = lists:flatmap(fun normalizeOperation/1, Operations2),
  RemoveClocks =
    fun(X) ->
      [Clock || {Clock, {remove, Y}} <- Operations, X == Y]
    end,
  Removed =
    fun(X) ->
      % there is a remove for X which is not followed by an add
      lists:any(fun(RemClock) -> [] == [Y || {AddClock, {add, Y}} <- Operations, X == Y, crdt_properties:clock_le(RemClock, AddClock)] end, RemoveClocks(X))
    end,
  Added = [X || {_, {add, X}} <- Operations],
  [X || X <- lists:usort(Added), not Removed(X)].

% transforms add_all and remove_all into single operations
normalizeOperation({Clock, {add_all, Elems}}) ->
  [{Clock, {add, Elem}} || Elem <- Elems];
normalizeOperation({Clock, {remove_all, Elems}}) ->
  [{Clock, {remove, Elem}} || Elem <- Elems];
normalizeOperation(X) ->
  [X].

% generates a random counter operation
op() ->
  oneof([
    {add, set_element()},
    {add_all, list(set_element())},
    {remove, set_element()},
    {remove_all, list(set_element())},
    {reset, {}}
  ]).

set_element() ->
  oneof([a, b]).

