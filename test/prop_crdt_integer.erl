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

-module(prop_crdt_integer).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_counter_spec/0, op/0, spec/1]).


prop_counter_spec() ->
 crdt_properties:crdt_satisfies_spec(antidote_crdt_integer, fun op/0, fun spec/1).


spec(Operations1) ->
  Operations = [normalizeOp(Op) || Op <- Operations1],
  ConcurrentValues =
    [{Clock, Val} || {Clock, {set, Val}} <- Operations,
      [] == [Clock2 || {Clock2, {set, _}} <- Operations, Clock =/= Clock2, crdt_properties:clock_le(Clock, Clock2)]],
  ConcurrentValues2 =
    case ConcurrentValues of
      [] -> [{#{}, 0}];
      _ -> ConcurrentValues
    end,
  Delta =
    fun(Clock) ->
      lists:sum([X || {C, {increment, X}} <- Operations, not crdt_properties:clock_le(C, Clock)])
    end,
  WithDelta = [Val + Delta(Clock) || {Clock,Val} <- ConcurrentValues2],
  lists:max(WithDelta).

normalizeOp({Clock, reset}) -> {Clock, {set, 0}};
normalizeOp(Op) -> Op.

% generates a random counter operation
op() ->
  oneof([
    {set, integer()},
    {increment, integer()},
    reset
  ]).

