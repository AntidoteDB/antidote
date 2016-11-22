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

-module(prop_crdt_big_counter).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_big_counter_spec/0, big_counter_op/0, big_counter_spec/1]).


prop_big_counter_spec() ->
 crdt_properties:crdt_satisfies_spec(antidote_crdt_big_counter, fun big_counter_op/0, fun big_counter_spec/1).


big_counter_spec(Operations1) ->
  Operations = crdt_properties:filter_resets(Operations1),
  lists:sum([X || {_, {increment, X}} <- Operations])
    - lists:sum([X || {_, {decrement, X}} <- Operations]).

% generates a random counter operation
big_counter_op() ->
  oneof([
    {increment, integer()},
    {decrement, integer()},
    {reset, {}}
  ]).

