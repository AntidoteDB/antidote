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

-module(prop_crdt_gset).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_gset_spec/0]).


prop_gset_spec() ->
 crdt_properties:crdt_satisfies_spec(antidote_crdt_gset, fun set_op/0, fun add_wins_set_spec/1).


add_wins_set_spec(Operations) ->
  lists:usort(
       [X || {_, {add, X}} <- Operations]
    ++ [X || {_, {add_all, Xs}} <- Operations, X <- Xs]
  ).

% generates a random counter operation
set_op() ->
  oneof([
    {add, set_element()},
    {add_all, list(set_element())}
  ]).

set_element() ->
  oneof([a,b,c,d]).