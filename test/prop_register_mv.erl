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

-module(prop_register_mv).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_register_mv_spec/0]).


prop_register_mv_spec() ->
 crdt_properties:crdt_satisfies_spec(register_mv, fun op/0, fun spec/1).


spec(Operations) ->
  lists:sort([Val || {assign, Val}  <- crdt_properties:latest_operations(Operations)]).



% generates a random operation
op() ->
  frequency([
    {5, {assign, oneof([a,b,c,d,e,f,g,h,i])}},
    {1, {reset, {}}}
  ]).

