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

-module(prop_crdt_flag_ew).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_flag_ew_spec/0, op/0, spec/1]).

prop_flag_ew_spec() ->
 crdt_properties:crdt_satisfies_spec(antidote_crdt_flag_ew, fun op/0, fun spec/1).


spec(Operations) ->
  [Clock || {Clock, {enable, {}}} <- Operations,
      % all values, such that not overridden by other assign
      [] == [Clock2 || {Clock2, {disable, {}}} <- Operations, Clock =/= Clock2, crdt_properties:clock_le(Clock, Clock2)],
      % and not overridden by reset
      [] == [Clock3 || {Clock3, {reset, {}}} <- Operations, crdt_properties:clock_le(Clock, Clock3)]
    ] =/= [].

% generates a random operation
op() ->
  % frequency([
  %   {5, {enable, {}}},
  %   {5, {disable, {}}},
  %   {1, {reset, {}}}
  % ]).
  oneof([
    {enable, {}},
    {disable, {}},
    {reset, {}}
  ]).

