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
-module(new_inter_dc_manager).
-include("antidote.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

-export([get_descriptor/0, observe_dc/1]).

%% TODO catch rpc errors

-spec get_descriptor() -> interdc_descriptor().
get_descriptor() ->
  Nodes = dc_utilities:get_my_dc_nodes(),
  Publishers = lists:map(fun(Node) -> rpc:call(Node, new_inter_dc_pub, get_address, []) end, Nodes),
  LogReaders = lists:map(fun(Node) -> rpc:call(Node, log_reader, get_address, []) end, Nodes),
  {dc_utilities:get_my_dc_id(), Publishers, LogReaders}.


-spec observe_dc(interdc_descriptor()) -> ok.
observe_dc(Descriptor) ->
  {DCID, Publishers, LogReaders} = Descriptor,

  %% Broadcast the DC info to all vnodes
  ok = new_inter_dc_sub_vnode:register_dc(DCID, LogReaders),

  %% Announce the new publisher addresses to all subscribers in this DC.
  %% Equivalently, we could just pick one node in the DC and delegate all the subscription work to it.
  %% But we want to balance the work, so all nodes take part in subscribing.
  Nodes = dc_utilities:get_my_dc_nodes(),
  lists:foreach(fun(Node) -> ok = rpc:call(Node, new_inter_dc_sub, add_dc, [Publishers]) end, Nodes),

  ok.