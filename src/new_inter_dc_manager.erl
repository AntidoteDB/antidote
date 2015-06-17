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

-export([subscribe/1, get_publishers/0]).

%% TODO catch rpc errors

%% Returns the list of publisher addresses for this DC, one address per node.
%% Each address is a ZeroMQ publisher socket.
-spec get_publishers() -> [pub_address()].
get_publishers() ->
  Nodes = dc_utilities:get_my_dc_nodes(),
  F = fun(Node) -> rpc:call(Node, new_inter_dc_pub, get_address, []) end,
  lists:map(F, Nodes).

%% Orders each node inside the cluster to subscribe to the specified list of publishers.
-spec subscribe([pub_address()]) -> ok.
subscribe(Publishers) ->
  Nodes = dc_utilities:get_my_dc_nodes(),
  F = fun(Node) -> rpc:call(Node, new_inter_dc_sub, add_publishers, [Publishers]) end,
  lists:foreach(F, Nodes).
