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

-export([add_dc/1, add_list_dcs/1, get_publishers/0]).

get_publishers() ->
  Nodes = dc_utilities:get_my_dc_nodes(),
  F = fun(Node) -> rpc:call(Node, new_inter_dc_pub, get_address, []) end,
  lists:map(F, Nodes).

%% Add info about a new DC. This info could be
%% used by other modules to communicate to other DC
-spec add_dc(dc_address()) -> ok.
add_dc(NewDC) -> new_inter_dc_sub:add_publisher(NewDC).

%% Add a list of DCs to this DC
-spec add_list_dcs([dc_address()]) -> ok.
add_list_dcs(DCs) -> lists:map(fun new_inter_dc_sub:add_publisher/1, DCs), ok.
