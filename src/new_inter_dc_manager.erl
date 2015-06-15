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

-spec start_broker(port()) -> {ok, dc_address()}.
-export([start_broker/1, stop_broker/0, get_dcs/0, add_dc/1, add_list_dcs/1]).

start_broker(Port) ->
  %% One node will now act as interDC node.
  {ok, _PubPid, _SubPid} = antidote_sup:start_pub(Port),
  {ok, new_inter_dc_publisher:get_address()}.

stop_broker() -> ok.

%% Returns all DCs known to this DC.
-spec get_dcs() ->{ok, [dc_address()]}.
get_dcs() -> {ok, new_inter_dc_subscriber:get_dcs()}.

%% Add info about a new DC. This info could be
%% used by other modules to communicate to other DC
-spec add_dc(dc_address()) -> ok.
add_dc(NewDC) -> new_inter_dc_subscriber:add_dc(NewDC).

%% Add a list of DCs to this DC
-spec add_list_dcs([dc_address()]) -> ok.
add_list_dcs(DCs) -> lists:map(fun new_inter_dc_subscriber:add_dc/1, DCs), ok.


