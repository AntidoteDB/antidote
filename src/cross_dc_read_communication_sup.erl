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

%% @doc : The supervisor in charge of all the socket acceptors.
%%  Supervisor starts up a pool of listeners which can accept incoming
%%  connection request from other DCs

-module(cross_dc_read_communication_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(Port) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Port]).

init([Port]) ->
    Listener = {cross_dc_read_communication_recvr,
                {cross_dc_read_communication_recvr, start_link, [Port]}, % pass the socket!
                permanent, 1000, worker, [cross_dc_read_communication_recvr]},

    SupWorkers = {cross_dc_read_communication_fsm_sup,
		  {cross_dc_read_communication_fsm_sup, start_link, []},
		  permanent, 1000, supervisor, [cross_dc_read_communication_fsm_sup]},
    {ok, {{one_for_one, 60, 3600}, [Listener, SupWorkers]}}.
