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
-module(antidote_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_rep/2]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc: start_rep(Port) - starts a server managed by Pid which listens for 
%% incomming tcp connection on port Port. Server receives updates to replicate 
%% from other DCs 
start_rep(Pid, Port) ->
    supervisor:start_child(?MODULE, {inter_dc_communication_sup,
                    {inter_dc_communication_sup, start_link, [Pid, Port]},
                    permanent, 5000, supervisor, [inter_dc_communication_sup]}).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    LoggingMaster = {logging_vnode_master,
                     {riak_core_vnode_master, start_link, [logging_vnode]},
                     permanent, 5000, worker, [riak_core_vnode_master]},
    ClockSIMaster = { clocksi_vnode_master,
                      {riak_core_vnode_master, start_link, [clocksi_vnode]},
                      permanent, 5000, worker, [riak_core_vnode_master]},

    InterDcRepMaster = {inter_dc_repl_vnode_master,
                        {riak_core_vnode_master, start_link,
                         [inter_dc_repl_vnode]},
                        permanent, 5000, worker, [riak_core_vnode_master]},

    InterDcRecvrMaster = { inter_dc_recvr_vnode_master,
                           {riak_core_vnode_master, start_link,
                            [inter_dc_recvr_vnode]},
                           permanent, 5000, worker, [riak_core_vnode_master]},

    ClockSIsTxCoordSup =  { clocksi_static_tx_coord_sup,
                           {clocksi_static_tx_coord_sup, start_link, []},
                           permanent, 5000, supervisor, [clockSI_static_tx_coord_sup]},

    ClockSIiTxCoordSup =  { clocksi_interactive_tx_coord_sup,
                            {clocksi_interactive_tx_coord_sup, start_link, []},
                            permanent, 5000, supervisor,
                            [clockSI_interactive_tx_coord_sup]},

    VectorClockMaster = {vectorclock_vnode_master,
                         {riak_core_vnode_master,  start_link,
                          [vectorclock_vnode]},
                         permanent, 5000, worker, [riak_core_vnode_master]},

    MaterializerMaster = {materializer_vnode_master,
                          {riak_core_vnode_master,  start_link,
                           [materializer_vnode]},
                          permanent, 5000, worker, [riak_core_vnode_master]},

    InterDcManager = {inter_dc_manager,
                        {inter_dc_manager, start_link, []},
                        permanent, 5000, worker, [inter_dc_manager]},

    {ok,
     {{one_for_one, 5, 10},
      [LoggingMaster,
       ClockSIMaster,
       ClockSIsTxCoordSup,
       ClockSIiTxCoordSup,
       InterDcRepMaster,
       InterDcRecvrMaster,
       InterDcManager,
       VectorClockMaster,
       MaterializerMaster]}}.
