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
-module(floppy_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    LoggingMaster = {logging_vnode_master,
                     {riak_core_vnode_master, start_link, [logging_vnode]},
                     permanent, 5000, worker, [riak_core_vnode_master]},
    RepMaster = {floppy_rep_vnode_master,
                 {riak_core_vnode_master, start_link, [floppy_rep_vnode]},
                 permanent, 5000, worker, [riak_core_vnode_master]},
    ClockSIMaster = { clocksi_vnode_master,
                      {riak_core_vnode_master, start_link, [clocksi_vnode]},
                      permanent, 5000, worker, [riak_core_vnode_master]},

    InterDcRepMaster = {inter_dc_repl_vnode_master,
                        {riak_core_vnode_master, start_link,
                         [inter_dc_repl_vnode]},
                        permanent, 5000, worker, [riak_core_vnode_master]},
    ClockSITxCoordSup =  { clocksi_tx_coord_sup,
                           {clocksi_tx_coord_sup, start_link, []},
                           permanent, 5000, supervisor, [clockSI_tx_coord_sup]},

    ClockSIiTxCoordSup =  { clocksi_interactive_tx_coord_sup,
                            {clocksi_interactive_tx_coord_sup, start_link, []},
                            permanent, 5000, supervisor,
                            [clockSI_interactive_tx_coord_sup]},

    ClockSIDSGenMaster = { clocksi_downstream_generator_vnode_master,
                           {riak_core_vnode_master,  start_link,
                            [clocksi_downstream_generator_vnode]},
                           permanent, 5000, worker, [riak_core_vnode_master]},

    VectorClockMaster = {vectorclock_vnode_master,
                         {riak_core_vnode_master,  start_link,
                          [vectorclock_vnode]},
                         permanent, 5000, worker, [riak_core_vnode_master]},

    MaterializerMaster = {materializer_vnode_master,
                          {riak_core_vnode_master,  start_link,
                           [materializer_vnode]},
                          permanent, 5000, worker, [riak_core_vnode_master]},

    CoordSup =  {floppy_coord_sup,
                 {floppy_coord_sup, start_link, []},
                 permanent, 5000, supervisor, [floppy_coord_sup]},
    RepSup = {floppy_rep_sup,
              {floppy_rep_sup, start_link, []},
              permanent, 5000, supervisor, [floppy_rep_sup]},
    InterDcRecvr = {inter_dc_recvr,
                    {inter_dc_recvr, start_link, []},
                    permanent, 5000, worker, [inter_dc_recvr]},
    {ok,
     {{one_for_one, 5, 10},
      [LoggingMaster, RepMaster, ClockSIMaster, ClockSITxCoordSup,
       ClockSIiTxCoordSup, InterDcRepMaster, CoordSup, RepSup, InterDcRecvr,
       ClockSIDSGenMaster,
       VectorClockMaster,
       MaterializerMaster]}}.
