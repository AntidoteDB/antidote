%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(antidote_sup).

-behaviour(supervisor).

-include("antidote.hrl").

%% API
-export([start_link/0
        ]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    Gingko = {gingko_vnode_master,
        {riak_core_vnode_master, start_link, [gingko_vnode]},
        permanent, 5000, worker, [riak_core_vnode_master]},

    LoggingMaster = {logging_vnode_master,
                     {riak_core_vnode_master, start_link, [logging_vnode]},
                     permanent, 5000, worker, [riak_core_vnode_master]},

    ClockSIMaster = { clocksi_vnode_master,
                      {riak_core_vnode_master, start_link, [clocksi_vnode]},
                      permanent, 5000, worker, [riak_core_vnode_master]},

    ClockSIiTxCoordSup =  { clocksi_interactive_coord_sup,
                            {clocksi_interactive_coord_sup, start_link, []},
                            permanent, 5000, supervisor,
                            [clockSI_interactive_coord_sup]},

    MaterializerMaster = {materializer_vnode_master,
                          {riak_core_vnode_master,  start_link,
                           [materializer_vnode]},
                          permanent, 5000, worker, [riak_core_vnode_master]},

    BCounterManager = ?CHILD(bcounter_mgr, worker, []),

    StableMetaData = ?CHILD(stable_meta_data_server, worker, []),

    InterDcSup = {inter_dc_sup,
        {inter_dc_sup, start_link, []},
        permanent, 5000, supervisor,
        [inter_dc_sup]},

    MetaDataManagerSup = {meta_data_manager_sup,
                          {meta_data_manager_sup, start_link, [stable_time_functions]},
                          permanent, 5000, supervisor,
                          [meta_data_manager_sup]},

    MetaDataSenderSup = {meta_data_sender_sup,
                         {meta_data_sender_sup, start_link, [[stable_time_functions]]},
                         permanent, 5000, supervisor,
                         [meta_data_sender_sup]},

    PbSup = #{id => antidote_pb_sup,
              start => {antidote_pb_sup, start_link, []},
              restart => permanent,
              shutdown => 5000,
              type => supervisor,
              modules => [antidote_pb_sup]},

    AntidoteStats = ?CHILD(antidote_stats, worker, []),

    {ok,
     {{one_for_one, 5, 10},
      [
       Gingko,
       LoggingMaster,
       ClockSIMaster,
       ClockSIiTxCoordSup,
       MaterializerMaster,
       InterDcSup,
       StableMetaData,
       MetaDataManagerSup,
       MetaDataSenderSup,
       BCounterManager,
       PbSup,
       AntidoteStats
       ]}}.
