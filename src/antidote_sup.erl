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
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).
-define(VNODE(I, M), {I, {riak_core_vnode_master, start_link, [M]}, permanent, 5000, worker, [riak_core_vnode_master]}).

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

    ClockSIMaster = { clocksi_vnode_master,
                      {riak_core_vnode_master, start_link, [clocksi_vnode]},
                      permanent, 5000, worker, [riak_core_vnode_master]},

    ClockSIsTxCoordSup =  { clocksi_static_tx_coord_sup,
                           {clocksi_static_tx_coord_sup, start_link, []},
                           permanent, 5000, supervisor, [clockSI_static_tx_coord_sup]},

    ClockSIiTxCoordSup =  { clocksi_interactive_tx_coord_sup,
                            {clocksi_interactive_tx_coord_sup, start_link, []},
                            permanent, 5000, supervisor,
                            [clockSI_interactive_tx_coord_sup]},
    
    ClockSIReadSup = {clocksi_readitem_sup,
    		      {clocksi_readitem_sup, start_link, []},
    		      permanent, 5000, supervisor,
    		      [clocksi_readitem_sup]},
    
    VectorClockMaster = {vectorclock_vnode_master,
                         {riak_core_vnode_master,  start_link,
                          [vectorclock_vnode]},
                         permanent, 5000, worker, [riak_core_vnode_master]},

    MaterializerMaster = {materializer_vnode_master,
                          {riak_core_vnode_master,  start_link,
                           [materializer_vnode]},
                          permanent, 5000, worker, [riak_core_vnode_master]},

    ZMQContextManager = ?CHILD(zmq_context, worker, []),
    InterDcPub = ?CHILD(inter_dc_pub, worker, []),
    InterDcSub = ?CHILD(inter_dc_sub, worker, []),
    InterDcSubVnode = ?VNODE(inter_dc_sub_vnode_master, inter_dc_sub_vnode),
    InterDcDepVnode = ?VNODE(inter_dc_dep_vnode_master, inter_dc_dep_vnode),
    InterDcLogReaderQMaster = ?CHILD(inter_dc_log_reader_query, worker, []),
    InterDcLogReaderRMaster = ?CHILD(inter_dc_log_reader_response, worker, []),
    InterDcLogSenderMaster = ?VNODE(inter_dc_log_sender_master, inter_dc_log_sender),

  {ok,
    {{one_for_one, 5, 10},
      [
        LoggingMaster,
        ClockSIMaster,
        ClockSIsTxCoordSup,
        ClockSIiTxCoordSup,
        ClockSIReadSup,
        VectorClockMaster,
        MaterializerMaster,
        ZMQContextManager,
        InterDcLogSenderMaster,
        InterDcLogReaderQMaster,
        InterDcLogReaderRMaster,
        InterDcPub,
        InterDcSub,
        InterDcSubVnode,
        InterDcDepVnode
      ]
    }
  }.
