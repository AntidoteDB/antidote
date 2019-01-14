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
-module(antidote_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case antidote_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, logging_vnode}]),
            ok = riak_core_node_watcher:service_up(logging, self()),
            %%ClockSI layer

            ok = riak_core:register([{vnode_module, clocksi_vnode}]),
            ok = riak_core_node_watcher:service_up(clocksi, self()),

            ok = riak_core:register([{vnode_module, materializer_vnode}]),
            ok = riak_core_node_watcher:service_up(materializer, self()),

            ok = riak_core:register([{vnode_module, inter_dc_log_sender_vnode}]),
            ok = riak_core_node_watcher:service_up(logsender, self()),

            ok = riak_core:register([{vnode_module, inter_dc_sub_vnode}]),
            ok = riak_core_node_watcher:service_up(inter_dc_sub, self()),

            ok = riak_core:register([{vnode_module, inter_dc_dep_vnode}]),
            ok = riak_core_node_watcher:service_up(inter_dc_dep, self()),

            ok = riak_core_ring_events:add_guarded_handler(antidote_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(antidote_node_event_handler, []),

            _IsRestart = inter_dc_manager:check_node_restart(),

            case application:get_env(antidote, auto_start_read_servers) of
                {ok, true} ->
                    %% start read servers
                    inter_dc_manager:start_bg_processes(stable);
                _->
                    ok %dont_start_read_servers
            end,
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
