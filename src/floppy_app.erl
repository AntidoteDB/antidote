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
-module(floppy_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% PB Services
-define(SERVICES, [{floppy_pb_counter, 94, 98},
                   {floppy_pb_set, 99, 101}]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case floppy_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, logging_vnode}]),
            ok = riak_core_node_watcher:service_up(logging, self()),
            %%ClockSI layer

            ok = riak_core:register([{vnode_module, clocksi_vnode}]),
            ok = riak_core_node_watcher:service_up(clocksi, self()),

            ok = riak_core:register(
                   [{vnode_module, clocksi_downstream_generator_vnode}]),
            ok = riak_core_node_watcher:service_up(
                   clocksi_downstream_generator, self()),

            ok = riak_core:register([{vnode_module, vectorclock_vnode}]),
            ok = riak_core_node_watcher:service_up(vectorclock, self()),

            ok = riak_core:register([{vnode_module, materializer_vnode}]),
            ok = riak_core_node_watcher:service_up(materializer, self()),

            ok = riak_core_ring_events:add_guarded_handler(floppy_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(floppy_node_event_handler, []),
            ok = riak_api_pb_service:register(?SERVICES),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
