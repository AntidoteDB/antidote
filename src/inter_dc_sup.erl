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

-module(inter_dc_sup).

-behaviour(supervisor).

-include("antidote.hrl").

-export([start_link/0]).
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
    LogResponseReaderSup = {inter_dc_query_response_sup,
        {inter_dc_query_response_sup, start_link, [?INTER_DC_QUERY_CONCURRENCY]},
        permanent, 5000, supervisor,
        [inter_dc_query_response_sup]},

    InterDcPub = ?CHILD(inter_dc_pub, worker, []),
    InterDcSub = ?CHILD(inter_dc_sub, worker, []),
    InterDcQueryReq = ?CHILD(inter_dc_query_req, worker, []),
    InterDcQueryReqRecv = ?CHILD(inter_dc_query_router, worker, []),


    InterDcSubVnode = ?VNODE(inter_dc_sub_vnode_master, inter_dc_sub_vnode),
    InterDcDepVnode = ?VNODE(inter_dc_dep_vnode_master, inter_dc_dep_vnode),
    InterDcLogSenderVnode = ?VNODE(inter_dc_log_sender_vnode_master, inter_dc_log_sender_vnode),

    {ok, {{one_for_one, 5, 10}, [
        LogResponseReaderSup,

        InterDcPub,
        InterDcSub,
        InterDcQueryReq,
        InterDcQueryReqRecv,
        InterDcSubVnode,
        InterDcDepVnode,
        InterDcLogSenderVnode
    ]}}.
