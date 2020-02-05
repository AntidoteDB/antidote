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

-module(wait_init).

-include_lib("kernel/include/logger.hrl").

-export([check_ready_nodes/1,
         wait_ready/1,
         check_ready/1
        ]).

%% @doc This function takes a list of physical nodes connected to the an
%% instance of the antidote distributed system.  For each of the physical nodes,
%% it checks if all of the vnodes have been initialized, i.e. ets tables
%% and readitem gen_servers have been started.
%% Returns true if all vnodes are initialized for all physical nodes,
%% false otherwise
-spec check_ready_nodes([node()]) -> boolean().
check_ready_nodes(Nodes) ->
    lists:all(fun check_ready/1, Nodes).

%% @doc This calls the check_ready function repeatedly
%% until it returns true.
-spec wait_ready(node()) -> true.
wait_ready(Node) ->
    case check_ready(Node) of
        true ->
            true;
        false ->
            timer:sleep(1000),
            wait_ready(Node)
    end.

%% @doc This function provides the same functionality as check_ready_nodes
%% except it takes as input a single physical node instead of a list
-spec check_ready(node()) -> boolean().
check_ready(Node) ->
    ?LOG_DEBUG("Checking if node ~w is ready ~n", [Node]),
    case rpc:call(Node, clocksi_vnode, check_tables_ready, []) of
        true ->
            case rpc:call(Node, materializer_vnode, check_tables_ready, []) of
                true ->
                    case rpc:call(Node, stable_meta_data_server, check_tables_ready, []) of
                        true ->
                            ?LOG_DEBUG("Node ~p is ready", [Node]),
                            true;
                        false ->
                            ?LOG_DEBUG("Node ~p is not ready", [Node]),
                            false
                    end;
                false ->
                    ?LOG_DEBUG("Node ~p is not ready", [Node]),
                    false
            end;
        false ->
            ?LOG_DEBUG("Checking if node ~p is ready", [Node]),
            false
    end.
