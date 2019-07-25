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

%% @doc Interface for antidote-admin commands.
%%

-module(antidote_console).
-include_lib("kernel/include/logger.hrl").

-export([staged_join/1,
         down/1,
         ringready/1]).

-ignore_xref([join/1,
              leave/1,
              remove/1,
              ringready/1]).

%% @doc Staged join operations against a cluster.
staged_join([NodeStr]) ->
    Node = list_to_atom(NodeStr),
    join(NodeStr, fun riak_core:staged_join/1,
         "Success: staged join request for ~p to ~p~n", [node(), Node]).

%% @doc Join a node to a cluster.
join(NodeStr, JoinFn, SuccessFmt, SuccessArgs) ->
    try
        case JoinFn(NodeStr) of
            ok ->
                ?LOG_INFO(SuccessFmt, SuccessArgs),
                ok;
            {error, not_reachable} ->
                ?LOG_ERROR("Node ~s is not reachable", [NodeStr]),
                error;
            {error, different_ring_sizes} ->
                ?LOG_ERROR("Failed: ~s has a different ring_creation_size",
                          [NodeStr]),
                error;
            {error, unable_to_get_join_ring} ->
                ?LOG_ERROR("Failed: Unable to get ring from ~s",
                          [NodeStr]),
                error;
            {error, not_single_node} ->
                ?LOG_ERROR("Failed: This node is already a member of a "
                          "cluster"),
                error;
            {error, self_join} ->
                ?LOG_ERROR("Failed: This node cannot join itself in a "
                          "cluster"),
                error;
            {error, _} ->
                ?LOG_ERROR("Join failed. Try again in a few moments.",
                          []),
                error
        end
    catch
        Exception:Reason ->
            ?LOG_ERROR("Join failed ~p: ~p", [Exception, Reason]),
            error
    end.

%% @doc Mark a node as down.
down([Node]) ->
    try
        case riak_core:down(list_to_atom(Node)) of
            ok ->
                ?LOG_INFO("Success: ~p marked as down", [Node]),
                ok;
            {error, is_up} ->
                ?LOG_ERROR("Failed: ~s is up", [Node]),
                error;
            {error, not_member} ->
                ?LOG_ERROR("Failed: ~p is not a member of the cluster.",
                          [Node]),
                error;
            {error, only_member} ->
                ?LOG_ERROR("Failed: ~p is the only member.", [Node]),
                error
        end
    catch
        Exception:Reason ->
            ?LOG_ERROR("Down failed ~p: ~p", [Exception, Reason]),
            error
    end.

%% @doc Determine whether the ring is ready or not.
ringready([]) ->
    try
        case riak_core_status:ringready() of
            {ok, Nodes} ->
                ?LOG_INFO("All nodes ~p agree on the ring",
                          [Nodes]);
            {error, {different_owners, N1, N2}} ->
                ?LOG_ERROR("Node ~p and ~p list different partition owners",
                          [N1, N2]),
                error;
            {error, {nodes_down, Down}} ->
                ?LOG_ERROR("Node ~p is down",
                          [Down]),
                error
        end
    catch
        Exception:Reason ->
            ?LOG_ERROR("Ring-ready failed with exception ~p: ~p",
                        [Exception, Reason]),
            error
    end.
