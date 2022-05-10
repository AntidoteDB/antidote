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

-module(riak_utils).

-include_lib("eunit/include/eunit.hrl").

-export([
    is_ring_ready/1,
    wait_until_ring_converged/1,
    wait_until_no_pending_changes/1,
    maybe_wait_for_changes/1,
    owners_according_to/1
]).


%% @doc Calls the riak core ring manager to check if the ring of the given node is ready
-spec is_ring_ready(node()) -> boolean().
is_ring_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} -> riak_core_ring:ring_ready(Ring);
        _ -> false
    end.


%% @doc Given a list of nodes, wait until all nodes believe the ring has
%%      converged (ie. `riak_core_ring:is_ready' returns `true').
-spec wait_until_ring_converged([node()]) -> ok.
wait_until_ring_converged(Nodes) ->
    [?assertEqual(ok, time_utils:wait_until(Node, fun is_ring_ready/1)) || Node <- Nodes],
    ok.


%% @doc Given a list of nodes, wait until all nodes believe there are no
%% on-going or pending ownership transfers.
-spec wait_until_no_pending_changes([node()]) -> ok | fail.
wait_until_no_pending_changes(Nodes) ->
    F = fun() ->
        rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
        {Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
        Changes = [ riak_core_ring:pending_changes(Ring) =:= [] || {ok, Ring} <- Rings ],
        BadNodes =:= [] andalso length(Changes) =:= length(Nodes) andalso lists:all(fun(T) -> T end, Changes)
        end,
    ?assertEqual(ok, time_utils:wait_until(F)),
    ok.


%% @doc TODO
-spec maybe_wait_for_changes(node()) -> ok | fail.
maybe_wait_for_changes(Node) ->
    wait_until_no_pending_changes([Node]).


%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
-spec owners_according_to(node()) -> [node()] | {badrpc, any()}.
owners_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Owners = [Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)],
            lists:usort(Owners);
        {badrpc, _} = BadRpc ->
            BadRpc
    end.
