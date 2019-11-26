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


%% This module exports methods to build a data center of multiple nodes and
%% connect data centers to start replcation among them.
%%  Usage Example: To create 3 DCs of 2 nodes each:
%%    create_dc(['antidote@node1', 'antidote@node2']),
%%    create_dc(['antidote@node3', 'antidote@node4']),
%%    create_dc(['antidote@node5', 'antidote@node6']),
%%    {ok, Descriptor1} = get_connection_descriptor() % on antidote@node1
%%    {ok, Descriptor2} = get_connection_descriptor() %% on antidote@node3
%%    {ok, Descriptor3} = get_connection_descriptor() %% on antidote@node5
%%    Descriptors = [Descriptor1, Descriptor2, Descriptor3],
%%    Execute subscribe_updates_from(Descriptors) on all 3 DCs

-module(antidote_dc_manager).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
    create_dc/1,
    get_connection_descriptor/0,
    subscribe_updates_from/1]
       ).


%% Build a ring of Nodes forming a data center
-spec create_dc([node()]) -> ok | {error, ring_not_ready}.
create_dc(Nodes) ->
    %% check if ring is ready first
    case riak_core_ring:ring_ready() of
        true -> join_new_nodes(Nodes);
        _ -> {error, ring_not_ready}
    end.


%% Start receiving updates from other DCs
-spec subscribe_updates_from([descriptor()]) -> ok.
subscribe_updates_from(DCDescriptors) ->
    _Connected = inter_dc_manager:observe_dcs_sync(DCDescriptors),
    %%TODO Check return for errors
    ok = inter_dc_manager:dc_successfully_started(),
    ok.


%% Get the DC connection descriptor to be given to other DCs
-spec get_connection_descriptor() -> {ok, descriptor()}.
get_connection_descriptor() ->
    inter_dc_manager:get_descriptor().


%% ---------- Internal Functions --------------

-spec join_new_nodes([node()]) -> ok.
join_new_nodes(Nodes) ->
    %% get the current ring
    {ok, CurrentRing} = riak_core_ring_manager:get_my_ring(),

    %% filter nodes that are not already in this nodes ring
    CurrentNodeMembers = riak_core_ring:all_members(CurrentRing),
    NewNodeMembers = [NewNode || NewNode <- Nodes, not lists:member(NewNode, CurrentNodeMembers)],
    plan_and_commit(NewNodeMembers).


-spec plan_and_commit([node()]) -> ok.
plan_and_commit([]) -> ?LOG_WARNING("No new nodes added to the ring of ~p", [node()]);
plan_and_commit(NewNodeMembers) ->
    lists:foreach(fun(Node) ->
        ?LOG_INFO("Checking if Node ~p is reachable (from ~p)", [Node, node()]),
        pong = net_adm:ping(Node)
                  end, NewNodeMembers),

    lists:foreach(fun(Node) ->
        ?LOG_INFO("Node ~p is joining my ring (~p)", [Node, node()]),
        ok = rpc:call(Node, riak_core, staged_join, [node()])
                  end, NewNodeMembers),

    lists:foreach(fun(Node) ->
        ?LOG_INFO("Checking if node ring is ready (~p)", [Node]),
        wait_until_ring_ready(Node)
                  end, NewNodeMembers),

    {ok, Actions, Transitions} = riak_core_claimant:plan(),
    ?LOG_DEBUG("Actions planned: ~p", [Actions]),
    ?LOG_DEBUG("Ring transitions planned: ~p", [Transitions]),

    %% only after commit returns ok the ring structure will change
    %% even if nothing changes, it returns {error, nothing_planned} indicating some serious error
    ok = riak_core_claimant:commit(),
    ?LOG_NOTICE("Ring committed and ring structure is changing. New ring members: ~p", [NewNodeMembers]),

    %% wait until ring is ready
    wait_until_ring_ready(node()),

    %% wait until ring has no pending changes
    %% this prevents writing to a ring which has not finished its balancing yet and therefore causes
    %% handoffs to be triggered
    %% FIXME this can be removed when #401 and #203 is fixed
    wait_until_ring_no_pending_changes(),


    %% start periodic heart beat
    ok = inter_dc_manager:start_bg_processes(stable_time_functions),
    ok.


%% @doc Wait until all nodes in this ring believe there are no
%% on-going or pending ownership transfers.
-spec wait_until_ring_no_pending_changes() -> ok.
wait_until_ring_no_pending_changes() ->
    {ok, CurrentRing} = riak_core_ring_manager:get_my_ring(),
    Nodes = riak_core_ring:all_members(CurrentRing),

    ?LOG_NOTICE("Wait until no pending changes on ~p", [Nodes]),
    F = fun() ->
        _ = rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
        {Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
        Changes = [ riak_core_ring:pending_changes(Ring) =:= [] || {ok, Ring} <- Rings ],
        BadNodes =:= [] andalso length(Changes) =:= length(Nodes) andalso lists:all(fun(T) -> T end, Changes)
        end,
    case F() of
        true -> ok;
        _ -> timer:sleep(500), wait_until_ring_no_pending_changes()
    end.


-spec wait_until_ring_ready(node()) -> ok.
wait_until_ring_ready(Node) ->
    Status = rpc:call(Node, riak_core_ring, ring_ready, []),
    case Status of
        true -> ok;
        false -> timer:sleep(100), wait_until_ring_ready(Node)
    end.
