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

-export([create_dc/1,
         get_connection_descriptor/0,
         subscribe_updates_from/1]
       ).

%% Build a ring of Nodes forming a data center
-spec create_dc([node()]) -> ok.
create_dc(Nodes) ->
    logger:info("Creating DC ring ~p", [Nodes]),
    %% Ensure each node owns 100% of it's own ring
    [[Node] = owners_according_to(Node) || Node <- Nodes],
    %% Join nodes
    [Node1|OtherNodes] = Nodes,
    case OtherNodes of
        [] ->
            %% no other nodes, nothing to join/plan/commit
            ok;
        _ ->
            %% ok do a staged join and then commit it, this eliminates the
            %% large amount of redundant handoff done in a sequential join
            [staged_join(Node, Node1) || Node <- OtherNodes],
            plan_and_commit(Node1),
            try_nodes_ready(Nodes, 3, 500)
    end,

    ok = wait_until_nodes_ready(Nodes),

    %% Ensure each node owns a portion of the ring
    wait_until_nodes_agree_about_ownership(Nodes),
    ok = wait_until_no_pending_changes(Nodes),
    wait_until_ring_converged(Nodes),
    wait_until(hd(Nodes), fun wait_init:check_ready/1),
    %% starts metadata services needed for intra-dc communication
    ok = inter_dc_manager:start_bg_processes(stable),
    ok.

%% Start receiving updates from other DCs
-spec subscribe_updates_from([#descriptor{}]) -> ok.
subscribe_updates_from(DCDescriptors) ->
    _Connected = inter_dc_manager:observe_dcs_sync(DCDescriptors),
    %%TODO Check return for errors
    ok = inter_dc_manager:dc_successfully_started(),
    ok.

%% Get the DC connection descriptor to be given to other DCs
-spec get_connection_descriptor() -> {ok, #descriptor{}}.
get_connection_descriptor() ->
    inter_dc_manager:get_descriptor().

%% ---------- Internal Functions --------------

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
owners_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            logger:info("Ring ~p", [Ring]),
            Owners = [Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)],
            logger:info("Owners ~p", [lists:usort(Owners)]),
            lists:usort(Owners);
        {badrpc, Reason} ->
            logger:info("Could not connect to Node ~p", [Node]),
            {badrpc, Reason}
    end.

%% @doc Have `Node' send a join request to `PNode'
staged_join(Node, PNode) ->
    logger:info("[join] ~p to (~p)", [Node, PNode]),
    ok = rpc:call(Node, riak_core, staged_join, [PNode]),
    ok.

plan_and_commit(Node) ->
    logger:info("planning and committing cluster join"),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            logger:info("plan: ring not ready"),
            maybe_wait_for_changes(Node),
            plan_and_commit(Node);
        {ok, _, _} ->
            do_commit(Node)
    end.
do_commit(Node) ->
    logger:info("Committing"),
    case rpc:call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            logger:info("commit: plan changed"),
            maybe_wait_for_changes(Node),
            plan_and_commit(Node);
        {error, ring_not_ready} ->
            logger:info("commit: ring not ready"),
            maybe_wait_for_changes(Node),
            do_commit(Node);
        {error, nothing_planned} ->
            %% Assume plan actually committed somehow
            ok;
        ok ->
            ok
    end.

try_nodes_ready([Node1 | _Nodes], 0, _SleepMs) ->
      logger:info("Nodes not ready after initial plan/commit, retrying"),
      plan_and_commit(Node1);
  try_nodes_ready(Nodes, N, SleepMs) ->
      ReadyNodes = [Node || Node <- Nodes, is_ready(Node) =:= true],
      case ReadyNodes of
          Nodes ->
              ok;
          _ ->
              timer:sleep(SleepMs),
              try_nodes_ready(Nodes, N-1, SleepMs)
      end.

maybe_wait_for_changes(Node) ->
    wait_until_no_pending_changes([Node]).

%% @doc Given a list of nodes, wait until all nodes believe there are no
%% on-going or pending ownership transfers.
-spec wait_until_no_pending_changes([node()]) -> ok | fail.
wait_until_no_pending_changes(Nodes) ->
    logger:info("Wait until no pending changes on ~p", [Nodes]),
    F = fun() ->
                rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
                {Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
                Changes = [ riak_core_ring:pending_changes(Ring) =:= [] || {ok, Ring} <- Rings ],
                BadNodes =:= [] andalso length(Changes) =:= length(Nodes) andalso lists:all(fun(T) -> T end, Changes)
        end,
    ok = wait_until(F),
    ok.

%% @doc Utility function used to construct test predicates. Retries the
%%      function `Fun' until it returns `true', or until the maximum
%%      number of retries is reached.
wait_until(Fun) when is_function(Fun) ->
    MaxTime = 600000, %% @TODO use config,
        Delay = 1000, %% @TODO use config,
        Retry = MaxTime div Delay,
    wait_until(Fun, Retry, Delay).

%% @doc Given a list of nodes, wait until all nodes are considered ready.
%%      See {@link wait_until_ready/1} for definition of ready.
wait_until_nodes_ready(Nodes) ->
    logger:info("Wait until nodes are ready : ~p", [Nodes]),
    [ok = wait_until(Node, fun is_ready/1) || Node <- Nodes],
    ok.

%% @private
is_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            case lists:member(Node, riak_core_ring:ready_members(Ring)) of
                true -> true;
                false -> {not_ready, Node}
            end;
        Other ->
            Other
    end.

wait_until_nodes_agree_about_ownership(Nodes) ->
    logger:info("Wait until nodes agree about ownership ~p", [Nodes]),
    Results = [ wait_until_owners_according_to(Node, Nodes) || Node <- Nodes ],
    lists:all(fun(X) -> ok =:= X end, Results).

%% @doc Convenience wrapper for wait_until for the myriad functions that
%% take a node as single argument.
wait_until(Node, Fun) when is_atom(Node), is_function(Fun) ->
    wait_until(fun() -> Fun(Node) end).

wait_until(Fun, Retry, Delay) when Retry > 0 ->
    wait_until_result(Fun, true, Retry, Delay).

wait_until_result(Fun, Result, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        Result ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until_result(Fun, Result, Retry-1, Delay)
    end.

wait_until_owners_according_to(Node, Nodes) ->
  SortedNodes = lists:usort(Nodes),
  F = fun(N) ->
      owners_according_to(N) =:= SortedNodes
  end,
  ok = wait_until(Node, F),
  ok.

%% @private
is_ring_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            riak_core_ring:ring_ready(Ring);
        _ ->
            false
    end.

%% @doc Given a list of nodes, wait until all nodes believe the ring has
%%      converged (ie. `riak_core_ring:is_ready' returns `true').
wait_until_ring_converged(Nodes) ->
    logger:info("Wait until ring converged on ~p", [Nodes]),
    [ok = wait_until(Node, fun is_ring_ready/1)|| Node <- Nodes],
    ok.
