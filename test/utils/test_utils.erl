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

-module(test_utils).

-include_lib("eunit/include/eunit.hrl").

-define(FORCE_KILL_TIMER, 1500).
-define(RIAK_SLEEP, 5000).

-export([
    at_init_testsuite/0,
    pmap/2,
    bucket/1,
    init_single_dc/2,
    init_multi_dc/2,
    connect_cluster/1,
    get_node_name/1,
    descriptors/1,
    web_ports/1,
    plan_and_commit/1,
    do_commit/1,
    try_nodes_ready/3,
    wait_until_nodes_ready/1,
    is_ready/1,
    wait_until_nodes_agree_about_ownership/1,
    staged_join/2,
    restart_nodes/2,
    partition_cluster/2,
    heal_cluster/2,
    join_cluster/1,
    set_up_clusters_common/1
]).

%% ===========================================
%% Node utilities
%% ===========================================

-export([
    start_node/2,
    kill_nodes/1,
    kill_and_restart_nodes/2,
    brutal_kill_nodes/1
]).

%% ===========================================
%% Common Test Initialization
%% ===========================================

init_single_dc(Suite, Config) ->
    ct:pal("[~p]", [Suite]),
    test_utils:at_init_testsuite(),

    StartDCs = fun(Nodes) ->
        test_utils:pmap(fun(N) -> test_utils:start_node(N, Config) end, Nodes)
               end,
    [Nodes] = test_utils:pmap( fun(N) -> StartDCs(N) end, [[dev1]] ),
    [Node] = Nodes,

    [{clusters, [Nodes]} | [{nodes, Nodes} | [{node, Node} | Config]]].


init_multi_dc(Suite, Config) ->
    ct:pal("[~p]", [Suite]),

    at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common([{suite_name, ?MODULE} | Config]),
    Nodes = hd(Clusters),
    [{clusters, Clusters} | [{nodes, Nodes} | Config]].


at_init_testsuite() ->
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _}, _}} -> ok
    end.


%% ===========================================
%% Node utilities
%% ===========================================

start_node(Name, Config) ->
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    ct:log("Starting node ~p", [Name]),

    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig = [
        {monitor_master, true},
        {erl_flags, "-smp"}, %% smp for the eleveldb god
        {startup_functions, [
            {code, set_path, [CodePath]}
        ]}],
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            PrivDir = proplists:get_value(priv_dir, Config),
            NodeDir = filename:join([PrivDir, Node]),

            ct:log("Starting riak_core"),
            ok = rpc:call(Node, application, load, [riak_core]),


            PlatformDir = NodeDir ++ "/data/",
            RingDir = PlatformDir ++ "/ring/",
            NumberOfVNodes = 4,
            filelib:ensure_dir(PlatformDir),
            filelib:ensure_dir(RingDir),

            ct:log("Setting environment for riak"),
            ok = rpc:call(Node, application, set_env, [riak_core, riak_state_dir, RingDir]),
            ok = rpc:call(Node, application, set_env, [riak_core, ring_creation_size, NumberOfVNodes]),

            ok = rpc:call(Node, application, set_env, [riak_core, platform_data_dir, PlatformDir]),
            ok = rpc:call(Node, application, set_env, [riak_core, handoff_port, web_ports(Name) + 3]),

            ok = rpc:call(Node, application, set_env, [riak_core, schema_dirs, ["../../_build/default/rel/antidote/lib/"]]),

            ok = rpc:call(Node, application, set_env, [ranch, pb_port, web_ports(Name) + 2]),

            ct:log("Starting antidote"),
            ok = rpc:call(Node, application, load, [antidote]),
            ok = rpc:call(Node, application, set_env, [antidote, pubsub_port, web_ports(Name) + 1]),
            ok = rpc:call(Node, application, set_env, [antidote, logreader_port, web_ports(Name)]),
            ok = rpc:call(Node, application, set_env, [antidote, metrics_port, web_ports(Name) + 4]),

            {ok, _} = rpc:call(Node, application, ensure_all_started, [antidote]),
            ct:pal("Node ~p started with ports ~p-~p", [Node, web_ports(Name), web_ports(Name)+4]),

            Node;
        {error, already_started, Node} ->
            ct:log("Node ~p already started, reusing node", [Node]),
            Node;
        {error, Reason, Node} ->
            ct:pal("Error starting node ~w, reason ~w, will retry", [Node, Reason]),
            ct_slave:stop(Name),
            time_utils:wait_until_offline(Node),
            start_node(Name, Config)
    end.


%% @doc Forces shutdown of nodes and restarts them again with given configuration
-spec kill_and_restart_nodes([node()], [tuple()]) -> [node()].
kill_and_restart_nodes(NodeList, Config) ->
    NewNodeList = brutal_kill_nodes(NodeList),
    restart_nodes(NewNodeList, Config).


%% @doc Kills all given nodes, crashes if one node cannot be stopped
-spec kill_nodes([node()]) -> [node()].
kill_nodes(NodeList) ->
    lists:map(fun(Node) -> {ok, Name} = ct_slave:stop(get_node_name(Node)), Name end, NodeList).


%% @doc Send force kill signals to all given nodes
-spec brutal_kill_nodes([node()]) -> [node()].
brutal_kill_nodes(NodeList) ->
    lists:map(fun(Node) ->
                  ct:pal("Killing node ~p", [Node]),
                  OSPidToKill = rpc:call(Node, os, getpid, []),
                  %% try a normal kill first, but set a timer to
                  %% kill -9 after X seconds just in case
%%                  rpc:cast(Node, timer, apply_after,
%%                      [?FORCE_KILL_TIMER, os, cmd, [io_lib:format("kill -9 ~s", [OSPidToKill])]]),
                  ct_slave:stop(get_node_name(Node)),
                  rpc:cast(Node, os, cmd, [io_lib:format("kill -15 ~s", [OSPidToKill])]),
                  Node
              end, NodeList).


%% @doc Restart nodes with given configuration
-spec restart_nodes([node()], [tuple()]) -> [node()].
restart_nodes(NodeList, Config) ->
    pmap(fun(Node) ->
        ct:pal("Restarting node ~p", [Node]),

        ct:log("Starting and waiting until vnodes are restarted at node ~w", [Node]),
        start_node(get_node_name(Node), Config),

        ct:log("Waiting until ring converged @ ~p", [Node]),
        riak_utils:wait_until_ring_converged([Node]),

        ct:log("Waiting until ready @ ~p", [Node]),
        time_utils:wait_until(Node, fun wait_init:check_ready/1),
        Node
         end, NodeList).


%% @doc Convert node to node atom
-spec get_node_name(node()) -> atom().
get_node_name(NodeAtom) ->
    Node = atom_to_list(NodeAtom),
    {match, [{Pos, _Len}]} = re:run(Node, "@"),
    list_to_atom(string:substr(Node, 1, Pos)).


%% @doc TODO
-spec pmap(fun(), list()) -> list().
pmap(F, L) ->
    Parent = self(),
    lists:foldl(
        fun(X, N) ->
            spawn_link(fun() ->
                           Parent ! {pmap, N, F(X)}
                       end),
            N+1
        end, 0, L),
    L2 = [receive {pmap, N, R} -> {N, R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.


partition_cluster(ANodes, BNodes) ->
    pmap(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, canttouchthis]),
                true = rpc:call(Node1, erlang, disconnect_node, [Node2]),
                ok = time_utils:wait_until_disconnected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.


heal_cluster(ANodes, BNodes) ->
    GoodCookie = erlang:get_cookie(),
    pmap(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, GoodCookie]),
                ok = time_utils:wait_until_connected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.


connect_cluster(Nodes) ->
  Clusters = [[Node] || Node <- Nodes],
  ct:log("Connecting DC clusters..."),

  pmap(fun(Cluster) ->
              Node1 = hd(Cluster),
              ct:log("Waiting until vnodes start on node ~p", [Node1]),
              time_utils:wait_until_registered(Node1, inter_dc_pub),
              time_utils:wait_until_registered(Node1, inter_dc_query_receive_socket),
              time_utils:wait_until_registered(Node1, inter_dc_query_response_sup),
              time_utils:wait_until_registered(Node1, inter_dc_query),
              time_utils:wait_until_registered(Node1, inter_dc_sub),
              time_utils:wait_until_registered(Node1, meta_data_sender_sup),
              time_utils:wait_until_registered(Node1, meta_data_manager_sup),
              ok = rpc:call(Node1, inter_dc_manager, start_bg_processes, [stable]),
              ok = rpc:call(Node1, logging_vnode, set_sync_log, [true])
          end, Clusters),

    Descriptors = descriptors(Clusters),
    Res = [ok || _ <- Clusters],
    pmap(fun(Cluster) ->
              Node = hd(Cluster),
              ct:log("Making node ~p observe other DCs...", [Node]),
              %% It is safe to make the DC observe itself, the observe() call will be ignored silently.
              Res = rpc:call(Node, inter_dc_manager, observe_dcs_sync, [Descriptors])
          end, Clusters),
    pmap(fun(Cluster) ->
              Node = hd(Cluster),
              ok = rpc:call(Node, inter_dc_manager, dc_successfully_started, [])
          end, Clusters),
    ct:log("DC clusters connected!").


descriptors(Clusters) ->
  lists:map(fun(Cluster) ->
    {ok, Descriptor} = rpc:call(hd(Cluster), inter_dc_manager, get_descriptor, []),
    Descriptor
  end, Clusters).


web_ports(dev1) -> 10015;
web_ports(dev2) -> 10025;
web_ports(dev3) -> 10035;
web_ports(dev4) -> 10045.

%% Build clusters
join_cluster(Nodes) ->
    ct:log("Joining: ~p", [Nodes]),
    %% Ensure each node owns 100% of it's own ring
    [?assertEqual([Node], riak_utils:owners_according_to(Node)) || Node <- Nodes],
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

    ct:log("Wait until nodes read"),
    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),

    %% Ensure each node owns a portion of the ring
    ct:log("Wait until agree about ownership"),
    wait_until_nodes_agree_about_ownership(Nodes),

    ct:log("Wait until no pending changes"),
    ?assertEqual(ok, riak_utils:wait_until_no_pending_changes(Nodes)),

    ct:log("Wait until ring converged"),
    riak_utils:wait_until_ring_converged(Nodes),

    ct:log("Check if nodes are fully ready"),
    time_utils:wait_until(hd(Nodes), fun wait_init:check_ready/1),
    ok.



%% @doc Have `Node' send a join request to `PNode'
staged_join(Node, PNode) ->
    timer:sleep(100),
    R = rpc:call(Node, riak_core, staged_join, [PNode]),
    ct:log("[join] ~p to (~p): ~p", [Node, PNode, R]),
    ?assertEqual(ok, R),
    ok.


plan_and_commit(Node) ->
    timer:sleep(100),
    ct:log("planning and committing cluster join"),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            ct:log("plan: ring not ready"),
            riak_utils:maybe_wait_for_changes(Node),
            plan_and_commit(Node);
        {ok, _, _} ->
            do_commit(Node)
    end.


do_commit(Node) ->
    ct:log("Committing"),
    case rpc:call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            ct:log("commit: plan changed"),
            timer:sleep(100),
            riak_utils:maybe_wait_for_changes(Node),
            plan_and_commit(Node);
        {error, ring_not_ready} ->
            ct:log("commit: ring not ready"),
            timer:sleep(100),
            riak_utils:maybe_wait_for_changes(Node),
            do_commit(Node);
        {error, nothing_planned} ->
            %% Assume plan actually committed somehow
            ok;
        ok ->
            ok
    end
.

try_nodes_ready([Node1 | _Nodes], 0, _SleepMs) ->
      ct:log("Nodes not ready after initial plan/commit, retrying"),
      plan_and_commit(Node1);
try_nodes_ready(Nodes, N, SleepMs) ->
  ReadyNodes = [Node || Node <- Nodes, is_ready(Node) =:= true],
  case ReadyNodes of
      Nodes ->
          ok;
      _ ->
          try_nodes_ready(Nodes, N-1, SleepMs)
  end.


%% @doc Given a list of nodes, wait until all nodes are considered ready.
%%      See {@link wait_until_ready/1} for definition of ready.
wait_until_nodes_ready(Nodes) ->
    ct:log("Wait until nodes are ready : ~p", [Nodes]),
    [?assertEqual(ok, time_utils:wait_until(Node, fun is_ready/1)) || Node <- Nodes],
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
    ct:log("Wait until nodes agree about ownership ~p", [Nodes]),
    Results = [ time_utils:wait_until_owners_according_to(Node, Nodes) || Node <- Nodes ],
    ?assert(lists:all(fun(X) -> ok =:= X end, Results)).


%% Build clusters for all test suites.
set_up_clusters_common(Config) ->
    ct:log("Building cluster"),

    StartDCs = fun(Nodes) ->
                      pmap(fun(N) -> start_node(N, Config) end, Nodes)
                  end,

    Clusters = pmap(
            fun(N) -> StartDCs(N) end,
            [[dev1, dev2], [dev3], [dev4]]
        ),


   [Cluster1, Cluster2, Cluster3] = Clusters,
   %% Do not join cluster if it is already done
   case riak_utils:owners_according_to(hd(Cluster1)) of % @TODO this is an adhoc check
     Cluster1 ->
         ok; % No need to build Cluster
     _ ->
        [join_cluster(Cluster) || Cluster <- Clusters],
        Clusterheads = [hd(Cluster) || Cluster <- Clusters],
        connect_cluster(Clusterheads)
   end,

   ct:log("Cluster joined and connected: ~p  ~p  ~p", [Cluster1, Cluster2, Cluster3]),
   [Cluster1, Cluster2, Cluster3].


bucket(BucketBaseAtom) ->
    BucketRandomSuffix = [rand:uniform(127)],
    Bucket = list_to_atom(atom_to_list(BucketBaseAtom) ++ BucketRandomSuffix),
    ct:log("Using random bucket: ~p", [Bucket]),
    Bucket.
