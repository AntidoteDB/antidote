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

-export([
    at_init_testsuite/0,
    pmap/2,
    bucket/1,
    init_single_dc/2,
    init_multi_dc/2,
    get_node_name/1,
    web_ports/1,
    restart_nodes/2,
    partition_cluster/3,
    partition/2,
    heal_cluster/2,
    set_up_clusters_common/1,
    unpack/1
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
        test_utils:pmap(fun(N) -> {_Status, Node} = test_utils:start_node(N, Config), Node end, Nodes)
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
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _}, _}} -> ok
    end,
    %% ETS table to map node names to peer controller pids.
    %% peer uses a TCP control channel that works even when Erlang
    %% distribution is disconnected (for partition testing).
    %% Owned by a dedicated holder process so it survives across suites.
    case ets:info(test_peer_pids) of
        undefined ->
            Holder = spawn(fun() -> receive stop -> ok end end),
            ets:new(test_peer_pids, [set, named_table, public, {heir, Holder, []}]);
        _ ->
            ok
    end.


%% ===========================================
%% Node utilities
%% ===========================================

start_node(Name, Config) ->
    {ok, Hostname} = inet:gethostname(),
    Node = list_to_atom(atom_to_list(Name) ++ "@" ++ Hostname),
    %% Check if node is already running (reuse across suites)
    case net_adm:ping(Node) of
        pong ->
            ct:log("Node ~p already started, reusing node", [Node]),
            {ready, Node};
        pang ->
            start_node_fresh(Name, Node, Config)
    end.

start_node_fresh(Name, _Node, Config) ->
    Cookie = atom_to_list(erlang:get_cookie()),
    %% Start node using peer with a TCP control channel so we can
    %% communicate with nodes even during simulated network partitions.
    case peer:start(#{name => Name, connection => 0,
                      args => ["-setcookie", Cookie]}) of
        {ok, PeerPid, Node} ->
            ets:insert(test_peer_pids, {Node, PeerPid}),
            ets:insert(test_peer_pids, {Name, PeerPid}),

            %% code path for compiled dependencies
            CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()) ,
            lists:foreach(fun(P) -> rpc:call(Node, code, add_patha, [P]) end, CodePath),

            % load application to allow for configuring the environment before starting
            ok = rpc:call(Node, application, load, [riak_core]),
            ok = rpc:call(Node, application, load, [antidote_stats]),
            ok = rpc:call(Node, application, load, [ranch]),
            ok = rpc:call(Node, application, load, [antidote]),

            %% get remote working dir of node
            {ok, NodeWorkingDir} = rpc:call(Node, file, get_cwd, []),

            %% DATA DIRS
            ok = rpc:call(Node, application, set_env, [antidote, data_dir, filename:join([NodeWorkingDir, Node, "antidote-data"])]),
            ok = rpc:call(Node, application, set_env, [riak_core, ring_state_dir, filename:join([NodeWorkingDir, Node, "data"])]),
            ok = rpc:call(Node, application, set_env, [riak_core, platform_data_dir, filename:join([NodeWorkingDir, Node, "data"])]),


            %% PORTS
            Port = web_ports(Name),
            ok = rpc:call(Node, application, set_env, [antidote, logreader_port, Port]),
            ok = rpc:call(Node, application, set_env, [antidote, pubsub_port, Port + 1]),
            ok = rpc:call(Node, application, set_env, [ranch, pb_port, Port + 2]),
            ok = rpc:call(Node, application, set_env, [riak_core, handoff_port, Port + 3]),
            ok = rpc:call(Node, application, set_env, [antidote_stats, metrics_port, Port + 4]),


            %% LOGGING Configuration
            %% add additional logging handlers to ensure easy access to remote node logs
            %% for each logging level
            LogRoot = filename:join([NodeWorkingDir, Node, "logs"]),
            %% set the logger configuration
            ok = rpc:call(Node, application, set_env, [antidote, logger, log_config(LogRoot)]),
            %% set primary output level, no filter
            rpc:call(Node, logger, set_primary_config, [level, all]),
            %% load additional logger handlers at remote node
            rpc:call(Node, logger, add_handlers, [antidote]),

            %% redirect slave logs to ct_master logs
            ok = rpc:call(Node, application, set_env, [antidote, ct_master, node()]),
            ConfLog = #{level => debug, formatter => {logger_formatter, #{single_line => true, max_size => 2048}}, config => #{type => standard_io}},
            _ = rpc:call(Node, logger, add_handler, [antidote_redirect_ct, ct_redirect_handler, ConfLog]),


            %% ANTIDOTE Configuration
            %% reduce number of actual log files created to 4, reduces start-up time of node
            ok = rpc:call(Node, application, set_env, [riak_core, ring_creation_size, 4]),
            ok = rpc:call(Node, application, set_env, [antidote, sync_log, true]),

            {ok, _} = rpc:call(Node, application, ensure_all_started, [antidote]),
            ct:pal("Node ~p started with ports ~p-~p", [Node, Port, Port + 4]),

            {connect, Node};
        {error, Reason} ->
            {ok, Hostname} = inet:gethostname(),
            Node = list_to_atom(atom_to_list(Name) ++ "@" ++ Hostname),
            ct:pal("Error starting node ~w, reason ~w, will retry", [Node, Reason]),
            stop_peer(Name),
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
    lists:map(fun(Node) ->
        Name = get_node_name(Node),
        stop_peer(Node),
        Name
    end, NodeList).


%% @doc Send force kill signals to all given nodes
-spec brutal_kill_nodes([node()]) -> [node()].
brutal_kill_nodes(NodeList) ->
    lists:map(fun(Node) ->
                  ct:pal("Killing node ~p", [Node]),
                  stop_peer(Node),
                  Node
              end, NodeList).


%% @doc Stop a peer node by name or node atom.
%% Uses peer:stop if a peer pid is available, otherwise kills via OS signal.
stop_peer(NameOrNode) ->
    case get_peer_pid(NameOrNode) of
        {ok, PeerPid} ->
            catch peer:stop(PeerPid),
            catch ets:delete(test_peer_pids, NameOrNode);
        none ->
            %% No peer pid (node was reused from a previous suite).
            %% Kill via OS signal.
            case rpc:call(NameOrNode, os, getpid, []) of
                OSPid when is_list(OSPid) ->
                    os:cmd("kill -9 " ++ OSPid);
                _ ->
                    ok
            end
    end.


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


%% @doc Partition BNodes from the cluster for DurationMs.
%% Each B node changes its own cookie and disconnects from everyone,
%% then restores the cookie after DurationMs. This avoids overlapping
%% partitions that trigger global's disconnect behavior on OTP 25+.
%% The caller should sleep for DurationMs before calling heal_cluster/2.
partition_cluster(_ANodes, BNodes, DurationMs) ->
    GoodCookie = erlang:get_cookie(),
    {Mod, Bin, File} = code:get_object_code(?MODULE),
    lists:foreach(fun(Node) ->
        case get_peer_pid(Node) of
            {ok, PeerPid} ->
                {module, Mod} = peer:call(PeerPid, code, load_binary, [Mod, File, Bin]),
                peer:cast(PeerPid, ?MODULE, partition, [DurationMs, GoodCookie]);
            none ->
                %% Node was reused from a previous suite — no peer pid.
                %% Use distribution (rpc) to load and trigger the partition.
                {module, Mod} = rpc:call(Node, code, load_binary, [Mod, File, Bin]),
                rpc:cast(Node, ?MODULE, partition, [DurationMs, GoodCookie])
        end
    end, BNodes),
    %% Wait for B nodes to isolate themselves
    timer:sleep(500),
    ok.


%% @doc Isolate this node from the cluster for DurationMs.
%% Changes the node's own cookie (rejecting all connections),
%% disconnects from all peers, waits, then restores the cookie.
partition(DurationMs, GoodCookie) ->
    erlang:set_cookie(node(), canttouchthis),
    [erlang:disconnect_node(N) || N <- nodes()],
    timer:sleep(DurationMs),
    erlang:set_cookie(node(), GoodCookie).


%% @doc Reconnect nodes after the partition has expired.
heal_cluster(ANodes, BNodes) ->
    pmap(fun({Node1, Node2}) ->
        rpc:call(Node1, net_adm, ping, [Node2]),
        ok = time_utils:wait_until_connected(Node1, Node2)
    end, [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.


%% @doc Look up the peer controller pid for a node (if available)
get_peer_pid(Node) ->
    case ets:info(test_peer_pids) of
        undefined -> none;
        _ ->
            case ets:lookup(test_peer_pids, Node) of
                [{_, PeerPid}] -> {ok, PeerPid};
                [] -> none
            end
    end.


web_ports(dev1) -> 10015;
web_ports(dev2) -> 10025;
web_ports(dev3) -> 10035;
web_ports(dev4) -> 10045;
web_ports(clusterdev1) -> 10115;
web_ports(clusterdev2) -> 10125;
web_ports(clusterdev3) -> 10135;
web_ports(clusterdev4) -> 10145;
web_ports(clusterdev5) -> 10155;
web_ports(clusterdev6) -> 10165;
web_ports(dcdev1) -> 10215;
web_ports(dcdev2) -> 10225;
web_ports(dcdev3) -> 10235.


%% Build clusters for all test suites.
set_up_clusters_common(Config) ->
    ClusterAndDcConfiguration = [[dev1, dev2], [dev3], [dev4]],

    StartDCs = fun(Nodes) ->
        %% start each node
        Cl = pmap(fun(N) ->
            start_node(N, Config)
                  end,
            Nodes),
        [{Status, Claimant} | OtherNodes] = Cl,

        %% check if node was reused or not
        case Status of
            ready -> ok;
            connect ->
                ct:pal("Creating a ring for claimant ~p and other nodes ~p", [Claimant, unpack(OtherNodes)]),
                ok = rpc:call(Claimant, antidote_dc_manager, add_nodes_to_dc, [unpack(Cl)])
        end,
        Cl
               end,

    Clusters = pmap(fun(Cluster) ->
        StartDCs(Cluster)
                    end, ClusterAndDcConfiguration),

    %% DCs started, but not connected yet
    pmap(fun([{Status, MainNode} | _] = CurrentCluster) ->
        case Status of
            ready -> ok;
            connect ->
                ct:pal("~p of ~p subscribing to other external DCs", [MainNode, unpack(CurrentCluster)]),

                Descriptors = lists:map(fun([{_Status, FirstNode} | _]) ->
                    {ok, Descriptor} = rpc:call(FirstNode, antidote_dc_manager, get_connection_descriptor, []),
                    Descriptor
                                        end, Clusters),

                %% subscribe to descriptors of other dcs
                ok = rpc:call(MainNode, antidote_dc_manager, subscribe_updates_from, [Descriptors])
        end
         end, Clusters),


    ct:log("Clusters joined and data centers connected connected: ~p", [ClusterAndDcConfiguration]),
    [unpack(DC) || DC <- Clusters].


bucket(BucketBaseAtom) ->
    BucketRandomSuffix = [rand:uniform(127)],
    Bucket = list_to_atom(atom_to_list(BucketBaseAtom) ++ BucketRandomSuffix),
    ct:log("Using random bucket: ~p", [Bucket]),
    Bucket.


%% logger configuration for each level
%% see http://erlang.org/doc/man/logger.html
log_config(LogDir) ->
    DebugConfig = #{level => debug,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "debug.log")}}},

    InfoConfig = #{level => info,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "info.log")}}},

    NoticeConfig = #{level => notice,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "notice.log")}}},

    WarningConfig = #{level => warning,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "warning.log")}}},

    ErrorConfig = #{level => error,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "error.log")}}},

    [
        {handler, debug_antidote, logger_std_h, DebugConfig},
        {handler, info_antidote, logger_std_h, InfoConfig},
        {handler, notice_antidote, logger_std_h, NoticeConfig},
        {handler, warning_antidote, logger_std_h, WarningConfig},
        {handler, error_antidote, logger_std_h, ErrorConfig}
    ].

-spec unpack([{ready | connect, atom()}]) -> [atom()].
unpack(NodesWithStatus) ->
    [Node || {_Status, Node} <- NodesWithStatus].
