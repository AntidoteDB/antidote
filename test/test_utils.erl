%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Helium Systems, Inc.  All Rights Reserved.
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

-module(test_utils).
-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, lager_transform}).

-define(TIMEOUT, 60000). %1 minute

-export([at_init_testsuite/0,
         pmap/2,
         wait_until/3,
         wait_until_result/4,
         wait_until_offline/1,
         wait_until_disconnected/2,
         wait_until_connected/2,
         wait_until_registered/2,
         start_node/2,
         connect_cluster/1,
         kill_and_restart_nodes/2,
         kill_nodes/1,
         brutal_kill_nodes/1,
         restart_nodes/2,
         partition_cluster/2,
         heal_cluster/2,
         join_cluster/1,
         set_up_clusters_common/1]).

at_init_testsuite() ->
%% this might help, might not...
    os:cmd(os:find_executable("epmd") ++ " -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _}, _}} -> ok
    end.

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

wait_until_offline(Node) ->
    wait_until(fun() ->
                pang == net_adm:ping(Node)
        end, 60*2, 500).

wait_until_disconnected(Node1, Node2) ->
    wait_until(fun() ->
                pang == rpc:call(Node1, net_adm, ping, [Node2])
        end, 60*2, 500).

wait_until_connected(Node1, Node2) ->
    wait_until(fun() ->
                pong == rpc:call(Node1, net_adm, ping, [Node2])
        end, 60*2, 500).

-spec kill_and_restart_nodes([node()], [tuple()]) -> [node()].
kill_and_restart_nodes(NodeList, Config) ->
    NewNodeList = brutal_kill_nodes(NodeList),
    restart_nodes(NewNodeList, Config).

%% when you just can't wait
-spec brutal_kill_nodes([node()]) -> [node()].
brutal_kill_nodes(NodeList) ->
    lists:map(fun(Node) ->
                  lager:info("Killing node ~p", [Node]),
                  OSPidToKill = rpc:call(Node, os, getpid, []),
                  %% try a normal kill first, but set a timer to
                  %% kill -9 after 5 seconds just in case
                  rpc:cast(Node, timer, apply_after,
                       [5000, os, cmd, [io_lib:format("kill -9 ~s", [OSPidToKill])]]),
                  rpc:cast(Node, os, cmd, [io_lib:format("kill -15 ~s", [OSPidToKill])]),
                  Node
              end, NodeList).

-spec kill_nodes([node()]) -> [node()].
kill_nodes(NodeList) ->
    lists:map(fun(Node) ->
                  %% Crash if stoping fails
                  {ok, Name1} = ct_slave:stop(get_node_name(Node)),
                  Name1
              end, NodeList).

-spec restart_nodes([node()], [tuple()]) -> [node()].
restart_nodes(NodeList, Config) ->
    pmap(fun(Node) ->
             start_node(get_node_name(Node), Config),
             ct:print("Waiting until vnodes are restarted at node ~w", [Node]),
             wait_until_ring_converged([Node]),
             wait_until(Node, fun wait_init:check_ready/1),
             Node
         end, NodeList).

-spec get_node_name(node()) -> atom().
get_node_name(NodeAtom) ->
    Node = atom_to_list(NodeAtom),
    {match, [{Pos, _Len}]} = re:run(Node, "@"),
    list_to_atom(string:substr(Node, 1, Pos)).

start_node(Name, Config) ->
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
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

            ct:print("Node dir: ~p", [NodeDir]),

            ok = rpc:call(Node, application, set_env, [lager, log_root, NodeDir]),
            ok = rpc:call(Node, application, load, [lager]),

            ok = rpc:call(Node, application, load, [riak_core]),

            PlatformDir = NodeDir ++ "/data/",
            RingDir = PlatformDir ++ "/ring/",
            NumberOfVNodes = 4,
            filelib:ensure_dir(PlatformDir),
            filelib:ensure_dir(RingDir),

            ok = rpc:call(Node, application, set_env, [riak_core, riak_state_dir, RingDir]),
            ok = rpc:call(Node, application, set_env, [riak_core, ring_creation_size, NumberOfVNodes]),

            ok = rpc:call(Node, application, set_env, [riak_core, platform_data_dir, PlatformDir]),
            ok = rpc:call(Node, application, set_env, [riak_core, handoff_port, web_ports(Name) + 3]),

            ok = rpc:call(Node, application, set_env, [riak_core, schema_dirs, ["../../_build/default/rel/antidote/lib/"]]),

            ok = rpc:call(Node, application, set_env, [riak_api, pb_port, web_ports(Name) + 2]),
            ok = rpc:call(Node, application, set_env, [riak_api, pb_ip, "127.0.0.1"]),

            ok = rpc:call(Node, application, load, [antidote]),
            ok = rpc:call(Node, application, set_env, [antidote, pubsub_port, web_ports(Name) + 1]),
            ok = rpc:call(Node, application, set_env, [antidote, logreader_port, web_ports(Name)]),
            ok = rpc:call(Node, application, set_env, [antidote, metrics_port, web_ports(Name) + 4]),

            {ok, _} = rpc:call(Node, application, ensure_all_started, [antidote]),
            ct:print("Node ~p started", [Node]),

            Node;
        {error, Reason, Node} ->
            ct:print("Error starting node ~w, reason ~w, will retry", [Node, Reason]),
            ct_slave:stop(Name),
            wait_until_offline(Node),
            start_node(Name, Config)
    end.

partition_cluster(ANodes, BNodes) ->
    pmap(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, canttouchthis]),
                true = rpc:call(Node1, erlang, disconnect_node, [Node2]),
                ok = wait_until_disconnected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.

heal_cluster(ANodes, BNodes) ->
    GoodCookie = erlang:get_cookie(),
    pmap(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, GoodCookie]),
                ok = wait_until_connected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.

% Waits until a certain registered name pops up on the remote node.
wait_until_registered(Node, Name) ->
    ct:print("Wait until ~p is up on ~p", [Name, Node]),
    F = fun() ->
                Registered = rpc:call(Node, erlang, registered, []),
                lists:member(Name, Registered)
        end,
    Delay = rt_retry_delay(),
    Retry = 360000 div Delay,
    wait_until(F, Retry, Delay).

%TODO Move to config
rt_retry_delay() -> 500.

web_ports(dev1) ->
    10015;
web_ports(dev2) ->
    10025;
web_ports(dev3) ->
    10035;
web_ports(dev4) ->
    10045.

pb_ports(Node) ->
    %% remove hostname from Node name
    NodeName = get_node_name(Node),
    web_ports(NodeName) + 2.

%% Build DC
join_cluster(Nodes) ->
    ct:print("Join nodes in to a DC ~p", [Nodes]),
    HeadNode = hd(Nodes),
    create_dc_pb('127.0.0.1', pb_ports(HeadNode), Nodes).

%% Connect DCs for replication
connect_cluster(Nodes) ->
   ct:print("Connect dcs ~p", [Nodes]),
   Descriptors = [descriptor(Node) || Node <- Nodes],
   [ok = connect_to_dcs('127.0.0.1', pb_ports(Node), Descriptors) || Node <- Nodes].

descriptor(Node) ->
  Address = '127.0.0.1',
  Port = pb_ports(Node),
  {ok, Descriptor} = get_connection_descriptor(Address, Port),
  Descriptor.

%% @doc Utility function used to construct test predicates. Retries the
%%      function `Fun' until it returns `true', or until the maximum
%%      number of retries is reached.
wait_until(Fun) when is_function(Fun) ->
    MaxTime = 600000, %% @TODO use config,
        Delay = 1000, %% @TODO use config,
        Retry = MaxTime div Delay,
    wait_until(Fun, Retry, Delay).

%% @doc Convenience wrapper for wait_until for the myriad functions that
%% take a node as single argument.
wait_until(Node, Fun) when is_atom(Node), is_function(Fun) ->
    wait_until(fun() -> Fun(Node) end).

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
    lager:info("Wait until ring converged on ~p", [Nodes]),
    [?assertEqual(ok, wait_until(Node, fun is_ring_ready/1)) || Node <- Nodes],
    ok.

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
owners_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            lager:info("Ring ~p", [Ring]),
            Owners = [Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)],
            lager:info("Owners ~p", [lists:usort(Owners)]),
            lists:usort(Owners);
        {badrpc, _}=BadRpc ->
            lager:info("Badrpc"),
            BadRpc
    end.

%% Build clusters for all test suites.
set_up_clusters_common(Config) ->
   StartDCs = fun(Nodes) ->
                      pmap(fun(N) ->
                              start_node(N, Config)
                           end, Nodes)
                  end,
   Clusters = pmap(fun(N) ->
                  StartDCs(N)
              end, [[dev1, dev2], [dev3], [dev4]]),
   [Cluster1, Cluster2, Cluster3] = Clusters,
   %% Do not join cluster if it is already done
   case owners_according_to(hd(Cluster1)) of % @TODO this is an adhoc check
     Cluster1 -> ok; % No need to build Cluster
     _ ->
        [join_cluster(Cluster) || Cluster <- Clusters],
        Clusterheads = [hd(Cluster) || Cluster <- Clusters],
        connect_cluster(Clusterheads)
   end,
   [Cluster1, Cluster2, Cluster3].

%% Join Nodes to create a DC
create_dc_pb(Address, Port, Nodes) ->
  {ok, Pid} = antidotec_pb_socket:start(Address, Port),
  NodesString = lists:map(fun(Node) ->
                            atom_to_list(Node)
                          end, Nodes),
  Request = antidote_pb_codec:encode(create_dc, NodesString),
  Result = antidotec_pb_socket:call_infinity(Pid, {req, Request, ?TIMEOUT}),
  Response = case Result of
      {error, timeout} ->
          {error, timeout};
      _ ->
          case antidote_pb_codec:decode_response(Result) of
              {opresponse, ok} ->
                  ok;
              {error, Reason} ->
                  {error, Reason};
              Other ->
                  {error, Other}
          end
  end,
  _Disconnected = antidotec_pb_socket:stop(Pid),
  Response.

%% Connect DC in Address/Port to other DCs given by their Descriptors
connect_to_dcs(Address, Port, Descriptors) ->
    {ok, Pid} = antidotec_pb_socket:start(Address, Port),
    Request = antidote_pb_codec:encode(connect_to_dcs, Descriptors),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, Request, ?TIMEOUT}),
    Response = case Result of
        {error, timeout} ->
            {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {opresponse, ok} ->
                    ok;
                {error, Reason} ->
                    {error, Reason};
                Other ->
                    {error, Other}
            end
    end,
    _Disconnected = antidotec_pb_socket:stop(Pid),
    Response.

%% Get the DC descriptor to be given to other DCs
get_connection_descriptor(Address, Port) ->
    {ok, Pid} = antidotec_pb_socket:start(Address, Port),
    Request = antidote_pb_codec:encode(get_connection_descriptor, ignore),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, Request, ?TIMEOUT}),
    Response = case Result of
        {error, timeout} ->
            {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {connection_descriptor, Descriptor} ->
                        {ok, Descriptor};
                {error, Reason} ->
                    {error, Reason};
                Other ->
                    {error, Other}
            end
    end,
    _Disconnected = antidotec_pb_socket:stop(Pid),
    Response.
