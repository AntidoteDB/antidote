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
-author("Annette Bieniusa <bieniusa@cs.uni-kl.de>").

-export([%get_cluster_members/1,
         pmap/2,
         wait_until/3,
         %wait_until_left/2,
         %wait_until_joined/2,
         wait_until_offline/1,
         wait_until_disconnected/2,
         wait_until_connected/2,
         wait_until_registered/2,
         start_node/3,
         connect_dcs/1,
         partition_cluster/2,
         heal_cluster/2]).

%get_cluster_members(Node) ->
%    {Node, {ok, Res}} = {Node, rpc:call(Node, plumtree_peer_service_manager, get_local_state, [])},
%    ?SET:value(Res).

pmap(F, L) ->
    Parent = self(),
    lists:foldl(
        fun(X, N) ->
                spawn_link(fun() ->
                            Parent ! {pmap, N, F(X)}
                    end),
                N+1
        end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        true ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry-1, Delay)
    end.

%wait_until_left(Nodes, LeavingNode) ->
%    wait_until(fun() ->
%                lists:all(fun(X) -> X == true end,
%                          pmap(fun(Node) ->
%                                not
%                                lists:member(LeavingNode,
%                                             get_cluster_members(Node))
%                        end, Nodes))
%        end, 60*2, 500).

%wait_until_joined(Nodes, ExpectedCluster) ->
%    wait_until(fun() ->
%                lists:all(fun(X) -> X == true end,
%                          pmap(fun(Node) ->
%                                lists:sort(ExpectedCluster) ==
%                                lists:sort(get_cluster_members(Node))
%                        end, Nodes))
%        end, 60*2, 500).

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

start_node(Name, Config, Case) ->
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
            NodeDir = filename:join([PrivDir, Node, Case]),
            
            ct:print("Node dir: ~p",[NodeDir]),

            ok = rpc:call(Node, application, load, [lager]),
            ok = rpc:call(Node, application, set_env, [lager, log_root, NodeDir]),
            
            ok = rpc:call(Node, application, load, [riak_core]),

            PlatformDir = NodeDir ++ "/data/",
            RingDir = PlatformDir ++ "/ring/",
            filelib:ensure_dir(PlatformDir),
            filelib:ensure_dir(RingDir),
            
            ok = rpc:call(Node, application, set_env, [riak_core, riak_state_dir, RingDir]),
            ok = rpc:call(Node, application, set_env, [riak_core, platform_data_dir, PlatformDir]),
            ok = rpc:call(Node, application, set_env, [riak_core, handoff_port, web_ports(Name) + 3]),
            
            ok = rpc:call(Node, application, set_env, [riak_core, schema_dirs, ["../../_build/default/rel/antidote/lib/"]]),

            ok = rpc:call(Node, application, set_env, [riak_api, pb_port, web_ports(Name) + 2]),
            ok = rpc:call(Node, application, set_env, [riak_api, pb_ip, "127.0.0.1"]),

            ok = rpc:call(Node, application, load, [antidote]),
            ok = rpc:call(Node, application, set_env, [antidote, pubsub_port, web_ports(Name) + 1]),
            ok = rpc:call(Node, application, set_env, [antidote, logreader_port, web_ports(Name)]),

            {ok, _} = rpc:call(Node, application, ensure_all_started, [antidote]),
            %ok = wait_until(fun() ->
            %                case rpc:call(Node, plumtree_peer_service_manager, get_local_state, []) of
            %                    {ok, _Res} -> true;
            %                    _ -> false
            %                end
            %        end, 60, 500),
            Node;
        {error, already_started, Node} ->
            ct_slave:stop(Name),
            wait_until_offline(Node),
            start_node(Name, Config, Case)
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

connect_dcs(Nodes) ->
  Clusters = [Nodes],
  ct:pal("Connecting DC clusters..."),
  lists:foreach(fun(Cluster) ->
    Node1 = hd(Cluster),
    ct:print("Waiting until vnodes start on node ~p", [Node1]),
    wait_until_registered(Node1, inter_dc_pub),
    wait_until_registered(Node1, inter_dc_log_reader_response),
    wait_until_registered(Node1, inter_dc_log_reader_query),
    wait_until_registered(Node1, inter_dc_sub),
    wait_until_registered(Node1, meta_data_sender_sup),
    wait_until_registered(Node1, meta_data_manager_sup),
    ok = rpc:call(Node1, inter_dc_manager, start_bg_processes, [stable])
  end, Clusters),
  Descriptors = descriptors(Clusters),
  Res = [ok || _ <- Clusters],
  lists:foreach(fun(Cluster) ->
    Node = hd(Cluster),
    ct:print("Making node ~p observe other DCs...", [Node]),
    %% It is safe to make the DC observe itself, the observe() call will be ignored silently.
    Res = rpc:call(Node, inter_dc_manager, observe_dcs_sync, [Descriptors])
  end, Clusters),
  ct:pal("DC clusters connected!").


% Waits until a certain registered name pops up on the remote node.
wait_until_registered(Node, _Name) ->
    ct:print("Wait until the ring manager is up on ~p", [Node]),

    F = fun() ->
                Registered = rpc:call(Node, erlang, registered, []),
                lists:member(riak_core_ring_manager, Registered)
        end,
    Delay = rt_retry_delay(),
    Retry = 360000 div Delay,
    ok = wait_until(F, Retry, Delay),
    ok.

descriptors(Clusters) ->
  lists:map(fun(Cluster) ->
    {ok, Descriptor} = rpc:call(hd(Cluster), inter_dc_manager, get_descriptor, []),
    Descriptor
  end, Clusters).

%TODO Move to config
rt_retry_delay() -> 500.

web_ports(dev1) ->
    10015;
web_ports(dev2) ->
    10025.