#!/usr/bin/env escript
%% -*- erlang -*-

-mode(compile).

-export([
    exec_join/2
]).

%% Usage: join_nodes.erl createdc 'script@host' 'antidote@host1' 'antidote@host2'
%%        join_nodes.erl connectdcs 'script@host' 'antidote@host1' 'antidote@host2'
%% createdc : connects nodes with in a dc to distribute data among the nodes.
%% connectdc : connect two dcs to start replicating data between them.
main([Func, ScriptNodeName | AntidoteNodeNames]) ->
    case net_kernel:start([list_to_atom(ScriptNodeName), longnames]) of
        {ok, _Pid} ->
            io:format("Script starting at node ~p", [ScriptNodeName]),
            erlang:set_cookie(node(), antidote),
            Nodes =
                try
                    lists:foldl(fun(NodeString, Acc) ->
                        Node = list_to_atom(NodeString),
                        lists:append([Node], Acc)
                    end, [], AntidoteNodeNames)
                catch
                    E:R  ->
                        io:format("~p, ~p", [E,R]),
                        bad_input_format
                end,
            case Nodes of
                bad_input_format ->
                    usage();
                _->
                    exec_join(Func, Nodes)
            end;
        {error, Reason} ->
            io:format("Could not start script. ~p", [Reason])
    end.

exec_join("connectdcs", ClusterHeads) ->
  io:format("~nSTARTING SCRIPT TO JOIN DCs OF NODES:~n~p~n", [ClusterHeads]),
  connect_cluster(ClusterHeads),
  io:format("~nSuccesfully connected clusters: ~w~n", [ClusterHeads]),
  io:format("~nSUCCESS! Finished connecting DC clusters!~n");

exec_join("createdc", Nodes) ->
      io:format("~nSTARTING SCRIPT TO CREATE CLUSTER OF NODES:~n~p~n", [Nodes]),
      %lists:foreach(fun (Node) -> erlang:set_cookie(Node, antidote) end, Nodes),
      join_cluster(Nodes),
      lists:foreach(fun (Node) -> rpc:call(Node, inter_dc_manager, start_bg_processes, [stable]) end, Nodes),
      io:format("~nSuccesfully joined nodes: ~w~n", [Nodes]),
      io:format("~nSUCCESS! Finished building cluster!~n").

usage() ->
    io:format("Usage join_dcs_script.erl <scriptnodename> <antidote@node1> <antidote@node2> .. ~n"),
    io:format("Example join_dcs_script.erl 'script@10.2.1.1 'antidote1@1.2.3.4' 'antidote2@5.6.7.8'"),
    halt(1).

-include_lib("eunit/include/eunit.hrl").

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

connect_cluster(Nodes) ->
    Clusters = [[Node] || Node <- Nodes],
    ct:pal("Connecting DC clusters..."),

    pmap(fun(Cluster) ->
        Node1 = hd(Cluster),
        io:format("Waiting until vnodes start on node ~p~n", [Node1]),
        wait_until_registered(Node1, inter_dc_pub),
        wait_until_registered(Node1, inter_dc_query_receive_socket),
        wait_until_registered(Node1, inter_dc_query_response_sup),
        wait_until_registered(Node1, inter_dc_query),
        wait_until_registered(Node1, inter_dc_sub),
        wait_until_registered(Node1, meta_data_sender_sup),
        wait_until_registered(Node1, meta_data_manager_sup),
        ok = rpc:call(Node1, inter_dc_manager, start_bg_processes, [stable]),
        ok = rpc:call(Node1, logging_vnode, set_sync_log, [true])
    end, Clusters),
    Descriptors = descriptors(Clusters),
    io:format("the clusters ~w~n", [Clusters]),
    Res = [ok || _ <- Clusters],
    pmap(fun(Cluster) ->
        Node = hd(Cluster),
        io:format("Making node ~p observe other DCs...~n", [Node]),
        %% It is safe to make the DC observe itself, the observe() call will be ignored silently.
        Res = rpc:call(Node, inter_dc_manager, observe_dcs_sync, [Descriptors])
    end, Clusters),
    pmap(fun(Cluster) ->
        Node = hd(Cluster),
        ok = rpc:call(Node, inter_dc_manager, dc_successfully_started, [])
    end, Clusters),
    ct:pal("DC clusters connected!").

% Waits until a certain registered name pops up on the remote node.
wait_until_registered(Node, Name) ->
    io:format("Wait until ~p is up on ~p~n", [Name,Node]),
    F = fun() ->
        Registered = rpc:call(Node, erlang, registered, []),
        lists:member(Name, Registered)
    end,
    Delay = rt_retry_delay(),
    Retry = 360000 div Delay,
    wait_until(F, Retry, Delay).

descriptors(Clusters) ->
    lists:map(fun(Cluster) ->
        {ok, Descriptor} = rpc:call(hd(Cluster), inter_dc_manager, get_descriptor, []),
        Descriptor
    end, Clusters).

%TODO Move to config
rt_retry_delay() -> 500.

%% Build clusters
join_cluster(Nodes) ->
    %% Ensure each node owns 100% of it's own ring
    [?assertEqual([Node], owners_according_to(Node, hd(Nodes))) || Node <- Nodes],
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

    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),

    %% Ensure each node owns a portion of the ring
    wait_until_nodes_agree_about_ownership(Nodes),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)),
    wait_until_ring_converged(Nodes),
    wait_until(hd(Nodes),fun check_ready/1),
    ok.

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
owners_according_to(Node, MainNode) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
%%            io:format("Ring ~p~n", [Ring]),
            Owners = [Owner || {_Idx, Owner} <- rpc:call(MainNode, riak_core_ring, all_owners, [Ring])],
            io:format("Owners ~p~n", [lists:usort(Owners)]),
            lists:usort(Owners);
        {badrpc, _}=BadRpc ->
%%            io:format("Badrpc"),
            BadRpc
    end.

%% @doc Have `Node' send a join request to `PNode'
staged_join(Node, PNode) ->
    timer:sleep(5000),
    R = rpc:call(Node, riak_core, staged_join, [PNode]),
    io:format("[join] ~p to (~p): ~p~n", [Node, PNode, R]),
    ?assertEqual(ok, R),
    ok.

plan_and_commit(Node) ->
    timer:sleep(5000),
    io:format("planning and committing cluster join"),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            io:format("plan: ring not ready"),
            timer:sleep(5000),
            maybe_wait_for_changes(Node),
            plan_and_commit(Node);
        {ok, _, _} ->
            do_commit(Node)
    end.
do_commit(Node) ->
    io:format("Committing"),
    case rpc:call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            io:format("commit: plan changed"),
            timer:sleep(100),
            maybe_wait_for_changes(Node),
            plan_and_commit(Node);
        {error, ring_not_ready} ->
            io:format("commit: ring not ready"),
            timer:sleep(100),
            maybe_wait_for_changes(Node),
            do_commit(Node);
        {error,nothing_planned} ->
            %% Assume plan actually committed somehow
            ok;
        ok ->
            ok
    end.

try_nodes_ready([Node1 | _Nodes], 0, _SleepMs) ->
    io:format("Nodes not ready after initial plan/commit, retrying"),
    plan_and_commit(Node1);
try_nodes_ready(Nodes, N, SleepMs) ->
    ReadyNodes = [Node || Node <- Nodes, is_ready(Node, hd(Nodes)) =:= true],
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
%%    io:format("Wait until no pending changes on ~p~n", [Nodes]),
    F = fun() ->
        rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
        {Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
        Changes = [rpc:call(hd(Nodes), riak_core_ring, pending_changes, [Ring]) =:= [] || {ok, Ring} <- Rings ],
        BadNodes =:= [] andalso length(Changes) =:= length(Nodes) andalso lists:all(fun(T) -> T end, Changes)
    end,
    ?assertEqual(ok, wait_until(F)),
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
%%    io:format("Wait until nodes are ready : ~p~n", [Nodes]),
    [?assertEqual(ok, wait_until(Node, fun is_ready/2)) || Node <- Nodes, hd(Nodes)],
    ok.

%% @private
is_ready(Node, MainNode) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            case lists:member(Node, rpc:call(MainNode, riak_core_ring, ready_members, [Ring])) of
                true -> true;
                false -> {not_ready, Node}
            end;
        Other ->
            Other
    end.

wait_until_nodes_agree_about_ownership(Nodes) ->
%%    io:format("Wait until nodes agree about ownership ~p~n", [Nodes]),
    Results = [ wait_until_owners_according_to(Node, Nodes) || Node <- Nodes ],
    ?assert(lists:all(fun(X) -> ok =:= X end, Results)).

%% @doc Convenience wrapper for wait_until for the myriad functions that
%% take a node as single argument.
wait_until(Node, Fun) when is_atom(Node), is_function(Fun) ->
    wait_until(fun() -> Fun(Node) end).

wait_until_owners_according_to(Node, Nodes) ->
    SortedNodes = lists:usort(Nodes),
    F = fun(N) ->
        owners_according_to(N, hd(Nodes)) =:= SortedNodes
    end,
    ?assertEqual(ok, wait_until(Node, F)),
    ok.

%% @private
is_ring_ready(Node, MainNode) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            rpc:call(MainNode, riak_core_ring, ring_ready, [Ring]);
        _ ->
            false
    end.

%% @doc Given a list of nodes, wait until all nodes believe the ring has
%%      converged (ie. `riak_core_ring:is_ready' returns `true').
wait_until_ring_converged(Nodes) ->
%%    io:format("Wait until ring converged on ~p~n", [Nodes]),
    [?assertEqual(ok, wait_until(Node, fun is_ring_ready/2)) || Node <- Nodes, hd(Nodes)],
    ok.

%% @doc This function takes a list of pysical nodes connected to the an
%% instance of the antidote distributed system.  For each of the phyisical nodes
%% it checks if all of the vnodes have been initialized, meaning ets tables
%% and gen_servers serving read have been started.
%% Returns true if all vnodes are initialized for all phyisical nodes,
%% false otherwise
-spec check_ready_nodes([node()]) -> true.
check_ready_nodes(Nodes) ->
    lists:all(fun check_ready/1, Nodes).

%% @doc This calls the check_ready function repatabliy
%% until it returns true
-spec wait_ready(node()) -> true.
wait_ready(Node) ->
    case check_ready(Node) of
        true ->
            true;
        false ->
            timer:sleep(1000),
            check_ready(Node)
    end.

%% @doc This function provides the same functionality as wait_ready_nodes
%% except it takes as input a sinlge physical node instead of a list
-spec check_ready(node()) -> boolean().
check_ready(Node) ->
    io:format("Checking if node ~w is ready ~n~n", [Node]),
    case rpc:call(Node,clocksi_vnode,check_tables_ready,[]) of
        true ->
            case rpc:call(Node,materializer_vnode,check_tables_ready,[]) of
                true ->
                    case rpc:call(Node,stable_meta_data_server,check_tables_ready,[]) of
                        true ->
                            io:format("Node ~w is ready! ~n~n", [Node]),
                            true;
                        false ->
                            io:format("Node ~w is not ready ~n~n", [Node]),
                            false
                    end;
                false ->
                    io:format("Node ~w is not ready ~n~n", [Node]),
                    false
            end;
        false ->
            io:format("Checking if node ~w is ready ~n~n", [Node]),
            false
    end.
