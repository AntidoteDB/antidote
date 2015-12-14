-module(eiger_interdc_test).

-export([confirm/0,
         simple_replication_test/2,
         partition_test/2,
         missing_dependency_test/1,
         multiple_keys_test/1,
         concurrent_updates_test/2
         ]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).


confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),

    Clean = rt_config:get(clean_cluster, true),

    Clusters1 = [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1,1,1]),
    Ports = [8091, 8092, 8093],

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),

    ok = common:setup_dc_manager(Clusters1, Ports, true),
    %simple_replication_test(Cluster1, Cluster2),

    Clusters2 = common:clean_clusters(Clusters1),
    ok = common:setup_dc_manager(Clusters2, Ports, Clean),
    %partition_test(Clusters2, Ports),

    Clusters3 = common:clean_clusters(Clusters2),
    ok = common:setup_dc_manager(Clusters3, Ports, Clean),
    %missing_dependency_test(Clusters3),

    Clusters4 = common:clean_clusters(Clusters3),
    ok = common:setup_dc_manager(Clusters4, Ports, Clean),
    %multiple_keys_test(Clusters4),

    Clusters5 = common:clean_clusters(Clusters4),
    ok = common:setup_dc_manager(Clusters5, Ports, Clean),
    concurrent_updates_test(Clusters5, Ports),
    pass.

%read in one dc and read the update from the other one.
simple_replication_test(Cluster1, Cluster2) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Key = simple_replication_test,
    {_, TS1} = WriteResult1 = rpc:call(Node1, antidote, eiger_updatetx,[[{Key, 3}],[]]),
    ?assertMatch({ok, _}, WriteResult1),
    ok=rpc:call(Node2, antidote, eiger_checkdeps,
                    [[{Key, TS1}]]),
    {ok, Result2, _}=rpc:call(Node2, antidote, eiger_readtx, [[Key]]),
    ?assertMatch([{Key, 3}], Result2),
    lager:info("Simple replication test passed!").

%test multiple failure scenarios
partition_test([Cluster1, Cluster2, Cluster3], [_Port1, _Port2, Port3]) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Node3 = hd(Cluster3),
    Key = partition_test,
    
    
    {_, TS1} = WriteResult1 = rpc:call(Node1, antidote, eiger_updatetx,[[{Key, 1}],[]]),
    ?assertMatch({ok, _}, WriteResult1),

    ok=rpc:call(Node3, antidote, eiger_checkdeps,[[{Key, TS1}]]),

    %% Simulate failure of NODE3 by stoping the receiver
    ok = rpc:call(Node3, inter_dc_manager, stop_receiver, []),

    {_, TS2} = WriteResult2 = rpc:call(Node1, antidote, eiger_updatetx,[[{Key, 2}],[]]),
    ?assertMatch({ok, _}, WriteResult2),

    {badrpc, _Reason}=rpc:call(Node3, antidote, eiger_checkdeps,[[{Key, TS2}]], 10000),

    {ok, Result2, _}=rpc:call(Node3, antidote, eiger_readtx, [[Key]]),
    ?assertMatch([{Key, 1}], Result2),

    ok=rpc:call(Node2, antidote, eiger_checkdeps,[[{Key, TS2}]]),

    {ok, Result3, _}=rpc:call(Node2, antidote, eiger_readtx, [[Key]]),
    ?assertMatch([{Key, 2}], Result3),

    %% NODE3 comes back 
    {ok, _} = rpc:call(Node3, inter_dc_manager, start_receiver, [Port3]),

    ok=rpc:call(Node3, antidote, eiger_checkdeps,[[{Key, TS2}]]),

    {ok, Result4, _}=rpc:call(Node3, antidote, eiger_readtx, [[Key]]),
    ?assertMatch([{Key, 2}], Result4),

    lager:info("partition_test test passed!").

%Test that a propagated update gets evetually updated, and it does if
% some dependencies are not satisfied.
missing_dependency_test([Cluster1, Cluster2, _Cluster3]) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Key = missing_dependency_test,
    {_, TS1} = WriteResult1 = rpc:call(Node1, antidote, eiger_updatetx,[[{Key, 1}],[]]),
    ?assertMatch({ok, _}, WriteResult1),

    {_, TS2} = WriteResult2 = rpc:call(Node1, antidote, eiger_updatetx,[[{Key, 2}],[{Key, TS1}]]),
    ?assertMatch({ok, _}, WriteResult2),

    ok=rpc:call(Node2, antidote, eiger_checkdeps, [[{Key, TS2}]]),

    {ok, Result2, _}=rpc:call(Node2, antidote, eiger_readtx, [[Key]]),
    ?assertMatch([{Key, 2}], Result2),
    
    {_, TS3} = WriteResult3 = rpc:call(Node1, antidote, eiger_updatetx,[[{Key, 3}],[{Key, {{node, cluster}, 89}}]]),
    ?assertMatch({ok, _}, WriteResult3),

    {badrpc, _Reason}=rpc:call(Node2, antidote, eiger_checkdeps,[[{Key, TS3}]], 10000),

    {ok, Result3, _}=rpc:call(Node2, antidote, eiger_readtx, [[Key]]),
    ?assertMatch([{Key, 2}], Result3),
    
    lager:info("missing dependency test passed!").

%Test that eiger protocol works with transaction of multiple keys
%and multiple dependencies 
multiple_keys_test([Cluster1, Cluster2, _Cluster3]) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    TotalKeys = 10,
    Keys = k_unique_numes(TotalKeys, 10000),
    KeyRead = multiple_keys_test,

    {_, Operations} = lists:foldl(fun(Key, {C, Acc}) ->
                                    {C + 1, Acc ++ [{Key, C}]}
                                  end, {1, []}, Keys),

    {_, TS1} = WriteResult1 = rpc:call(Node1, antidote, eiger_updatetx,[[{KeyRead, 9}],[]]),
    ?assertMatch({ok, _}, WriteResult1),

    {_, TS2} = WriteResult2 = rpc:call(Node1, antidote, eiger_updatetx,[Operations,[]]),
    ?assertMatch({ok, _}, WriteResult2),

    Dependencies0 = lists:foldl(fun(Key, Acc) ->
                                    Acc ++ [{Key, TS2}]
                                end, [], Keys),
    Dependencies1 = Dependencies0 ++ [{KeyRead, TS1}],

    ok=rpc:call(Node2, antidote, eiger_checkdeps, [Dependencies1]),
    
    {ok, Result2, _}=rpc:call(Node2, antidote, eiger_readtx, [[KeyRead]]),
    ?assertMatch([{KeyRead, 9}], Result2),

    lager:info("multiple keys test passed!").

%Test what happends when clusters concurrently update the same key
concurrent_updates_test([Cluster1, Cluster2, _Cluster3], [Port1, Port2, _Port3]) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    
    Key1 = concurrent_updates_test_key1,
    Key2 = concurrent_updates_test_key2,
    Key3 = concurrent_updates_test_key3,
    Key4 = concurrent_updates_test_key4,

    {_, TS1} = WriteResult1 = rpc:call(Node1, antidote, eiger_updatetx,[[{Key1, 4}, {Key2, 5}],[]]),
    ?assertMatch({ok, _}, WriteResult1),
    
    ok=rpc:call(Node2, antidote, eiger_checkdeps, [[{Key1, TS1}, {Key2, TS1}]]),

    ok = rpc:call(Node2, inter_dc_manager, stop_receiver, []),
    ok = rpc:call(Node1, inter_dc_manager, stop_receiver, []),
    
    {_, TS2} = WriteResult2 = rpc:call(Node1, antidote, eiger_updatetx,[[{Key2, 6}, {Key3, 2}],[]]),
    ?assertMatch({ok, _}, WriteResult2),
    
    {_, TS3} = WriteResult3 = rpc:call(Node2, antidote, eiger_updatetx,[[{Key2, 7}, {Key4, 9}],[]]),
    ?assertMatch({ok, _}, WriteResult3),

    {ok, _} = rpc:call(Node2, inter_dc_manager, start_receiver, [Port2]),
    {ok, _} = rpc:call(Node1, inter_dc_manager, start_receiver, [Port1]),

    ok=rpc:call(Node2, antidote, eiger_checkdeps, [[{Key2, TS2}, {Key3, TS2}]]),
    ok=rpc:call(Node1, antidote, eiger_checkdeps, [[{Key2, TS3}, {Key4, TS3}]]),

    {ok, Result1, _}=rpc:call(Node1, antidote, eiger_readtx, [[Key2]]),
    {ok, Result2, _}=rpc:call(Node2, antidote, eiger_readtx, [[Key2]]),

    lager:info("Result 1: ~p, Result 2: ~p", [Result1, Result2]),
    ?assertMatch(Result1, Result2),

    lager:info("concurrent updates test passed").

k_unique_numes(Num, Range) ->
    Seq = lists:seq(1, Num),
    {L, _} = lists:foldl(fun(_, {L, Set}) ->
                            N = uninum(Range, Set),
                            {[N|L], sets:add_element(N, Set)}
                         end, {[],  sets:new()}, Seq),
    L.

uninum(Range, Set) ->
    R = random:uniform(Range),
    case sets:is_element(R, Set) of
        true ->
            uninum(Range, Set);
        false ->
            R
    end.