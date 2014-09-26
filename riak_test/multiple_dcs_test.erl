-module(multiple_dcs_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1,1,1]),
    HeadCluster1 = hd(Cluster1),
    HeadCluster2 = hd(Cluster2),
    HeadCluster3 = hd(Cluster3),

    {ok, DC1} = rpc:call(HeadCluster1, inter_dc_manager, start_receiver,[8091]),
    {ok, DC2} = rpc:call(HeadCluster2, inter_dc_manager, start_receiver,[8092]),
    {ok, DC3} = rpc:call(HeadCluster3, inter_dc_manager, start_receiver,[8093]),
    lager:info("Receivers start results ~p, ~p and ~p", [DC1, DC2, DC3]),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),
    
    
    ok = rpc:call(HeadCluster1, inter_dc_manager, add_list_dcs,[[DC2, DC3]]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, add_list_dcs,[[DC1, DC3]]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, add_list_dcs,[[DC1, DC2]]),
    
    simple_replication_test(Cluster1, Cluster2, Cluster3),
    pass.

simple_replication_test(Cluster1, Cluster2, Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Node3 = hd(Cluster3),
   % Result = rpc:call(Node2, inter_dc_communication_sup, start_link,[8091]),
   % lager:info("Sup start result ~p", [Result]),
    %%timer:sleep(10000), %% REMOVE this
    WriteResult1 = rpc:call(Node1,
                           floppy, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                           floppy, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
                           floppy, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult3),
    ReadResult = rpc:call(Node1, floppy, read,
                           [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 3}, ReadResult),

    timer:sleep(10000),
    ReadResult2 = rpc:call(Node3,
                           floppy, read,
                           [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 3}, ReadResult2),
    ReadResult3 = rpc:call(Node2,
                           floppy, read,
                           [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 3}, ReadResult3),
    lager:info("Done first round of read, I am gonna append"),
    WriteResult4= rpc:call(Node2,
                           floppy, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult4),
    lager:info("Done append in Node2"),
    WriteResult5= rpc:call(Node3,
                           floppy, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult5),
    lager:info("Done append in Node3"),
    timer:sleep(10000),
    lager:info("Done waiting, I am gonna read"),
    ReadResult3 = rpc:call(Node1,
                           floppy, read,
                           [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 5}, ReadResult3),
    ReadResult4 = rpc:call(Node2,
                           floppy, read,
                           [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 5}, ReadResult4),
    ReadResult5 = rpc:call(Node3,
                           floppy, read,
                           [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 5}, ReadResult5),
    pass.
