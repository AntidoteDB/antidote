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
    {ok,{_,_,CommitTime}}=WriteResult3,
    ReadResult = rpc:call(Node1, floppy, read,
                           [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 3}, ReadResult),

    ReadResult2 = rpc:call(Node3,
                           floppy, clocksi_read,
                           [CommitTime, key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet1],_} }= ReadResult2,
    ?assertEqual(3, ReadSet1),

    ReadResult3 = rpc:call(Node2,
                           floppy, clocksi_read,
                           [CommitTime, key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet2],_} }= ReadResult3,
    ?assertEqual(3, ReadSet2),

    lager:info("Done first round of read, I am gonna append"),
    WriteResult4= rpc:call(Node2,
                           floppy, clocksi_bulk_update,
                           [ CommitTime,
                           [{update, key1, riak_dt_gcounter, {increment, ucl}}]]),
    ?assertMatch({ok, _}, WriteResult4),
    {ok,{_,_,CommitTime2}}=WriteResult4,
    lager:info("Done append in Node2"),
    WriteResult5= rpc:call(Node3,
                           floppy, clocksi_bulk_update,
                           [CommitTime2,
                           [{update, key1, riak_dt_gcounter, {increment, ucl}}]]),
    ?assertMatch({ok, _}, WriteResult5),
    {ok,{_,_,CommitTime3}}=WriteResult5,
    lager:info("Done append in Node3"),
    lager:info("Done waiting, I am gonna read"),

    SnapshotTime =
        CommitTime3,
    ReadResult4 = rpc:call(Node1,
                           floppy, clocksi_read,
                           [SnapshotTime, key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet4],_} }= ReadResult4,
    ?assertEqual(5, ReadSet4),

    ReadResult5 = rpc:call(Node2,
                           floppy, clocksi_read,
                           [SnapshotTime,key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet5],_} }= ReadResult5,
    ?assertEqual(5, ReadSet5),
    ReadResult6 = rpc:call(Node3,
                           floppy, clocksi_read,
                           [SnapshotTime,key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet6],_} }= ReadResult6,
    ?assertEqual(5, ReadSet6),
    pass.
