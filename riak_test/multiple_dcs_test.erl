-module(multiple_dcs_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1,1,1]),
    HeadCluster1 = hd(Cluster1),
    HeadCluster2 = hd(Cluster2),
    HeadCluster3 = hd(Cluster3),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),
    timer:sleep(500), %%TODO: wait for inter_dc_manager to be up
    {ok, DC1} = rpc:call(HeadCluster1, inter_dc_manager, start_receiver,[8091]),
    {ok, DC2} = rpc:call(HeadCluster2, inter_dc_manager, start_receiver,[8092]),
    {ok, DC3} = rpc:call(HeadCluster3, inter_dc_manager, start_receiver,[8093]),
    lager:info("Receivers start results ~p, ~p and ~p", [DC1, DC2, DC3]),

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
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult3),
    ReadResult = rpc:call(Node1, antidote, read,
                          [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 3}, ReadResult),
    lager:info("Done append in Node1"),
    
    waituntil(fun() ->
                      ReadResult2 = rpc:call(Node3,
                                             antidote, read,
                                             [key1, riak_dt_gcounter]),
                      {ok, ReadSet1}=ReadResult2,
                      3==ReadSet1
              end),
    lager:info("Done Read in Node3"),
    waituntil(fun() ->
                      ReadResult3 = rpc:call(Node2,
                                             antidote, read,
                                             [key1, riak_dt_gcounter]),
                      {ok, ReadSet2}=ReadResult3,
                      3==ReadSet2
              end),
   
    lager:info("Done first round of read, I am gonna append"),
    WriteResult4= rpc:call(Node2,
                           antidote, ec_bulk_update,
                           [[{update, key1, riak_dt_gcounter, {increment, ucl}},{update, key1, riak_dt_gcounter, {increment, ucl}}]]),
    ?assertMatch({ok, _}, WriteResult4),
    lager:info("Done append in Node2"),
    WriteResult5= rpc:call(Node3,
                           antidote, ec_bulk_update,
                           [[{update, key1, riak_dt_gcounter, {increment, ucl}},{update, key1, riak_dt_gcounter, {increment, ucl}}]]),
    ?assertMatch({ok, _}, WriteResult5),
    lager:info("Done append in Node3"),
    lager:info("Done waiting, I am gonna read"),


    waituntil(fun() ->
                      {ok, R1} = rpc:call(Node1,
                                             antidote, read,
                                             [key1, riak_dt_gcounter]),
                      7==R1
              end),
    waituntil(fun() ->
                      {ok,R2} = rpc:call(Node2,
                                             antidote, read,
                                             [key1, riak_dt_gcounter]),
                      7==R2
              end),
    waituntil(fun() ->
                      {ok,R3} = rpc:call(Node3,
                                             antidote, read,
                                             [key1, riak_dt_gcounter]),
                      7==R3
              end),
    
    pass.

waituntil(Fun) ->
    case Fun() of
        true ->
            ok;
        false ->
            Fun()
    end.
