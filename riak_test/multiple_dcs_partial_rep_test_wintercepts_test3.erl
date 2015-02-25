-module(multiple_dcs_partial_rep_test_wintercepts_test3).

-export([confirm/0, multiple_writes/4]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

%% Inercept test 3
%% This test blocks the sending of time meta-data
%% So that DCs cannot advance the clocks of external DCs
%% but should not block local progress
%% See inter_dc_Communications_sender_intercepts.hrl for more information
%%
%% Keys 1-15 at DC 1
%% 10-25 at DC 2
%% 20-35 at DC 3


confirm() ->
    [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1,1,1]),
    HeadCluster1 = hd(Cluster1),
    HeadCluster2 = hd(Cluster2),
    HeadCluster3 = hd(Cluster3),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),
    timer:sleep(500), %%TODO: wait for inter_dc_manager to be up
    {ok, DC1} = rpc:call(HeadCluster1, inter_dc_manager, start_receiver,[90001]),
    {ok, DC1Read} = rpc:call(HeadCluster1, inter_dc_manager, start_read_receiver,[80001]),
    {ok, DC1SafeSend} = rpc:call(HeadCluster1, inter_dc_manager, start_safe_send_receiver,[90011]),

    {ok, DC2} = rpc:call(HeadCluster2, inter_dc_manager, start_receiver,[90002]),
    {ok, DC2Read} = rpc:call(HeadCluster2, inter_dc_manager, start_read_receiver,[80002]),
    {ok, DC2SafeSend} = rpc:call(HeadCluster2, inter_dc_manager, start_safe_send_receiver,[90012]),

    {ok, DC3} = rpc:call(HeadCluster3, inter_dc_manager, start_receiver,[90003]),
    {ok, DC3Read} = rpc:call(HeadCluster3, inter_dc_manager, start_read_receiver,[80003]),
    {ok, DC3SafeSend} = rpc:call(HeadCluster3, inter_dc_manager, start_safe_send_receiver,[90013]),

    lager:info("Receivers start results ~p, ~p and ~p", [DC1, DC2, DC3]),
    lager:info("ReceiversRead start results ~p, ~p and ~p", [DC1Read, DC2Read, DC3Read]),
    lager:info("ReceiversSafeSend start results ~p, ~p and ~p", [DC1SafeSend, DC2SafeSend, DC3SafeSend]),

    ok = rpc:call(HeadCluster1, inter_dc_manager, add_list_dcs,[[DC2, DC3]]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, add_list_dcs,[[DC1, DC3]]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, add_list_dcs,[[DC1, DC2]]),

    ok = rpc:call(HeadCluster1, inter_dc_manager, add_list_read_dcs,[[DC2Read, DC3Read]]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, add_list_read_dcs,[[DC1Read, DC3Read]]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, add_list_read_dcs,[[DC1Read, DC2Read]]),

    ok = rpc:call(HeadCluster1, inter_dc_manager, add_list_safe_send_dcs,[[DC2SafeSend, DC3SafeSend]]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, add_list_safe_send_dcs,[[DC1SafeSend, DC3SafeSend]]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, add_list_safe_send_dcs,[[DC1SafeSend, DC2SafeSend]]),

    ok = rpc:call(HeadCluster1, inter_dc_manager, set_replication_keys, [dc1_test_intercepts_test3]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, set_replication_keys, [dc2_test_intercepts_test3]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, set_replication_keys, [dc3_test_intercepts_test3]),

    replication_intercept_test3(Cluster1, Cluster2, Cluster3),
    pass.


replication_intercept_test3(Cluster1, Cluster2, Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Node3 = hd(Cluster3),
    DC1key = key6,
    DC2Key = key16,
    DC3Key = key26,
    
    %% Be sure you can write to each node for keys they replicate
    WriteResult1 = rpc:call(Node1,
			    antidote, append,
			    [DC1Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult1),

    WriteResult2 = rpc:call(Node2,
			    antidote, append,
			    [DC2Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),
    
    WriteResult3 = rpc:call(Node3,
			    antidote, append,
			    [DC3Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult3),
    

    %% Be sure you can write to each node for keys they dont replicate
    WriteResult4 = rpc:call(Node1,
			    antidote, append,
			    [DC2Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult4),
    {ok,{_,_,CommitTime}}=WriteResult4,

    WriteResult5 = rpc:call(Node2,
			    antidote, append,
			    [DC3Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult5),
    WriteResult6 = rpc:call(Node3,
			    antidote, append,
			    [DC1Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult6),



    %%Sleep a bit to let propagation happen
    timer:sleep(5000),

    %% Ensure you can read the keys you replicate and
    %% Don't replicate, but should only read
    %% your local updates

    ReadResult = rpc:call(Node1, antidote, read,
			  [DCKey1, riak_dt_gcounter]),
    ?assertEqual(1, ReadReasult),
    ReadResult2 = rpc:call(Node1, antidote, read,
			  [DCKey2, riak_dt_gcounter]),
    ?assertEqual(1, ReadReasult2),
    ReadResult3 = rpc:call(Node1, antidote, read,
			  [DCKey3, riak_dt_gcounter]),
    ?assertEqual(1, ReadReasult3),


    %% Ensure you can do transactions on your local keys
    %% But reads at external DCs should fail becuase
    %% they haven't recived the updates upto your timestamp
    ReadResult4 = rpc:call(Node1,
			   antidote, clocksi_read,
			   [CommitTime, DCKey1, riak_dt_gcounter]),
    {ok, {_,[ReadSet1],_} }= ReadResult4,
    ?assertEqual(1, ReadSet1),
    lager:info("Done Read in Node1"),

    ReadResult5 = rpc:call(Node1,
			   antidote, clocksi_read,
			   [CommitTime, DCKey2, riak_dt_gcounter]),
    ?assertEqual({error, _}, ReadResult5),
    lager:info("Done Read in Node1").
    
