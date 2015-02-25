-module(multiple_dcs_partial_rep_test_wintercepts).

-export([confirm/0, multiple_writes/4]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

%% Inercept test 1
%% This test performs updates to key5 through
%% DC1 which does not replicate that key
%% Cross DC updates are prevented by an intercept
%% Checks are then done to be sure reads are still performed correctly
%% See inter_dc_communication_sender_intercepts.hrl for more details
%%
%% key5 is not replicated at DC1, but at DC2 and DC3


confirm() ->
    [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1,1,1]),
    HeadCluster1 = hd(Cluster1),
    HeadCluster2 = hd(Cluster2),
    HeadCluster3 = hd(Cluster3),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),
    timer:sleep(500), %%TODO: wait for inter_dc_manager to be up
    {ok, DC1} = rpc:call(HeadCluster1, inter_dc_manager, start_receiver,[90021]),
    {ok, DC1Read} = rpc:call(HeadCluster1, inter_dc_manager, start_read_receiver,[80001]),
    {ok, DC1SafeSend} = rpc:call(HeadCluster1, inter_dc_manager, start_safe_send_receiver,[90001]),

    {ok, DC2} = rpc:call(HeadCluster2, inter_dc_manager, start_receiver,[90022]),
    {ok, DC2Read} = rpc:call(HeadCluster2, inter_dc_manager, start_read_receiver,[80002]),
    {ok, DC2SafeSend} = rpc:call(HeadCluster2, inter_dc_manager, start_safe_send_receiver,[90002]),

    {ok, DC3} = rpc:call(HeadCluster3, inter_dc_manager, start_receiver,[90023]),
    {ok, DC3Read} = rpc:call(HeadCluster3, inter_dc_manager, start_read_receiver,[80003]),
    {ok, DC3SafeSend} = rpc:call(HeadCluster3, inter_dc_manager, start_safe_send_receiver,[90003]),

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

    ok = rpc:call(HeadCluster1, inter_dc_manager, set_replication_keys, [dc1_test_with_intercepts]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, set_replication_keys, [dc2_test_with_intercepts]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, set_replication_keys, [dc3_test_with_intercepts]),

    replication_intercept_test1(Cluster1, Cluster2, Cluster3),
    pass.


replication_intercept_test1(Cluster1, Cluster2, Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Node3 = hd(Cluster3),
    AKey = key5,
    
    WriteResult1 = rpc:call(Node1,
			    antidote, append,
			    [AKey, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
			    antidote, append,
			    [AKey, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
			    antidote, append,
			    [Akey, riak_dt_gcounter, {increment, ucl}]),
		      ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,

    %%Sleep a bit to let propagation happen
    timer:sleep(5000),

    ReadResult = rpc:call(Node1, antidote, read,
			  [Akey, riak_dt_gcounter]),
    %% Should get 0 beacause reading from an external DC
    %% that has not recived any updates
    ?assertEqual(0, ReadReasult),

    ReadResult2 = rpc:call(Node1,
			   antidote, clocksi_read,
			   [CommitTime, Akey, riak_dt_gcounter]),
    %%This is an error because the local DC doesn't replicate the key
    %% and the external DCs have not recieved the updates needed
    %% so should timeout
    ?assertEqual({error, _}, ReadResult2),
    lager:info("Done read in Node1"),

    ReadResult3 = rpc:call(Node3,
			   antidote, clocksi_read,
			   [CommitTime, Akey, riak_dt_gcounter]),
    %%    {ok, {_,[ReadSet1],_} }= ReadResult3,
    %%    ?assertEqual(0, ReadSet1),
    ?assertEqual({error, _}, ReadResult3),
    lager:info("Done Read in Node2"),
   
    ReadResult4 = rpc:call(Node2,
			   antidote, clocksi_read,
			   [CommitTime, Akey, riak_dt_gcounter]),
    %%   {ok, {_,[ReadSet2],_} }= ReadResult4,
    %%   ?assertEqual(0, ReadSet2),
    ?assertEqual({error, _}, ReadResult4),
    lager:info("Done Read in Node3").


