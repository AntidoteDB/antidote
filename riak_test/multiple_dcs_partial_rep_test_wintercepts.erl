-module(multiple_dcs_partial_rep_test_wintercepts).

-export([confirm/0]).

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

    {ok, DC1up,DC1read} = rpc:call(HeadCluster1, antidote_sup, start_recvrs,[local,9021,8001]),
    {ok, DC2up,DC2read} = rpc:call(HeadCluster2, antidote_sup, start_recvrs,[local,9022,8002]),
    {ok, DC3up,DC3read} = rpc:call(HeadCluster3, antidote_sup, start_recvrs,[local,9023,8003]),
    lager:info("Receivers start results ~p, ~p and ~p", [DC1up, DC2up, DC3up]),

    ok = rpc:call(HeadCluster1, inter_dc_manager, add_list_dcs,[[DC2up, DC3up]]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, add_list_dcs,[[DC1up, DC3up]]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, add_list_dcs,[[DC1up, DC2up]]),

    ok = rpc:call(HeadCluster1, inter_dc_manager, add_list_read_dcs,[[DC2read, DC3read]]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, add_list_read_dcs,[[DC1read, DC3read]]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, add_list_read_dcs,[[DC1read, DC2read]]),

    ok = rpc:call(HeadCluster1, antidote_sup, start_senders, []),
    ok = rpc:call(HeadCluster2, antidote_sup, start_senders, []),
    ok = rpc:call(HeadCluster3, antidote_sup, start_senders, []),

    ok = rpc:call(HeadCluster1, inter_dc_manager, set_replication_keys, [get_key_list(DC1read,DC1up,DC2read,DC2up,DC3read,DC3up)]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, set_replication_keys, [get_key_list(DC1read,DC1up,DC2read,DC2up,DC3read,DC3up)]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, set_replication_keys, [get_key_list(DC1read,DC1up,DC2read,DC2up,DC3read,DC3up)]),

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
			    [AKey, riak_dt_gcounter, {increment, ucl}]),
		      ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,

    %%Sleep a bit to let propagation happen
    timer:sleep(5000),

    ReadReasult = rpc:call(Node1, antidote, read,
			  [AKey, riak_dt_gcounter]),
    %% Should get 0 beacause reading from an external DC
    %% that has not recived any updates
    ?assertEqual(0, ReadReasult),

    ReadResult2 = rpc:call(Node1,
			   antidote, clocksi_read,
			   [CommitTime, AKey, riak_dt_gcounter]),
    %%This is an error because the local DC doesn't replicate the key
    %% and the external DCs have not recieved the updates needed
    %% so should timeout
    ?assertMatch({error, _}, ReadResult2),
    lager:info("Done read in Node1"),

    ReadResult3 = rpc:call(Node3,
			   antidote, clocksi_read,
			   [CommitTime, AKey, riak_dt_gcounter]),
    %%    {ok, {_,[ReadSet1],_} }= ReadResult3,
    %%    ?assertEqual(0, ReadSet1),
    ?assertMatch({error, _}, ReadResult3),
    lager:info("Done Read in Node3"),
   
    ReadResult4 = rpc:call(Node2,
			   antidote, clocksi_read,
			   [CommitTime, AKey, riak_dt_gcounter]),
    %%   {ok, {_,[ReadSet2],_} }= ReadResult4,
    %%   ?assertEqual(0, ReadSet2),
    ?assertMatch({error, _}, ReadResult4),
    lager:info("Done Read in Node2").


get_key_list(_DC1read,_DC1up,DC2read,DC2up,DC3read,DC3up) ->
    [{key5,[DC2read,DC3read],[DC2up,DC3up]}].
