-module(multiple_dcs_partial_rep_test_wintercepts_test3).

-export([confirm/0]).

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




    {ok, DC1up,DC1read} = rpc:call(HeadCluster1, antidote_sup, start_recvrs,[local,9051,8001]),
    {ok, DC2up,DC2read} = rpc:call(HeadCluster2, antidote_sup, start_recvrs,[local,9052,8002]),
    {ok, DC3up,DC3read} = rpc:call(HeadCluster3, antidote_sup, start_recvrs,[local,9053,8003]),
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
    DC1Key = key6,
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
			  [DC1Key, riak_dt_gcounter]),
    ?assertEqual(1, ReadResult),
    ReadResult2 = rpc:call(Node1, antidote, read,
			  [DC2Key, riak_dt_gcounter]),
    ?assertEqual(1, ReadResult2),
    ReadResult3 = rpc:call(Node1, antidote, read,
			  [DC3Key, riak_dt_gcounter]),
    ?assertEqual(1, ReadResult3),


    %% Ensure you can do transactions on your local keys
    %% But reads at external DCs should fail becuase
    %% they haven't recived the updates upto your timestamp
    ReadResult4 = rpc:call(Node1,
			   antidote, clocksi_read,
			   [CommitTime, DC1Key, riak_dt_gcounter]),
    {ok, {_,[ReadSet1],_} }= ReadResult4,
    ?assertEqual(1, ReadSet1),
    lager:info("Done Read in Node1"),

    ReadResult5 = rpc:call(Node1,
			   antidote, clocksi_read,
			   [CommitTime, DC2Key, riak_dt_gcounter]),
    ?assertMatch({error, _}, ReadResult5),
    lager:info("Done Read in Node1").
    


get_key_list(DC1read,DC1up,DC2read,DC2up,DC3read,DC3up) ->
    Key1 = [{key1,[DC1read],[DC1up]}],
    Key2 = [{key2,[DC1read],[DC1up]}],
    Key3 = [{key3,[DC1read],[DC1up]}],
    Key4 = [{key4,[DC1read],[DC1up]}],
    Key5 = [{key5,[DC1read],[DC1up]}],
    Key6 = [{key6,[DC1read],[DC1up]}],
    Key7 = [{key7,[DC1read],[DC1up]}],
    Key8 = [{key8,[DC1read],[DC1up]}],
    Key9 = [{key9,[DC1read],[DC1up]}],
    Key10 = [{key10,[DC1read,DC2read],[DC1up,DC2up]}],
    Key11 = [{key11,[DC1read,DC2read],[DC1up,DC2up]}],
    Key12 = [{key12,[DC1read,DC2read],[DC1up,DC2up]}],
    Key13 = [{key13,[DC1read,DC2read],[DC1up,DC2up]}],
    Key14 = [{key14,[DC1read,DC2read],[DC1up,DC2up]}],
    Key15 = [{key15,[DC1read,DC2read],[DC1up,DC2up]}],
    Key16 = [{key16,[DC2read],[DC2up]}],
    Key17 = [{key17,[DC2read],[DC2up]}],
    Key18 = [{key18,[DC2read],[DC2up]}],
    Key19 = [{key19,[DC2read],[DC2up]}],
    Key20 = [{key20,[DC2read,DC3read],[DC2up,DC3up]}],
    Key21 = [{key21,[DC2read,DC3read],[DC2up,DC3up]}],
    Key22 = [{key22,[DC2read,DC3read],[DC2up,DC3up]}],
    Key23 = [{key23,[DC2read,DC3read],[DC2up,DC3up]}],
    Key24 = [{key24,[DC2read,DC3read],[DC2up,DC3up]}],
    Key25 = [{key25,[DC2read,DC3read],[DC2up,DC3up]}],
    Key26 = [{key26,[DC3read],[DC3up]}],
    Key27 = [{key27,[DC3read],[DC3up]}],
    Key28 = [{key28,[DC3read],[DC3up]}],
    Key29 = [{key29,[DC3read],[DC3up]}],
    Key30 = [{key30,[DC3read],[DC3up]}],
    Key1 ++ Key2 ++ Key3 ++ Key4 ++ Key5 ++ Key6 ++ Key7 ++ Key8  ++ Key9
	++ Key10 ++ Key11 ++ Key12 ++ Key13 ++ Key14 ++ Key15 ++ Key16 ++ Key17
	++ Key18 ++ Key19 ++ Key20 ++ Key21 ++ Key22 ++ Key23 ++ Key24 ++ Key25
	++ Key26 ++ Key27 ++ Key28 ++ Key29 ++ Key30.
