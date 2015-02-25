-module(multiple_dcs_partial_rep_test_wintercepts_test2).

-export([confirm/0, multiple_writes/4]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

%% Inercept test 2
%% This test performs updates to key5 through
%% DC1 which does not replicate that key
%% Updates are only allowed to propagate to the 3rd DC
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
    {ok, DC1} = rpc:call(HeadCluster1, inter_dc_manager, start_receiver,[90031]),
    {ok, DC1Read} = rpc:call(HeadCluster1, inter_dc_manager, start_read_receiver,[80001]),
    {ok, DC1SafeSend} = rpc:call(HeadCluster1, inter_dc_manager, start_safe_send_receiver,[90001]),

    {ok, DC2} = rpc:call(HeadCluster2, inter_dc_manager, start_receiver,[90032]),
    {ok, DC2Read} = rpc:call(HeadCluster2, inter_dc_manager, start_read_receiver,[80002]),
    {ok, DC2SafeSend} = rpc:call(HeadCluster2, inter_dc_manager, start_safe_send_receiver,[90002]),

    {ok, DC3} = rpc:call(HeadCluster3, inter_dc_manager, start_receiver,[90033]),
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

    replication_intercept_test2(Cluster1, Cluster2, Cluster3),
    pass.


replication_intercept_test2(Cluster1, Cluster2, Cluster3) ->
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
    %% Should get 0 beacause reading from external DC2
    %% that has not recived any updates
    ?assertEqual(0, ReadReasult),

    ReadResultDC3 = rpc:call(Node3, antidote, read,
			  [Akey, riak_dt_gcounter]),
    %% Should get 3 beacause reading from external DC3
    %% that has recieved the updates
    ?assertEqual(3, ReadReasultDC3),

    %% Should fail at DC2, then move onto DC3, and get 3
    ReadResult2 = rpc:call(Node1,
			   antidote, clocksi_read,
			   [CommitTime, Akey, riak_dt_gcounter]),
    {ok, {_,[ReadSet1],_} }= ReadResult2,
    ?assertEqual(3, ReadSet1),
    lager:info("Done Read in Node1"),

    ReadResult3 = rpc:call(Node3,
			   antidote, clocksi_read,
			   [CommitTime, Akey, riak_dt_gcounter]),
    {ok, {_,[ReadSet2],_} }= ReadResult3,
    ?assertEqual(3, ReadSet2),
    lager:info("Done Read in Node2"),

    ReadResult4 = rpc:call(Node2,
			   antidote, clocksi_read,
			   [CommitTime, Akey, riak_dt_gcounter]),
    {ok, {_,[ReadSet3],_} }= ReadResult4,
    ?assertEqual(0, ReadSet3),
    lager:info("Done Read in Node3").


    

simple_replication_test(Cluster1, Cluster2, Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Node3 = hd(Cluster3),
    KeysList = [key5, key15, key25],
    

    list:fold(fun(AKey, _Acc) ->
		      WriteResult1 = rpc:call(Node1,
					      antidote, append,
					      [KeyList, riak_dt_gcounter, {increment, ucl}]),
		      ?assertMatch({ok, _}, WriteResult1),
		      WriteResult2 = rpc:call(Node1,
					      antidote, append,
					      [KeyList, riak_dt_gcounter, {increment, ucl}]),
		      ?assertMatch({ok, _}, WriteResult2),
		      WriteResult3 = rpc:call(Node1,
					      antidote, append,
					      [Akey, riak_dt_gcounter, {increment, ucl}]),
		      ?assertMatch({ok, _}, WriteResult3),
		      {ok,{_,_,CommitTime}}=WriteResult3,
		      ReadResult = rpc:call(Node1, antidote, read,
					    [Akey, riak_dt_gcounter]),
		      ?assertEqual({ok, 3}, ReadResult),
		      
		      lager:info("Done append in Node1"),
		      ReadResult2 = rpc:call(Node3,
					     antidote, clocksi_read,
					     [CommitTime, Akey, riak_dt_gcounter]),
		      {ok, {_,[ReadSet1],_} }= ReadResult2,
		      ?assertEqual(3, ReadSet1),
		      lager:info("Done Read in Node3"),
		      ReadResult3 = rpc:call(Node2,
					     antidote, clocksi_read,
					     [CommitTime, Akey, riak_dt_gcounter]),
		      {ok, {_,[ReadSet2],_} }= ReadResult3,
		      ?assertEqual(3, ReadSet2),
		      
		      lager:info("Done first round of read, I am gonna append"),
		      WriteResult4= rpc:call(Node2,
                           antidote, clocksi_bulk_update,
					     [ CommitTime,
					       [{update, Akey, riak_dt_gcounter, {increment, ucl}}]]),
		      ?assertMatch({ok, _}, WriteResult4),
		      {ok,{_,_,CommitTime2}}=WriteResult4,
		      lager:info("Done append in Node2"),
		      WriteResult5= rpc:call(Node3,
					     antidote, clocksi_bulk_update,
					     [CommitTime2,
					      [{update, Akey, riak_dt_gcounter, {increment, ucl}}]]),
		      ?assertMatch({ok, _}, WriteResult5),
		      {ok,{_,_,CommitTime3}}=WriteResult5,
		      lager:info("Done append in Node3"),
		      lager:info("Done waiting, I am gonna read"),
		      
		      SnapshotTime =
			  CommitTime3,
		      ReadResult4 = rpc:call(Node1,
					     antidote, clocksi_read,
					     [SnapshotTime, Akey, riak_dt_gcounter]),
		      {ok, {_,[ReadSet4],_} }= ReadResult4,
		      ?assertEqual(5, ReadSet4),
		      lager:info("Done read in Node1"),
		      ReadResult5 = rpc:call(Node2,
					     antidote, clocksi_read,
					     [SnapshotTime,Akey, riak_dt_gcounter]),
		      {ok, {_,[ReadSet5],_} }= ReadResult5,
		      ?assertEqual(5, ReadSet5),
		      lager:info("Done read in Node2"),
		      ReadResult6 = rpc:call(Node3,
					     antidote, clocksi_read,
					     [SnapshotTime,Akey, riak_dt_gcounter]),
		      {ok, {_,[ReadSet6],_} }= ReadResult6,
		      ?assertEqual(5, ReadSet6)
	      end,
	      [], KeyList),
    pass.


parallel_writes_test(Cluster1, Cluster2, Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Node3 = hd(Cluster3),
    Pid = self(),
    %% WriteFun = fun(A,B,C,D) ->
    %%                    multiple_writes(A,B,C,D)
    %%            end,
    KeyList = [key6, key16, key26],
    

    list:fold(fun(AKey, _Acc) ->
		      spawn(?MODULE, multiple_writes,[Node1,AKey, node1, Pid]),
		      spawn(?MODULE, multiple_writes,[Node2,AKey, node2, Pid]),
		      spawn(?MODULE, multiple_writes,[Node3,AKey, node3, Pid]),
		      Result = receive
				   {ok, CT1} ->
				       receive
					   {ok, CT2} ->
					       receive
						   {ok, CT3} ->
						       Time = dict:merge(fun(_K, T1,T2) ->
										 max(T1,T2)
									 end,
									 CT3, dict:merge(
										fun(_K, T1,T2) ->
											max(T1,T2)
										end,
										CT1, CT2)),
						       ReadResult1 = rpc:call(Node1,
									      antidote, clocksi_read,
									      [Time, AKey, riak_dt_gcounter]),
						       {ok, {_,[ReadSet1],_} }= ReadResult1,
						       ?assertEqual(15, ReadSet1),
						       ReadResult2 = rpc:call(Node2,
									      antidote, clocksi_read,
									      [Time, AKey, riak_dt_gcounter]),
						       {ok, {_,[ReadSet2],_} }= ReadResult2,
						       ?assertEqual(15, ReadSet2),
						       ReadResult3 = rpc:call(Node3,
									      antidote, clocksi_read,
									      [Time, AKey, riak_dt_gcounter]),
						       {ok, {_,[ReadSet3],_} }= ReadResult3,
						       ?assertEqual(15, ReadSet3),
						       lager:info("Parallel reads passed"),
						       pass
					       end
				       end
			       end,
		      ?assertEqual(Result, pass),
	      end,
	      [], KeyList),
    pass.


multiple_writes(Node, Key, Actor, ReplyTo) ->
    WriteResult1 = rpc:call(Node,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, Actor}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, Actor}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, Actor}]),
    ?assertMatch({ok, _}, WriteResult3),
    WriteResult4 = rpc:call(Node,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, Actor}]),
    ?assertMatch({ok, _}, WriteResult4),
    WriteResult5 = rpc:call(Node,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, Actor}]),
    ?assertMatch({ok, _}, WriteResult5),
    {ok,{_,_,CommitTime}}=WriteResult5,
    ReplyTo ! {ok, CommitTime}.
