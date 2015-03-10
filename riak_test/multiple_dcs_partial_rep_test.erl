-module(multiple_dcs_partial_rep_test).

-export([confirm/0, multiple_writes/4]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

%% Basic Partial replication tests
%% These tests don't use intercepts
%% Keys 1-10 only exist at DC 1
%% 11-20 at DC 2
%% 21-30 at DC 3
%% Each test writes a key that is replicated at a different
%% DC and checks that the keys and nodes can be read from anywhere

confirm() ->
    [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1,1,1]),
    HeadCluster1 = hd(Cluster1),
    HeadCluster2 = hd(Cluster2),
    HeadCluster3 = hd(Cluster3),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),
    timer:sleep(500), %%TODO: wait for inter_dc_manager to be up



    {ok, DC1up,DC1read} = rpc:call(HeadCluster1, antidote_sup, start_recvrs,[local,7001,8001]),
    {ok, DC2up,DC2read} = rpc:call(HeadCluster2, antidote_sup, start_recvrs,[local,7002,8002]),
    {ok, DC3up,DC3read} = rpc:call(HeadCluster3, antidote_sup, start_recvrs,[local,7003,8003]),
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

    simple_replication_test(Cluster1, Cluster2, Cluster3),
    parallel_writes_test(Cluster1, Cluster2, Cluster3),
    external_read_causality_test(Cluster1, Cluster2, Cluster3),
    pass.


%% This test will check to see that when a key is not replicated at a DC
%% and a read is performed though this DC for that key,
%% the value returned from the external DC is consistent with
%% the snapshot of the local DC
external_read_causality_test(Cluster1, Cluster2, _Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    %%Node3 = hd(Cluster3),
    Key = key15,
    
    WriteResult = rpc:call(Node1,
			   antidote, append,
			   [Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),
    {ok,{_,_,CommitTime}}=WriteResult,
    
    WriteResult2 = rpc:call(Node2,
			    antidote, append,
			    [Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),
    
    %%Let the updates have time to propagate
    
    ReadResult = rpc:call(Node1,
			   antidote, clocksi_read,
			  [CommitTime, Key, riak_dt_gcounter]),
    {ok, {_,[ReadSet1],_} }= ReadResult,
    ?assertEqual(2, ReadSet1),
    lager:info("Done Read in Node1").

    



simple_replication_test(Cluster1, Cluster2, Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Node3 = hd(Cluster3),
    KeyList = [key5, key15, key25],
    

    list:foldl(fun(AKey, _Acc) ->
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
		      ReadResult = rpc:call(Node1, antidote, read,
					    [AKey, riak_dt_gcounter]),
		      ?assertEqual({ok, 3}, ReadResult),
		      
		      lager:info("Done append in Node1"),
		      ReadResult2 = rpc:call(Node3,
					     antidote, clocksi_read,
					     [CommitTime, AKey, riak_dt_gcounter]),
		      {ok, {_,[ReadSet1],_} }= ReadResult2,
		      ?assertEqual(3, ReadSet1),
		      lager:info("Done Read in Node3"),
		      ReadResult3 = rpc:call(Node2,
					     antidote, clocksi_read,
					     [CommitTime, AKey, riak_dt_gcounter]),
		      {ok, {_,[ReadSet2],_} }= ReadResult3,
		      ?assertEqual(3, ReadSet2),
		      
		      lager:info("Done first round of read, I am gonna append"),
		      WriteResult4= rpc:call(Node2,
                           antidote, clocksi_bulk_update,
					     [ CommitTime,
					       [{update, AKey, riak_dt_gcounter, {increment, ucl}}]]),
		      ?assertMatch({ok, _}, WriteResult4),
		      {ok,{_,_,CommitTime2}}=WriteResult4,
		      lager:info("Done append in Node2"),
		      WriteResult5= rpc:call(Node3,
					     antidote, clocksi_bulk_update,
					     [CommitTime2,
					      [{update, AKey, riak_dt_gcounter, {increment, ucl}}]]),
		      ?assertMatch({ok, _}, WriteResult5),
		      {ok,{_,_,CommitTime3}}=WriteResult5,
		      lager:info("Done append in Node3"),
		      lager:info("Done waiting, I am gonna read"),
		      
		      SnapshotTime =
			  CommitTime3,
		      ReadResult4 = rpc:call(Node1,
					     antidote, clocksi_read,
					     [SnapshotTime, AKey, riak_dt_gcounter]),
		      {ok, {_,[ReadSet4],_} }= ReadResult4,
		      ?assertEqual(5, ReadSet4),
		      lager:info("Done read in Node1"),
		      ReadResult5 = rpc:call(Node2,
					     antidote, clocksi_read,
					     [SnapshotTime,AKey, riak_dt_gcounter]),
		      {ok, {_,[ReadSet5],_} }= ReadResult5,
		      ?assertEqual(5, ReadSet5),
		      lager:info("Done read in Node2"),
		      ReadResult6 = rpc:call(Node3,
					     antidote, clocksi_read,
					     [SnapshotTime,AKey, riak_dt_gcounter]),
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
    
    lists:foldl(fun(AKey, _Acc) ->
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
		       ?assertEqual(Result, pass)
		end,
		1, KeyList),
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
    Key10 = [{key10,[DC1read],[DC1up]}],
    Key11 = [{key11,[DC2read],[DC2up]}],
    Key12 = [{key12,[DC2read],[DC2up]}],
    Key13 = [{key13,[DC2read],[DC2up]}],
    Key14 = [{key14,[DC2read],[DC2up]}],
    Key15 = [{key15,[DC2read],[DC2up]}],
    Key16 = [{key16,[DC2read],[DC2up]}],
    Key17 = [{key17,[DC2read],[DC2up]}],
    Key18 = [{key18,[DC2read],[DC2up]}],
    Key19 = [{key19,[DC2read],[DC2up]}],
    Key20 = [{key20,[DC2read],[DC2up]}],
    Key21 = [{key21,[DC3read],[DC3up]}],
    Key22 = [{key22,[DC3read],[DC3up]}],
    Key23 = [{key23,[DC3read],[DC3up]}],
    Key24 = [{key24,[DC3read],[DC3up]}],
    Key25 = [{key25,[DC3read],[DC3up]}],
    Key26 = [{key26,[DC3read],[DC3up]}],
    Key27 = [{key27,[DC3read],[DC3up]}],
    Key28 = [{key28,[DC3read],[DC3up]}],
    Key29 = [{key29,[DC3read],[DC3up]}],
    Key30 = [{key30,[DC3read],[DC3up]}],
    Key1 ++ Key2 ++ Key3 ++ Key4 ++ Key5 ++ Key6 ++ Key7 ++ Key8  ++ Key9
	++ Key10 ++ Key11 ++ Key12 ++ Key13 ++ Key14 ++ Key15 ++ Key16 ++ Key17
	++ Key18 ++ Key19 ++ Key20 ++ Key21 ++ Key22 ++ Key23 ++ Key24 ++ Key25
	++ Key26 ++ Key27 ++ Key28 ++ Key29 ++ Key30.
