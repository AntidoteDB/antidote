-module(multiple_dcs_test).

-export([confirm/0, multiple_writes/4]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).


confirm() ->
    % Must be a power of 2, minimum 8 and maximum 1024.
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, 8}]}
    ]),
    [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1,1,1]),
    HeadCluster1 = hd(Cluster1),
    HeadCluster2 = hd(Cluster2),
    HeadCluster3 = hd(Cluster3),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),

    rt:wait_until_registered(HeadCluster1, inter_dc_pub),
    rt:wait_until_registered(HeadCluster2, inter_dc_pub),
    rt:wait_until_registered(HeadCluster3, inter_dc_pub),

    rt:wait_until_registered(HeadCluster1, inter_dc_log_reader_response),
    rt:wait_until_registered(HeadCluster2, inter_dc_log_reader_response),
    rt:wait_until_registered(HeadCluster3, inter_dc_log_reader_response),

    rt:wait_until_registered(HeadCluster1, inter_dc_log_reader_query),
    rt:wait_until_registered(HeadCluster2, inter_dc_log_reader_query),
    rt:wait_until_registered(HeadCluster3, inter_dc_log_reader_query),

    rt:wait_until_registered(HeadCluster1, inter_dc_sub),
    rt:wait_until_registered(HeadCluster2, inter_dc_sub),
    rt:wait_until_registered(HeadCluster3, inter_dc_sub),

    {ok, DC1} = rpc:call(HeadCluster1, inter_dc_manager, get_descriptor, []),
    {ok, DC2} = rpc:call(HeadCluster2, inter_dc_manager, get_descriptor, []),
    {ok, DC3} = rpc:call(HeadCluster3, inter_dc_manager, get_descriptor, []),

    lager:info("DCs: ~p, ~p and ~p", [DC1, DC2, DC3]),
    lager:info("Connecting DCs..."),
    ok = rpc:call(HeadCluster1, inter_dc_manager, observe_dcs_sync, [[DC2, DC3]]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, observe_dcs_sync, [[DC1, DC3]]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, observe_dcs_sync, [[DC1, DC2]]),
    lager:info("DCs connected!"),

    simple_replication_test(Cluster1, Cluster2, Cluster3),
    parallel_writes_test(Cluster1, Cluster2, Cluster3),
    failure_test(Cluster1, Cluster2, Cluster3),
    pass.

simple_replication_test(Cluster1, Cluster2, Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Node3 = hd(Cluster3),
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl2}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl3}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,
    ReadResult = rpc:call(Node1, antidote, read,
                          [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 3}, ReadResult),

    lager:info("Done append in Node1"),
    ReadResult2 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [CommitTime, key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet1],_} }= ReadResult2,
    ?assertEqual(3, ReadSet1),
    lager:info("Done Read in Node3"),
    ReadResult3 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [CommitTime, key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet2],_} }= ReadResult3,
    ?assertEqual(3, ReadSet2),

    lager:info("Done first round of read, I am gonna append"),
    WriteResult4= rpc:call(Node2,
                           antidote, clocksi_bulk_update,
                           [ CommitTime,
                             [{update, {key1, riak_dt_gcounter, {increment, ucl4}}}]]),
    ?assertMatch({ok, _}, WriteResult4),
    {ok,{_,_,CommitTime2}}=WriteResult4,
    lager:info("Done append in Node2"),
    WriteResult5= rpc:call(Node3,
                           antidote, clocksi_bulk_update,
                           [CommitTime2,
                            [{update, {key1, riak_dt_gcounter, {increment, ucl5}}}]]),
    ?assertMatch({ok, _}, WriteResult5),
    {ok,{_,_,CommitTime3}}=WriteResult5,
    lager:info("Done append in Node3"),
    lager:info("Done waiting, I am gonna read"),

    SnapshotTime =
        CommitTime3,
    ReadResult4 = rpc:call(Node1,
                           antidote, clocksi_read,
                           [SnapshotTime, key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet4],_} }= ReadResult4,
    ?assertEqual(5, ReadSet4),
    lager:info("Done read in Node1"),
    ReadResult5 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [SnapshotTime,key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet5],_} }= ReadResult5,
    ?assertEqual(5, ReadSet5),
    lager:info("Done read in Node2"),
    ReadResult6 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [SnapshotTime,key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet6],_} }= ReadResult6,
    ?assertEqual(5, ReadSet6),
    pass.

parallel_writes_test(Cluster1, Cluster2, Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Node3 = hd(Cluster3),
    Key = parkey,
    Pid = self(),
    %% WriteFun = fun(A,B,C,D) ->
    %%                    multiple_writes(A,B,C,D)
    %%            end,
    spawn(?MODULE, multiple_writes,[Node1,Key, node1, Pid]),
    spawn(?MODULE, multiple_writes,[Node2,Key, node2, Pid]),
    spawn(?MODULE, multiple_writes,[Node3,Key, node3, Pid]),
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
                           [Time, Key, riak_dt_gcounter]),
                        {ok, {_,[ReadSet1],_} }= ReadResult1,
                        ?assertEqual(15, ReadSet1),
                        ReadResult2 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [Time, Key, riak_dt_gcounter]),
                        {ok, {_,[ReadSet2],_} }= ReadResult2,
                        ?assertEqual(15, ReadSet2),
                        ReadResult3 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [Time, Key, riak_dt_gcounter]),
                        {ok, {_,[ReadSet3],_} }= ReadResult3,
                        ?assertEqual(15, ReadSet3),
                        lager:info("Parallel reads passed"),
                        pass
                    end
            end
    end,
    ?assertEqual(Result, pass),
    pass.

multiple_writes(Node, Key, Actor, ReplyTo) ->
    WriteResult1 = rpc:call(Node,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, list_to_atom(atom_to_list(Actor) ++ atom_to_list(ucl6))}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, list_to_atom(atom_to_list(Actor) ++ atom_to_list(ucl7))}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, list_to_atom(atom_to_list(Actor) ++ atom_to_list(ucl8))}]),
    ?assertMatch({ok, _}, WriteResult3),
    WriteResult4 = rpc:call(Node,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, list_to_atom(atom_to_list(Actor) ++ atom_to_list(ucl9))}]),
    ?assertMatch({ok, _}, WriteResult4),
    WriteResult5 = rpc:call(Node,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, list_to_atom(atom_to_list(Actor) ++ atom_to_list(ucl10))}]),
    ?assertMatch({ok, _}, WriteResult5),
    {ok,{_,_,CommitTime}}=WriteResult5,
    ReplyTo ! {ok, CommitTime}.

%% Test: when a DC is disconnected for a while and connected back it should
%%  be able to read the missing updates. This should not affect the causal 
%%  dependency protocol
failure_test(Cluster1, Cluster2, Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Node3 = hd(Cluster3),
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [ftkey1, riak_dt_gcounter, {increment, ucl11}]),
    ?assertMatch({ok, _}, WriteResult1),

    %% Simulate failure of NODE3 by stoping the receiver
    {ok, D1} = rpc:call(Node1, inter_dc_manager, get_descriptor, []),
    {ok, D2} = rpc:call(Node2, inter_dc_manager, get_descriptor, []),

    ok = rpc:call(Node3, inter_dc_manager, forget_dcs, [[D1, D2]]),

    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [ftkey1, riak_dt_gcounter, {increment, ucl12}]),
    ?assertMatch({ok, _}, WriteResult2),
    %% Induce some delay
    rpc:call(Node3, antidote, read,
             [ftkey1, riak_dt_gcounter]),

    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [ftkey1, riak_dt_gcounter, {increment, ucl13}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,
    ReadResult = rpc:call(Node1, antidote, read,
                          [ftkey1, riak_dt_gcounter]),
    ?assertEqual({ok, 3}, ReadResult),
    lager:info("Done append in Node1"),

    %% NODE3 comes back
    ok = rpc:call(Node3, inter_dc_manager, observe_dcs, [[D1, D2]]),

    ReadResult3 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [CommitTime, ftkey1, riak_dt_gcounter]),
    {ok, {_,[ReadSet2],_} }= ReadResult3,
    ?assertEqual(3, ReadSet2),
    lager:info("Done read from Node2"),
    ReadResult2 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [CommitTime, ftkey1, riak_dt_gcounter]),
    {ok, {_,[ReadSet1],_} }= ReadResult2,
    ?assertEqual(3, ReadSet1),
    lager:info("Done Read in Node3"),
    pass.
   
