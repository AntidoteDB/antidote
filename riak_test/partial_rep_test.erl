-module(partial_rep_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),

    Clean = rt_config:get(clean_cluster, true),

    [Cluster1, Cluster2] = rt:build_clusters([1,1]),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),

    %% Enable partial replication
    ok = rpc:call(hd(Cluster1), partial_repli_utils, set_partial_rep, [true]),
    ok = rpc:call(hd(Cluster2), partial_repli_utils, set_partial_rep, [true]),

    {ok, Prot} = rpc:call(hd(Cluster1), application, get_env, [antidote, txn_prot]),
    ?assertMatch(clocksi, Prot),

    ok = common:setup_dc_manager([Cluster1, Cluster2], first_run),
    simple_external_read_test(Cluster1, Cluster2),

    [Cluster3, Cluster4] = common:clean_and_rebuild_clusters([Cluster1, Cluster2]),
    ok = common:setup_dc_manager([Cluster3, Cluster4], Clean),
    parallel_external_read_test(Cluster3, Cluster4),

    [Cluster5, Cluster6] = common:clean_and_rebuild_clusters([Cluster3, Cluster4]),
    ok = common:setup_dc_manager([Cluster5, Cluster6], Clean),
    to_log_external_read_test(Cluster5, Cluster6),

    pass.

%% This will perform a read and update to a key that is not replicated
%% at the DC that the client is connected to
%% The update is performed first so that when the read is performed
%% the update is included as part of the read request (the update
%% is included so that the read is not blocked waiting for the
%% snapshot at the external DC to catch up to the snapshot used
%% for the read [the snapshot will be behind at the external DC
%% because the sabilisation mechanism takes some time])
simple_external_read_test(Cluster1, Cluster2) ->
    Node1 = hd(Cluster1),
    _Node2 = hd(Cluster2),
    Key = <<"notexternal">>,
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, ucl1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, ucl2}]),
    ?assertMatch({ok, _}, WriteResult2),
    Result = rpc:call(Node1, antidote, read,
                      [Key, riak_dt_gcounter]),
    ?assertEqual({ok, 2}, Result),

    lager:info("Simple external read test passed!"),
    pass.

%% This is the same as simple_external_read_test
%% except is uses the transactional parrallel
%% read path
parallel_external_read_test(Cluster1, Cluster2) ->
    Node1 = hd(Cluster1),
    _Node2 = hd(Cluster2),
    Key1 = <<"parallel_read1">>,
    Bucket = <<"bucket">>,
    Key2 = <<"parallel_read2">>,
    BObj1 = {Key1, riak_dt_gcounter, Bucket},
    BObj2 = {Key2, riak_dt_gcounter, Bucket},
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [{Key1,Bucket}, riak_dt_gcounter, {increment, ucl1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [{Key2,Bucket}, riak_dt_gcounter, {increment, ucl2}]),
    ?assertMatch({ok, _}, WriteResult2),
    {ok,{_,_,CommitTime}}=WriteResult2,
    Result = rpc:call(Node1, antidote, read_objects,
                      [CommitTime, [], [BObj1, BObj2]]),
    lager:info("the read obj results ~p", [Result]),
    ?assertMatch({ok,[1,1],_},Result),

    lager:info("Parallel external read test passed"),    
    pass.

%% This will perform a lot of reads and updates to a key
%% that is not replicated at the DC the client is connected
%% to.  At the end a read will be performed with an old
%% snapshot, so the operation will have to check the log
%% (instead of the materializer cache like in the previous tests)
%% for any updates that happened locally so that the read
%% will not be blocked at the external DC
to_log_external_read_test(Cluster1,_Cluster2) ->
    FirstNode = hd(Cluster1),
    Type = crdt_pncounter,
    Key = <<"log_read">>,
    increment_counter(FirstNode, Key, 10),
    {ok, TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    increment_counter(FirstNode, Key, 100),
    %% old read value is 0
    {ok, ReadResult1} = rpc:call(FirstNode,
        antidote, clocksi_iread, [TxId, Key, Type]),
    ?assertEqual(10, ReadResult1),
    %% most recent read value is 15
    {ok, {_, [ReadResult2], _}} = rpc:call(FirstNode,
        antidote, clocksi_read, [Key, Type]),
    ?assertEqual(110, ReadResult2),
    lager:info("External read to log test passed"),
    pass.

%% Auxiliary method to increment a counter N times.
increment_counter(_FirstNode, _Key, 0) ->
    ok;
increment_counter(FirstNode, Key, N) ->
    WriteResult = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, crdt_pncounter, {increment, a}}}]]),
    ?assertMatch({ok, _}, WriteResult),
    increment_counter(FirstNode, Key, N - 1).


