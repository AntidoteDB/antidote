-module(clock_si).
-export([confirm/0, clockSI_test1/1, clockSI_test2/1, clockSI_test3/1,
		clockSI_test_read_wait/1, clockSI_test4/1, clockSI_test_read_time/1, spawn_read/3]).
-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    clockSI_test1(Nodes),
    clockSI_test2 (Nodes),
    clockSI_test3 (Nodes),
    clockSI_test4 (Nodes),
    clockSI_test_read_time(Nodes),
    clockSI_test_read_wait(Nodes),
    rt:clean_cluster(Nodes),
    ok.
    
%% @doc The following function tests that ClockSI can run a non-interactive tx
%% that updates multiple partitions.
clockSI_test1(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test1 started"), 
    Result=rpc:call(FirstNode, floppy, clockSI_execute_TX, [now(), 
    [{update, 11, {increment, a}},{update, 11, {increment, a}}, {read, 11, riak_dt_pncounter}, {update, 12, {increment, a}}, {read, 12, riak_dt_pncounter}]]), 
    {ok, {_, ReadSet, _}}=Result,
    ?assertMatch([2,1], ReadSet),
    lager:info("Test1 passed"),
    ok.



%% @doc The following function tests that ClockSI can run an interactive tx.
%% that updates multiple partitions.
clockSI_test2(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test2 started"), 
    {ok,TxId}=rpc:call(FirstNode, floppy, clockSI_istart_tx, [now()]),
    ReadResult0=rpc:call(FirstNode, floppy, clockSI_iread, [TxId, abc, riak_dt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, floppy, clockSI_iupdate, [TxId, abc, {increment, 4}]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, floppy, clockSI_iread, [TxId, abc, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult),
    WriteResult1=rpc:call(FirstNode, floppy, clockSI_iupdate, [TxId, bcd, {increment, 4}]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, floppy, clockSI_iread, [TxId, bcd, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult1),
    WriteResult2=rpc:call(FirstNode, floppy, clockSI_iupdate, [TxId, cde, {increment, 4}]),
    ?assertEqual(ok, WriteResult2),
    ReadResult2=rpc:call(FirstNode, floppy, clockSI_iread, [TxId, cde, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult2),
    CommitTime=rpc:call(FirstNode, floppy, clockSI_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime), 
    End=rpc:call(FirstNode, floppy, clockSI_icommit, [TxId]), 
    ?assertMatch({ok, _}, End),
    lager:info("Test2 passed"), 
    ok.
    


%% The following function tests that ClockSI can run both a single read and a bulk-update tx.
clockSI_test3(Nodes) ->
	lager:info("Test3 started"),
	FirstNode = hd(Nodes),
    Result= rpc:call(FirstNode, floppy, clockSI_bulk_update, [now(), [{update, 3, 
    {increment, a}}, {update, 3, {increment, b}}]]),
    ?assertMatch({ok, _}, Result),
    Result2= rpc:call(FirstNode, floppy, clockSI_read, [now(), 3, riak_dt_pncounter]),
    {ok, {_, ReadSet, _}}=Result2,
    ?assertMatch([2], ReadSet),
    lager:info("Test3 passed"),
    ok.

%% The following function tests that ClockSI can excute a read-only interactive tx.
clockSI_test4(Nodes) ->
    lager:info("Test4 started"),
    FirstNode = hd(Nodes),
    lager:info("Node1: ~p", [FirstNode]), 
    {ok,TxId1}=rpc:call(FirstNode, floppy, clockSI_istart_tx, [now()]),
    %?assertMatch({ok, _}, TxId1), 
    lager:info("Tx Started, id : ~p", [TxId1]),
    ReadResult1=rpc:call(FirstNode, floppy, clockSI_iread, [TxId1, abc, riak_dt_pncounter]),
    lager:info("Tx Reading..."),
    ?assertMatch({ok, _}, ReadResult1),
    lager:info("Tx Read value...~p", [ReadResult1]),
    CommitTime1=rpc:call(FirstNode, floppy, clockSI_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1), 
    lager:info("Tx sent prepare, got commitTime=..., id : ~p", [CommitTime1]), 
    End1=rpc:call(FirstNode, floppy, clockSI_icommit, [TxId1]),   
    ?assertMatch({ok, _}, End1),
    lager:info("Tx Committed."),
    lager:info("Test 4 passed."), 
    ok.
    
%% The following function tests that ClockSI waits, when reading, for a tx that 
%% has updated an element that it wants to read and has a smaller TxId, but has not yet committed.
clockSI_test_read_time(Nodes) ->
    %% Start a new tx,  perform an update over key abc, and send prepare.
    lager:info("Test read_time started"),
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]), 
    lager:info("LastNode: ~p", [LastNode]),  
    {ok,TxId}=rpc:call(FirstNode, floppy, clockSI_istart_tx, [now()]),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    %% start a different tx and try to read key read_time. 
    {ok,TxId1}=rpc:call(LastNode, floppy, clockSI_istart_tx, [now()]),
    %?assertMatch({ok, _}, TxId), 
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult=rpc:call(FirstNode, floppy, clockSI_iupdate, [TxId, read_time, {increment, 4}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),
    CommitTime=rpc:call(FirstNode, floppy, clockSI_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    lager:info("Tx1 sent prepare, got commitTime=..., id : ~p", [CommitTime]), 
    %% try to read key read_time. 
    %?assertMatch({ok, _}, TxId1),  
    lager:info("Tx2 Reading..."),
    ReadResult1=rpc:call(LastNode, floppy, clockSI_iread, [TxId1, read_time, riak_dt_pncounter]),
    lager:info("Tx2 Reading..."),
    ?assertMatch({ok, 0}, ReadResult1),
    lager:info("Tx2 Read value...~p", [ReadResult1]),
    
    %% commit the first tx.
    End=rpc:call(FirstNode, floppy, clockSI_icommit, [TxId]),   
    ?assertMatch({ok, _}, End),
    lager:info("Tx1 Committed."),
    
    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, floppy, clockSI_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1), 
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]), 
    End1=rpc:call(LastNode, floppy, clockSI_icommit, [TxId1]),   
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),
    lager:info("Test read_time passed"),
    ok.
    
%% The following function tests that ClockSI does not read values inserted by a tx with higher
%% commit timestamp than the snapshot time of the reading tx. 
clockSI_test_read_wait(Nodes) ->
	lager:info("Test read_wait started"),
    %% Start a new tx,  perform an update over key read_wait_test, and send prepare.
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]), 
    lager:info("LastNode: ~p", [LastNode]),  
    {ok,TxId}=rpc:call(FirstNode, floppy, clockSI_istart_tx, [now()]),
    %?assertMatch({ok, _}, TxId), 
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, floppy, clockSI_iupdate, [TxId, read_wait_test, {increment, 4}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),
    CommitTime=rpc:call(FirstNode, floppy, clockSI_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    lager:info("Tx1 sent prepare, got commitTime=..., id : ~p", [CommitTime]), 
    
    timer:sleep(1000),
    
    %% start a different tx and try to read key read_wait_test. 
    {ok,TxId1}=rpc:call(LastNode, floppy, clockSI_istart_tx, [now()]),
    %?assertMatch({ok, _}, TxId1), 
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    lager:info("Tx2 Reading..."),
    
    Pid=spawn(clock_si, spawn_read, [LastNode, TxId1, self()]),    
    %timer:sleep(1000),
    
    %% commit the first tx.
    End=rpc:call(FirstNode, floppy, clockSI_icommit, [TxId]),   
    ?assertMatch({ok, _}, End),
    lager:info("Tx1 Committed."),
    

    receive
        {Pid, ReadResult1} ->
            %receive the read value
            ?assertMatch({ok, 1}, ReadResult1),
            lager:info("Tx2 Read value...~p", [ReadResult1])
    end,
    
    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, floppy, clockSI_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1), 
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]), 
    End1=rpc:call(LastNode, floppy, clockSI_icommit, [TxId1]),   
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),
    lager:info("Test read_wait passed"),
    ok.

spawn_read(LastNode, TxId, Return) ->
    ReadResult=rpc:call(LastNode, floppy, clockSI_iread, [TxId, read_wait_test, riak_dt_pncounter]),
    Return ! {self(), ReadResult}.
