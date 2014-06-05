-module(floppystore).
-export([confirm/0, simple_test/1]).
-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    clockSI_test1(Nodes),
    clockSI_test2 (Nodes),
    clockSI_test3 (Nodes),
    clockSI_test_read_wait(Nodes),
    rt:clean_cluster(Nodes),
    ok.
    
simple_test(Nodes) ->
   % [Nodes] = rt:build_clusters([2]),
 
    FirstNode = hd(Nodes),
 
    Values = [1, 2, 3, 4, 5],
 
    FinalValue = lists:foldl(fun(Value, Acc) ->
        ?assertMatch({ok, _},
            rpc:call(FirstNode,
                floppy, append, [abc, {{increment, Value}, 4}])),
        Acc + Value end, 0, Values),
 
    ReadResult = rpc:call(FirstNode, floppy, read, [abc, riak_dt_gcounter]),
    ?assertEqual(FinalValue, ReadResult),
    ok.

%% The following function tests that ClockSI can run a non-interactive tx.
clockSI_test1(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Node1: ~p", [FirstNode]), 
    Result=rpc:call(FirstNode, floppy, clockSI_execute_TX, [now(), 
    [{update, 1, {increment, a}}, {read, 1, riak_dt_pncounter}, {update, 1, {decrement, a}}]]), 
    ?assertMatch({ok, _}, Result),
    ok.
    
%% The following function tests that ClockSI can run an interactive tx.
%% that updates multiple partitions.
clockSI_test2(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Node1: ~p", [FirstNode]), 
    {ok,TxId}=rpc:call(FirstNode, floppy, clockSI_istart_tx, [clockSI_vnode:now_milisec(now())]),
    ReadResult=rpc:call(FirstNode, floppy, clockSI_iread, [TxId, abc, riak_dt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult),
    WriteResult=rpc:call(FirstNode, floppy, clockSI_iupdate, [TxId, abc, {increment, 4}]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, floppy, clockSI_iread, [TxId, abc, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult),
    WriteResult1=rpc:call(FirstNode, floppy, clockSI_iupdate, [TxId, bcd, {increment, 4}]),
    ?assertEqual(ok, WriteResult1),
    WriteResult2=rpc:call(FirstNode, floppy, clockSI_iupdate, [TxId, cde, {increment, 4}]),
    ?assertEqual(ok, WriteResult2),
    CommitTime=rpc:call(FirstNode, floppy, clockSI_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime), 
    End=rpc:call(FirstNode, floppy, clockSI_icommit, [TxId]), 
    ?assertMatch({ok, _}, End),
    ok.
    
%% The following function tests that ClockSI waits, when reading, for a tx that 
%% has updated an element that it wants to read and has a smaller TxId, but has not yet committed.
clockSI_test_read_wait(Nodes) ->
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]), 
    lager:info("LastNode: ~p", [LastNode]),  
    {ok,TxId}=rpc:call(FirstNode, floppy, clockSI_istart_tx, [clockSI_vnode:now_milisec(now())]),
    ?assertMatch({ok, _}, TxId), 
    lager:info("Tx1 Started, id : ~p", [TxId]),
    {ok,TxId1}=rpc:call(LastNode, floppy, clockSI_istart_tx, [clockSI_vnode:now_milisec(now())]),
    ?assertMatch({ok, _}, TxId1), 
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult=rpc:call(FirstNode, floppy, clockSI_iupdate, [TxId, abc, {increment, 4}]),
    lager:info("Tx1 Writing..., id : ~n"),
    ?assertEqual(ok, WriteResult),
    CommitTime=rpc:call(FirstNode, floppy, clockSI_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    lager:info("Tx1 sent prepare, got commitTime=..., id : ~p", [CommitTime]), 
    ReadResult1=rpc:call(LastNode, floppy, clockSI_iread, [TxId1, abc, riak_dt_pncounter]),
    lager:info("Tx2 Reading...~n"),
    ?assertMatch({ok, _}, ReadResult1),
    lager:info("Tx2 Read value...~p", [ReadResult1]),
    End=rpc:call(FirstNode, floppy, clockSI_icommit, [TxId]),   
    ?assertMatch({ok, _}, End),
    lager:info("Tx1 Committed.~n"),
    CommitTime1=rpc:call(FirstNode, floppy, clockSI_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1), 
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]), 
    End1=rpc:call(FirstNode, floppy, clockSI_icommit, [TxId1]),   
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed.~n"),
    lager:info("Test finished.~n"),
    
    ok.

%% The following function tests that ClockSI can run both a single read and a bulk-update tx.
clockSI_test3(Nodes) ->
    FirstNode = hd(Nodes),
    Result= rpc:call(FirstNode, floppy, clockSI_bulk_update, [now(), [{update, 1, {increment, a}}, {update, 1, {decrement, a}}]]),
    ?assertMatch({ok, _}, Result),
    ReadResult= rpc:call(FirstNode, floppy, clockSI_read, [now(), 1, riak_dt_pncounter]),
    ?assertMatch({ok, _}, ReadResult),
    ok.
