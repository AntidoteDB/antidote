%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(clocksi_test).

-export([confirm/0, clocksi_test1/1, clocksi_test2/1,
         clocksi_test_read_wait/1, clocksi_test4/1, clocksi_test_read_time/1,
         clocksi_test_certification_check/1,
         clocksi_multiple_test_certification_check/1, spawn_read/3]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    clocksi_test1(Nodes),
    clocksi_test2 (Nodes),
    clocksi_tx_noclock_test(Nodes),
    clocksi_single_key_update_read_test(Nodes),
    clocksi_multiple_key_update_read_test(Nodes),
    clocksi_test4 (Nodes),
    clocksi_test_read_time(Nodes),
    clocksi_test_read_wait(Nodes),
    clocksi_test_certification_check(Nodes),
    clocksi_multiple_test_certification_check(Nodes),
    clocksi_multiple_read_update_test(Nodes),
    rt:clean_cluster(Nodes),
    pass.

%% @doc The following function tests that ClockSI can run a non-interactive tx
%%      that updates multiple partitions.
clocksi_test1(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test1 started"),
    Type = riak_dt_pncounter,
    Result=rpc:call(FirstNode, floppy, clocksi_execute_tx,
                    [now(),
                     [{update, 11, Type, {increment, a}},
                      {update, 11, Type, {increment, a}},
                      {read, 11, riak_dt_pncounter},
                      {update, 12, Type, {increment, a}},
                      {read, 12, riak_dt_pncounter}]]),
    lager:info("*** ~p",[Result]),
    {ok, {_, ReadSet, _}}=Result,
    ?assertMatch([2,1], ReadSet),
    lager:info("Test1 passed"),
    pass.

%% @doc The following function tests that ClockSI can run an interactive tx.
%%      that updates multiple partitions.
clocksi_test2(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Type = riak_dt_pncounter,
    {ok,TxId}=rpc:call(FirstNode, floppy, clocksi_istart_tx, [now()]),
    ReadResult0=rpc:call(FirstNode, floppy, clocksi_iread,
                         [TxId, abc, riak_dt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, floppy, clocksi_iupdate,
                         [TxId, abc, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, floppy, clocksi_iread,
                        [TxId, abc, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult),
    WriteResult1=rpc:call(FirstNode, floppy, clocksi_iupdate,
                          [TxId, bcd, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, floppy, clocksi_iread,
                         [TxId, bcd, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult1),
    WriteResult2=rpc:call(FirstNode, floppy, clocksi_iupdate,
                          [TxId, cde, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult2),
    ReadResult2=rpc:call(FirstNode, floppy, clocksi_iread,
                         [TxId, cde, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult2),
    CommitTime=rpc:call(FirstNode, floppy, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    End=rpc:call(FirstNode, floppy, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),

    ReadResult3 = rpc:call(FirstNode, floppy, clocksi_read, [now(), abc, Type]),
    {ok, {_,[ReadVal],_}} = ReadResult3,
    ?assertEqual(ReadVal, 1),
    lager:info("Test2 passed"),
    pass.

%% @doc Test to execute transaction with out explicit clock time
clocksi_tx_noclock_test(Nodes) ->
    FirstNode = hd(Nodes),
    Key = itx,
    Type = riak_dt_pncounter,
    {ok,TxId}=rpc:call(FirstNode, floppy, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, floppy, clocksi_iread,
                         [TxId, Key, riak_dt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult0=rpc:call(FirstNode, floppy, clocksi_iupdate,
                          [TxId, Key, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult0),
    CommitTime=rpc:call(FirstNode, floppy, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    End=rpc:call(FirstNode, floppy, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),
    ReadResult1 = rpc:call(FirstNode, floppy, clocksi_read,
                           [now(), Key, riak_dt_pncounter]),
    {ok, {_, ReadSet1, _}}= ReadResult1,
    ?assertMatch([1], ReadSet1),

    FirstNode = hd(Nodes),
    WriteResult1 = rpc:call(FirstNode, floppy, clocksi_bulk_update,
                            [[{update, Key, Type, {increment, a}}]]),
    ?assertMatch({ok, _}, WriteResult1),
    ReadResult2= rpc:call(FirstNode, floppy, clocksi_read,
                          [now(), Key, riak_dt_pncounter]),
    {ok, {_, ReadSet2, _}}=ReadResult2,
    ?assertMatch([2], ReadSet2),
    lager:info("Test3 passed"),
    pass.

%% @doc The following function tests that ClockSI can run both a single
%%      read and a bulk-update tx.
clocksi_single_key_update_read_test(Nodes) ->
    lager:info("Test3 started"),
    FirstNode = hd(Nodes),
    Key = k3,
    Type = riak_dt_pncounter,
    Result= rpc:call(FirstNode, floppy, clocksi_bulk_update,
                     [now(),
                      [{update, Key, Type, {increment, a}},
                       {update, Key, Type, {increment, b}}]]),
    ?assertMatch({ok, _}, Result),
    Result2= rpc:call(FirstNode, floppy, clocksi_read,
                      [now(), Key, riak_dt_pncounter]),
    {ok, {_, ReadSet, _}}=Result2,
    ?assertMatch([2], ReadSet),
    lager:info("Test3 passed"),
    pass.

%% @doc Verify that multiple reads/writes are successful.
clocksi_multiple_key_update_read_test(Nodes) ->
    Firstnode = hd(Nodes),
    Type = riak_dt_pncounter,
    Key1 = keym1,
    Key2 = keym2,
    Key3 = keym3,
    Ops = [{update,Key1, Type, {increment,a}},
           {update,Key2, Type, {{increment,10},a}},
           {update,Key3, Type, {increment,a}}],
    Writeresult = rpc:call(Firstnode, floppy, clocksi_bulk_update,
                           [now(), Ops]),
    ?assertMatch({ok,{_Txid, _Readset, _Committime}}, Writeresult),

    {ok,{_,[ReadResult1],_}} = rpc:call(Firstnode, floppy, clocksi_read,
                                        [now(), Key1, riak_dt_pncounter]),
    {ok,{_,[ReadResult2],_}} = rpc:call(Firstnode, floppy, clocksi_read,
                                        [now(), Key2, riak_dt_pncounter]),
    {ok,{_,[ReadResult3],_}} = rpc:call(Firstnode, floppy, clocksi_read,
                                        [now(), Key3, riak_dt_pncounter]),
    ?assertMatch(ReadResult1,1),
    ?assertMatch(ReadResult2,10),
    ?assertMatch(ReadResult3,1),
    pass.

%% @doc The following function tests that ClockSI can excute a
%%      read-only interactive tx.
clocksi_test4(Nodes) ->
    lager:info("Test4 started"),
    FirstNode = hd(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    {ok,TxId1}=rpc:call(FirstNode, floppy, clocksi_istart_tx, [now()]),

    lager:info("Tx Started, id : ~p", [TxId1]),
    ReadResult1=rpc:call(FirstNode, floppy, clocksi_iread,
                         [TxId1, abc, riak_dt_pncounter]),
    lager:info("Tx Reading..."),
    ?assertMatch({ok, _}, ReadResult1),
    lager:info("Tx Read value...~p", [ReadResult1]),
    CommitTime1=rpc:call(FirstNode, floppy, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(FirstNode, floppy, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx Committed."),
    lager:info("Test 4 passed."),
    pass.

%% @doc The following function tests that ClockSI waits, when reading,
%%      for a tx that has updated an element that it wants to read and
%%      has a smaller TxId, but has not yet committed.
clocksi_test_read_time(Nodes) ->
    %% Start a new tx,  perform an update over key abc, and send prepare.
    lager:info("Test read_time started"),
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    Type = riak_dt_pncounter,
    {ok,TxId}=rpc:call(FirstNode, floppy, clocksi_istart_tx, [now()]),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    %% start a different tx and try to read key read_time.
    {ok,TxId1}=rpc:call(LastNode, floppy, clocksi_istart_tx, [now()]),

    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult=rpc:call(FirstNode, floppy, clocksi_iupdate,
                         [TxId, read_time, Type, {increment, 4}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),
    CommitTime=rpc:call(FirstNode, floppy, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    lager:info("Tx1 sent prepare, got commitTime=..., id : ~p", [CommitTime]),
    %% try to read key read_time.

    lager:info("Tx2 Reading..."),
    ReadResult1=rpc:call(LastNode, floppy, clocksi_iread,
                         [TxId1, read_time, riak_dt_pncounter]),
    lager:info("Tx2 Reading..."),
    ?assertMatch({ok, 0}, ReadResult1),
    lager:info("Tx2 Read value...~p", [ReadResult1]),

    %% commit the first tx.
    End=rpc:call(FirstNode, floppy, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),
    lager:info("Tx1 Committed."),

    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, floppy, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(LastNode, floppy, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),
    lager:info("Test read_time passed"),
    pass.

%% @doc The following function tests that ClockSI does not read values
%%      inserted by a tx with higher commit timestamp than the snapshot time
%%      of the reading tx.
clocksi_test_read_wait(Nodes) ->
    lager:info("Test read_wait started"),
    %% Start a new tx, update a key read_wait_test, and send prepare.
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    Type = riak_dt_pncounter,
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    {ok,TxId}=rpc:call(FirstNode, floppy, clocksi_istart_tx, [now()]),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, floppy, clocksi_iupdate,
                         [TxId, read_wait_test, Type, {increment, 4}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),
    {ok, CommitTime}=rpc:call(FirstNode, floppy, clocksi_iprepare, [TxId]),
    lager:info("Tx1 sent prepare, got commitTime=..., id : ~p", [CommitTime]),
    %% start a different tx and try to read key read_wait_test.
    {ok,TxId1}=rpc:call(LastNode, floppy, clocksi_istart_tx,
                        [CommitTime]),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    lager:info("Tx2 Reading..."),
    Pid=spawn(?MODULE, spawn_read, [LastNode, TxId1, self()]),
    %% Delay first transaction
    timer:sleep(100),
    %% commit the first tx.
    End=rpc:call(FirstNode, floppy, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),
    lager:info("Tx1 Committed."),

    receive
        {Pid, ReadResult1} ->
            %%receive the read value
            ?assertMatch({ok, 1}, ReadResult1),
            lager:info("Tx2 Read value...~p", [ReadResult1])
    end,

    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, floppy, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(LastNode, floppy, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),
    lager:info("Test read_wait passed"),
    pass.

spawn_read(LastNode, TxId, Return) ->
    ReadResult=rpc:call(LastNode, floppy, clocksi_iread,
                        [TxId, read_wait_test, riak_dt_pncounter]),
    Return ! {self(), ReadResult}.

%% @doc The following function tests the certification check algorithm,
%%      when two concurrent txs modify a single object, one hast to abort.
clocksi_test_certification_check(Nodes) ->
    lager:info("clockSI_test_certification_check started"),
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    Type = riak_dt_pncounter,
    %% Start a new tx,  perform an update over key write.
    {ok,TxId}=rpc:call(FirstNode, floppy, clocksi_istart_tx, [now()]),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, floppy, clocksi_iupdate,
                         [TxId, write, Type, {increment, 1}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),

    %% Start a new tx,  perform an update over key write.
    {ok,TxId1}=rpc:call(LastNode, floppy, clocksi_istart_tx, [now()]),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult1=rpc:call(LastNode, floppy, clocksi_iupdate,
                          [TxId1, write, Type, {increment, 2}]),
    lager:info("Tx2 Writing..."),
    ?assertEqual(ok, WriteResult1),
    lager:info("Tx1 finished concurrent write..."),

    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, floppy, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(LastNode, floppy, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),

    %% commit the first tx.
    CommitTime=rpc:call(FirstNode, floppy, clocksi_iprepare, [TxId]),
    ?assertMatch({aborted, TxId}, CommitTime),
    lager:info("Tx1 sent prepare, got message: ~p", [CommitTime]),
    lager:info("Tx1 aborted. Test passed!"),
    pass.

%% @doc The following function tests the certification check algorithm.
%%      when two concurrent txs modify a single object, one hast to abort.
%%      Besides, it updates multiple partitions.
clocksi_multiple_test_certification_check(Nodes) ->
    lager:info("clockSI_test_certification_check started"),
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    Type = riak_dt_pncounter,
    %% Start a new tx,  perform an update over key write.
    {ok,TxId}=rpc:call(FirstNode, floppy, clocksi_istart_tx, [now()]),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, floppy, clocksi_iupdate,
                         [TxId, write, Type, {increment, 1}]),
    lager:info("Tx1 Writing 1..."),
    ?assertEqual(ok, WriteResult),
    WriteResultb=rpc:call(FirstNode, floppy, clocksi_iupdate,
                          [TxId, write2, Type, {increment, 1}]),
    lager:info("Tx1 Writing 2..."),
    ?assertEqual(ok, WriteResultb),
    WriteResultc=rpc:call(FirstNode, floppy, clocksi_iupdate,
                          [TxId, write3, Type, {increment, 1}]),
    lager:info("Tx1 Writing 3..."),
    ?assertEqual(ok, WriteResultc),

    %% Start a new tx,  perform an update over key write.
    {ok,TxId1}=rpc:call(LastNode, floppy, clocksi_istart_tx, [now()]),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult1=rpc:call(LastNode, floppy, clocksi_iupdate,
                          [TxId1, write, Type, {increment, 2}]),
    lager:info("Tx2 Writing..."),
    ?assertEqual(ok, WriteResult1),
    lager:info("Tx1 finished concurrent write..."),

    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, floppy, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(LastNode, floppy, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),

    %% commit the first tx.
    CommitTime=rpc:call(FirstNode, floppy, clocksi_iprepare, [TxId]),
    ?assertMatch({aborted, TxId}, CommitTime),
    lager:info("Tx1 sent prepare, got message: ~p", [CommitTime]),
    lager:info("Tx1 aborted. Test passed!"),
    pass.

%% @doc Read an update a key multiple times.
clocksi_multiple_read_update_test(Nodes) ->
    Node = hd(Nodes),
    Key = get_random_key(),
    NTimes = 100,
    {ok,Result1} = rpc:call(Node, floppy, read,
                       [Key, riak_dt_pncounter]),
    lists:foreach(fun(_)->
                          read_update_test(Node, Key) end,
                  lists:seq(1,NTimes)),
    {ok,Result2} = rpc:call(Node, floppy, read,
                       [Key, riak_dt_pncounter]),
    ?assertEqual(Result1+NTimes, Result2),
    pass.

%% @doc Test updating prior to a read.
read_update_test(Node, Key) ->
    Type = riak_dt_pncounter,
    {ok,Result1} = rpc:call(Node, floppy, read,
                       [Key, Type]),
    {ok,_} = rpc:call(Node, floppy, clocksi_bulk_update,
                      [now(), [{update, Key, Type, {increment,a}}]]),
    {ok,Result2} = rpc:call(Node, floppy, read,
                       [Key, Type]),
    ?assertEqual(Result1+1,Result2),
    pass.

get_random_key() ->
    random:seed(now()),
    random:uniform(1000).
