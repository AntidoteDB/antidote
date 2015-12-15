%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
% This file is provided to you under the Apache License,
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

-export([confirm/0,
         clocksi_test1/1,
	     clocksi_test2/1,
	     clocksi_test3/1,
	     clocksi_test5/1,
         clocksi_test_read_wait/1,
	     clocksi_test4/1,
	     clocksi_test_read_time/1,
         spawn_read/4,
	     clocksi_test_prepare/1,
         clocksi_tx_noclock_test/1,
         clocksi_single_key_update_read_test/1,
         clocksi_multiple_key_update_read_test/1,
         clocksi_test_certification_check/1,
         clocksi_multiple_test_certification_check/1,
         clocksi_multiple_read_update_test/1,
         clocksi_concurrency_test/1,
	     spawn_com/2]).

-include_lib("eunit/include/eunit.hrl").
-include("antidote.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
                              {riak_core, [{ring_creation_size, NumVNodes}]},
                              {antidote, [{txn_prot, clocksi}]}                              
                             ]),
    [Nodes] = rt:build_clusters([3]),
    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Nodes),fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),
    lager:info("Nodes: ~p", [Nodes]),
    {ok, Prot} = rpc:call(hd(Nodes), application, get_env, [antidote, txn_prot]),
    ?assertMatch(clocksi, Prot),
    clocksi_test1(Nodes),

    [Nodes1] = common:clean_clusters([Nodes]),
    clocksi_test2(Nodes1),

    [Nodes2] = common:clean_clusters([Nodes1]),
    clocksi_test3(Nodes2),

    [Nodes3] = common:clean_clusters([Nodes2]),
    clocksi_test_prepare(Nodes3),

    [Nodes4] = common:clean_clusters([Nodes3]),
    clocksi_test5(Nodes4),

    [Nodes5] = common:clean_clusters([Nodes4]),
    clocksi_tx_noclock_test(Nodes5),

    [Nodes6] = common:clean_clusters([Nodes5]),
    clocksi_single_key_update_read_test(Nodes6),

    [Nodes7] = common:clean_clusters([Nodes6]),
    clocksi_multiple_key_update_read_test(Nodes7),

    [Nodes8] = common:clean_clusters([Nodes7]),
    clocksi_test4 (Nodes8),

    [Nodes9] = common:clean_clusters([Nodes8]),
    clocksi_test_read_time(Nodes9),

    [Nodes10] = common:clean_clusters([Nodes9]),
    clocksi_test_read_wait(Nodes10),

    [Nodes11] = common:clean_clusters([Nodes10]),
    clocksi_multiple_read_update_test(Nodes11),

    [Nodes12] = common:clean_clusters([Nodes11]),
    clocksi_concurrency_test(Nodes12),

    case rpc:call(hd(Nodes), application, get_env, [antidote, txn_cert]) of
        {ok, true} ->
            [Nodes13] = common:clean_clusters([Nodes12]),
            clocksi_test_certification_check(Nodes13),

            [Nodes14] = common:clean_clusters([Nodes13]),
            clocksi_multiple_test_certification_check(Nodes14);
        _ -> 
            pass
    end,
    pass.

%% @doc The following function tests that ClockSI can run a non-interactive tx
%%      that updates multiple partitions.
clocksi_test1(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test1 started"),
    Key1=clocksi_test1_key1,
    Key2=clocksi_test1_key2,
    Type = riak_dt_pncounter,
    %% Empty transaction works,
    Result0=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[]]),
    ?assertMatch({ok, _}, Result0),
    Result1=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[]]),
    ?assertMatch({ok, _}, Result1),

    % A simple read returns empty
    Result11=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                     [{read, {Key1, Type}}]]),
    ?assertMatch({ok, _}, Result11),
    {ok, {_, ReadSet11, _}}=Result11, 
    ?assertMatch([0], ReadSet11),

    %% Read what you wrote
    Result2=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                      [{read, {Key1, Type}},
                      {update, {Key1, Type, {increment, a}}},
                      {update, {Key2, Type, {increment, a}}},
                      {read, {Key1, Type}}]]),
    ?assertMatch({ok, _}, Result2),
    {ok, {_, ReadSet2, _}}=Result2, 
    ?assertMatch([0,1], ReadSet2),

    %% Update is persisted && update to multiple keys are atomic
    Result3=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                     [{read, {Key1, Type}},
                      {read, {Key2, Type}}]]),
    ?assertMatch({ok, _}, Result3),
    {ok, {_, ReadSet3, _}}=Result3,
    ?assertEqual([1,1], ReadSet3),

    %% Multiple updates to a key in a transaction works
    Result5=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                     [{update, {Key1, Type, {increment, a}}},
                      {update, {Key1, Type, {increment, a}}}]]),
    ?assertMatch({ok,_}, Result5),

    Result6=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                     [{read, {Key1, Type}}]]),
    {ok, {_, ReadSet6, _}}=Result6,
    ?assertEqual(3, hd(ReadSet6)),
    pass.

%% @doc The following function tests that ClockSI can run an interactive tx.
%%      that updates multiple partitions.
clocksi_test2(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Type = riak_dt_pncounter,
    Key1=clocksi_test2_key1,
    Key2=clocksi_test2_key2,
    Key3=clocksi_test2_key3,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult),
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key2, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key2, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult1),
    WriteResult2=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key3, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult2),
    ReadResult2=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key3, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult2),
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    End=rpc:call(FirstNode, antidote, clocksi_icommit, [TxId]),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End),
    {ok,{_Txid, CausalSnapshot}} = End,
    ReadResult3 = rpc:call(FirstNode, antidote, clocksi_read,
                           [CausalSnapshot, Key1, Type]),
    {ok, {_,[ReadVal],_}} = ReadResult3,
    ?assertEqual(ReadVal, 1),
    lager:info("Test2 passed"),
    pass.

%% @doc The following function tests that ClockSI can run an interactive tx.
%%      It tests the API operation that allows clients to run interactive txs
%%      explicitely calling prepare and commit.
clocksi_test3(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Key1=clocksi_test3_key1,
    Key2=clocksi_test3_key2,
    Key3=clocksi_test3_key3,
    Type = riak_dt_pncounter,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult),
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key2, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key2, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult1),
    WriteResult2=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key3, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult2),
    ReadResult2=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key3, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult2),
    End=rpc:call(FirstNode, antidote, clocksi_full_icommit, [TxId]),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End),
    {ok,{_Txid, CausalSnapshot}} = End,
    ReadResult3 = rpc:call(FirstNode, antidote, clocksi_read,
                           [CausalSnapshot, Key1, Type]),
    {ok, {_,[ReadVal],_}} = ReadResult3,
    ?assertEqual(ReadVal, 1),
    lager:info("Test3 passed"),
    pass.

%% @doc This test makes sure to block pending reads when a prepare is in progress
%% that could violate atomicity if not blocked
clocksi_test_prepare(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test prepare started"),
    Type = riak_dt_pncounter,

    Key1=clocksi_test_prepare_key1,
    Preflist = rpc:call(FirstNode,log_utilities,get_preflist_from_key,[aaa],1),
    IndexNode = hd(Preflist),

    Key2 = find_key_same_node(FirstNode,IndexNode,1),

    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, a1}]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult),
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),

    timer:sleep(3000),

    {ok,TxIdRead}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),    

    timer:sleep(3000),

    {ok,TxId1}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId1, Key2, Type, {increment, a2}]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId1, Key2, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult1),
    CommitTime1=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
       
    spawn(?MODULE, spawn_com, [FirstNode, TxId]),

    ReadResultR=rpc:call(FirstNode, antidote, clocksi_iread,
			 [TxIdRead, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResultR),

    End1=rpc:call(FirstNode, antidote, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End1),
    
    lager:info("Test prepare passed"),
    pass.

find_key_same_node(FirstNode,IndexNode,Num) ->
    NewKey = list_to_atom(atom_to_list(aaa) ++ integer_to_list(Num)),
    Preflist = rpc:call(FirstNode,log_utilities,get_preflist_from_key,[aaa]),
    case hd(Preflist) == IndexNode of
	true ->
	    NewKey;
	false ->
	    find_key_same_node(FirstNode,IndexNode,Num+1)
    end.

spawn_com(FirstNode, TxId) ->
    timer:sleep(3000),
    End1 = rpc:call(FirstNode, antidote, clocksi_icommit, [TxId]),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End1).
    

%% @doc The following function tests that ClockSI can run an interactive tx.
%%      that updates only one partition. This type of txs use a only-one phase 
%%      commit.
clocksi_test5(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Type = riak_dt_pncounter,
    Key1=clocksi_test5_key1,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult),
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key1, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 2}, ReadResult1),
    WriteResult2=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key1, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult2),
    ReadResult2=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, riak_dt_pncounter]),
    ?assertEqual({ok, 3}, ReadResult2),
    End=rpc:call(FirstNode, antidote, clocksi_full_icommit, [TxId]),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End),
    {ok,{_Txid, CausalSnapshot}} = End,
    ReadResult3 = rpc:call(FirstNode, antidote, clocksi_read,
                           [CausalSnapshot, Key1, Type]),
    {ok, {_,[ReadVal],_}} = ReadResult3,
    ?assertEqual(ReadVal, 3),
    lager:info("Test5 passed"),
    pass.


%% @doc Test to execute transaction with out explicit clock time
clocksi_tx_noclock_test(Nodes) ->
    FirstNode = hd(Nodes),
    Key = clocksi_tx_noclock_test_key1,
    Type = riak_dt_pncounter,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key, riak_dt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult0=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult0),
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    End=rpc:call(FirstNode, antidote, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),
    ReadResult1 = rpc:call(FirstNode, antidote, clocksi_read,
                           [Key, riak_dt_pncounter]),
    {ok, {_, ReadSet1, _}}= ReadResult1,
    ?assertMatch([1], ReadSet1),

    FirstNode = hd(Nodes),
    WriteResult1 = rpc:call(FirstNode, antidote, clocksi_bulk_update,
                            [[{update, {Key, Type, {increment, a}}}]]),
    ?assertMatch({ok, _}, WriteResult1),
    ReadResult2= rpc:call(FirstNode, antidote, clocksi_read,
                          [Key, riak_dt_pncounter]),
    {ok, {_, ReadSet2, _}}=ReadResult2,
    ?assertMatch([2], ReadSet2),
    lager:info("Test3 passed"),
    pass.

%% @doc The following function tests that ClockSI can run both a single
%%      read and a bulk-update tx.
clocksi_single_key_update_read_test(Nodes) ->
    lager:info("Test3 started"),
    FirstNode = hd(Nodes),
    Key = clocksi_single_key_update_read_test_key1,
    Type = riak_dt_pncounter,
    Result= rpc:call(FirstNode, antidote, clocksi_bulk_update,
                     [
                      [{update, {Key, Type, {increment, a}}},
                       {update, {Key, Type, {increment, b}}}]]),
    ?assertMatch({ok, _}, Result),
    {ok,{_,_,CommitTime}} = Result,
    Result2= rpc:call(FirstNode, antidote, clocksi_read,
                      [CommitTime, Key, riak_dt_pncounter]),
    {ok, {_, ReadSet, _}}=Result2,
    ?assertMatch([2], ReadSet),
    lager:info("Test3 passed"),
    pass.

%% @doc Verify that multiple reads/writes are successful.
clocksi_multiple_key_update_read_test(Nodes) ->
    Firstnode = hd(Nodes),
    Type = riak_dt_pncounter,
    Key1 = clocksi_multiple_key_update_read_test_key1,
    Key2 = clocksi_multiple_key_update_read_test_key2,
    Key3 = clocksi_multiple_key_update_read_test_key3,
    Ops = [{update, {Key1, Type, {increment,a}}},
           {update, {Key2, Type, {{increment,10},a}}},
           {update,{Key3, Type, {increment,a}}}],
    Writeresult = rpc:call(Firstnode, antidote, clocksi_bulk_update,
                           [Ops]),
    ?assertMatch({ok,{_Txid, _Readset, _Committime}}, Writeresult),
    {ok,{_Txid, _Readset, Committime}} = Writeresult,
    {ok,{_,[ReadResult1],_}} = rpc:call(Firstnode, antidote, clocksi_read,
                                        [Committime, Key1, riak_dt_pncounter]),
    {ok,{_,[ReadResult2],_}} = rpc:call(Firstnode, antidote, clocksi_read,
                                        [Committime, Key2, riak_dt_pncounter]),
    {ok,{_,[ReadResult3],_}} = rpc:call(Firstnode, antidote, clocksi_read,
                                        [Committime, Key3, riak_dt_pncounter]),
    ?assertMatch(ReadResult1,1),
    ?assertMatch(ReadResult2,10),
    ?assertMatch(ReadResult3,1),
    pass.

%% @doc The following function tests that ClockSI can excute a
%%      read-only interactive tx.
clocksi_test4(Nodes) ->
    lager:info("Test4 started"),
    FirstNode = hd(Nodes),
    Key1 = clocksi_test4_key1,
    lager:info("Node1: ~p", [FirstNode]),
    {ok,TxId1}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),

    lager:info("Tx Started, id : ~p", [TxId1]),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId1, Key1, riak_dt_pncounter]),
    lager:info("Tx Reading..."),
    ?assertMatch({ok, _}, ReadResult1),
    lager:info("Tx Read value...~p", [ReadResult1]),
    CommitTime1=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(FirstNode, antidote, clocksi_icommit, [TxId1]),
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
    Key1 = clocksi_test_read_time_key1,
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    Type = riak_dt_pncounter,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    %% start a different tx and try to read key read_time.
    {ok,TxId1}=rpc:call(LastNode, antidote, clocksi_istart_tx, []),

    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 4}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    lager:info("Tx1 sent prepare, got commitTime=..., id : ~p", [CommitTime]),
    %% try to read key read_time.

    lager:info("Tx2 Reading..."),
    ReadResult1=rpc:call(LastNode, antidote, clocksi_iread,
                         [TxId1, Key1, riak_dt_pncounter]),
    lager:info("Tx2 Reading..."),
    ?assertMatch({ok, 0}, ReadResult1),
    lager:info("Tx2 Read value...~p", [ReadResult1]),

    %% commit the first tx.
    End=rpc:call(FirstNode, antidote, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),
    lager:info("Tx1 Committed."),

    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(LastNode, antidote, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),
    lager:info("Test read_time passed"),
    pass.

%% @doc The following function tests that ClockSI does not read values
%%      inserted by a tx with higher commit timestamp than the snapshot time
%%      of the reading tx.
clocksi_test_read_wait(Nodes) ->
    lager:info("Test read_wait started"),
    Key1 = clocksi_test_read_wait_key1,
    %% Start a new tx, update a key read_wait_test, and send prepare.
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    Type = riak_dt_pncounter,
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 4}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),
    {ok, CommitTime}=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    lager:info("Tx1 sent prepare, got commitTime=..., id : ~p", [CommitTime]),
    %% start a different tx and try to read key read_wait_test.
    {ok,TxId1}=rpc:call(LastNode, antidote, clocksi_istart_tx,
                        []),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    lager:info("Tx2 Reading..."),
    Pid=spawn(?MODULE, spawn_read, [LastNode, TxId1, self(), Key1]),
    %% Delay first transaction
    timer:sleep(100),
    %% commit the first tx.
    End=rpc:call(FirstNode, antidote, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),
    lager:info("Tx1 Committed."),

    receive
        {Pid, ReadResult1} ->
            %%receive the read value
            ?assertMatch({ok, 1}, ReadResult1),
            lager:info("Tx2 Read value...~p", [ReadResult1])
    end,

    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(LastNode, antidote, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),
    lager:info("Test read_wait passed"),
    pass.

spawn_read(LastNode, TxId, Return, Key) ->
    ReadResult=rpc:call(LastNode, antidote, clocksi_iread,
                        [TxId, Key, riak_dt_pncounter]),
    Return ! {self(), ReadResult}.

%% @doc The following function tests the certification check algorithm,
%%      when two concurrent txs modify a single object, one hast to abort.
clocksi_test_certification_check(Nodes) ->
    lager:info("clockSI_test_certification_check started"),
    Key1 = clockSI_test_certification_check_key1,
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    Type = riak_dt_pncounter,
    %% Start a new tx,  perform an update over key write.
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 1}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),

    %% Start a new tx,  perform an update over key write.
    {ok,TxId1}=rpc:call(LastNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult1=rpc:call(LastNode, antidote, clocksi_iupdate,
                          [TxId1, Key1, Type, {increment, 2}]),
    lager:info("Tx2 Writing..."),
    ?assertEqual(ok, WriteResult1),
    lager:info("Tx1 finished concurrent write..."),

    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(LastNode, antidote, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),

    %% commit the first tx.
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
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

    Key1 = clocksi_multiple_test_certification_check_key1,
    Key2 = clocksi_multiple_test_certification_check_key2,
    Key3 = clocksi_multiple_test_certification_check_key3,
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    Type = riak_dt_pncounter,
    %% Start a new tx,  perform an update over key write.
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 1}]),
    lager:info("Tx1 Writing 1..."),
    ?assertEqual(ok, WriteResult),
    WriteResultb=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key2, Type, {increment, 1}]),
    lager:info("Tx1 Writing 2..."),
    ?assertEqual(ok, WriteResultb),
    WriteResultc=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key3, Type, {increment, 1}]),
    lager:info("Tx1 Writing 3..."),
    ?assertEqual(ok, WriteResultc),

    %% Start a new tx,  perform an update over key write.
    {ok,TxId1}=rpc:call(LastNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult1=rpc:call(LastNode, antidote, clocksi_iupdate,
                          [TxId1, Key1, Type, {increment, 2}]),
    lager:info("Tx2 Writing..."),
    ?assertEqual(ok, WriteResult1),
    lager:info("Tx1 finished concurrent write..."),

    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(LastNode, antidote, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),

    %% commit the first tx.
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({aborted, TxId}, CommitTime),
    lager:info("Tx1 sent prepare, got message: ~p", [CommitTime]),
    lager:info("Tx1 aborted. Test passed!"),
    pass.

%% @doc Read an update a key multiple times.
clocksi_multiple_read_update_test(Nodes) ->
    Node = hd(Nodes),
    Key = get_random_key(),
    NTimes = 100,
    {ok,Result1} = rpc:call(Node, antidote, read,
                       [Key, riak_dt_pncounter]),
    lists:foreach(fun(_)->
                          read_update_test(Node, Key) end,
                  lists:seq(1,NTimes)),
    {ok,Result2} = rpc:call(Node, antidote, read,
                       [Key, riak_dt_pncounter]),
    ?assertEqual(Result1+NTimes, Result2),
    pass.

%% @doc Test updating prior to a read.
read_update_test(Node, Key) ->
    Type = riak_dt_pncounter,
    {ok,Result1} = rpc:call(Node, antidote, read,
                       [Key, Type]),
    {ok,_} = rpc:call(Node, antidote, clocksi_bulk_update,
                      [[{update, {Key, Type, {increment,a}}}]]),
    {ok,Result2} = rpc:call(Node, antidote, read,
                       [Key, Type]),
    ?assertEqual(Result1+1,Result2),
    pass.

get_random_key() ->
    random:seed(now()),
    random:uniform(1000).

%% @doc The following function tests how two concurrent transactions work
%%      when they are interleaved.
clocksi_concurrency_test(Nodes) ->
    lager:info("clockSI_concurrency_test started"),
    Node = hd(Nodes),
    %% read txn starts before the write txn's prepare phase,
    Key = conc,
    {ok, TxId1} = rpc:call(Node, antidote, clocksi_istart_tx, []),
    rpc:call(Node, antidote, clocksi_iupdate,
             [TxId1, Key, riak_dt_gcounter, {increment, ucl}]),
    rpc:call(Node, antidote, clocksi_iprepare, [TxId1]),
    {ok, TxId2} = rpc:call(Node, antidote, clocksi_istart_tx, []),
    Pid = self(),
    spawn( fun() ->
                   rpc:call(Node, antidote, clocksi_iupdate,
                            [TxId2, Key, riak_dt_gcounter, {increment, ucl1}]),
                   rpc:call(Node, antidote, clocksi_iprepare, [TxId2]),
                   {ok,_}= rpc:call(Node, antidote, clocksi_icommit, [TxId2]),
                   Pid ! ok
           end),

    {ok,_}= rpc:call(Node, antidote, clocksi_icommit, [TxId1]),
     receive
         ok ->
             Result= rpc:call(Node,
                              antidote, read, [Key, riak_dt_gcounter]),
             ?assertEqual({ok, 2}, Result),
             pass
     end.
