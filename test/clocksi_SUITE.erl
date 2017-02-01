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

-module(clocksi_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([clocksi_test1/1,
         clocksi_test2/1,
         clocksi_test3/1,
         clocksi_test5/1,
         clocksi_test_read_wait/1,
         clocksi_test4/1,
         clocksi_test_read_time/1,
         clocksi_test_prepare/1,
         clocksi_tx_noclock_test/1,
         clocksi_single_key_update_read_test/1,
         clocksi_multiple_key_update_read_test/1,
         clocksi_test_certification_check/1,
         clocksi_multiple_test_certification_check/1,
         clocksi_multiple_read_update_test/1,
         clocksi_concurrency_test/1,
         clocksi_parallel_ops_test/1,
         clocksi_static_parallel_writes_test/1,
         spawn_read/5,
         spawn_com/2]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

init_per_suite(Config) ->
    lager_common_test_backend:bounce(debug),
    test_utils:at_init_testsuite(),

    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = hd(Clusters), %% This test needs only one cluster
    % Nodes = test_utils:pmap(fun(N) ->
    %                 test_utils:start_suite(N, Config)
    %         end, [dev1, dev2, dev3]),
    % ok = test_utils:join_cluster(Nodes),
    % Check that the clocksi protocol is tested
    {ok, Prot} = rpc:call(hd(Nodes), application, get_env, [antidote, txn_prot]),
    ?assertMatch(clocksi, Prot),

    %test_utils:connect_dcs(Nodes),
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
     Config.

end_per_testcase(_, _) ->
    ok.

all() -> [clocksi_test1,
         clocksi_test2,
         clocksi_test3,
         clocksi_test5,
         clocksi_test_read_wait,
         clocksi_test4,
         clocksi_test_read_time,
         clocksi_test_prepare,
         clocksi_tx_noclock_test,
         clocksi_single_key_update_read_test,
         clocksi_multiple_key_update_read_test,
         clocksi_test_certification_check,
         clocksi_multiple_test_certification_check,
         clocksi_multiple_read_update_test,
         clocksi_concurrency_test,
         clocksi_parallel_ops_test,
         clocksi_static_parallel_writes_test].

%% @doc The following function tests that ClockSI can run a non-interactive tx
%%      that updates multiple partitions.
clocksi_test1(Config) ->
    Nodes = proplists:get_value(nodes, Config),

    FirstNode = hd(Nodes),
    lager:info("Test1 started"),
    Key1=clocksi_test1_key1,
    Key2=clocksi_test1_key2,
    Type = antidote_crdt_counter,
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
                      {update, {Key1, Type, {increment, 1}}},
                      {update, {Key2, Type, {increment, 1}}},
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
                     [{update, {Key1, Type, {increment, 1}}},
                      {update, {Key1, Type, {increment, 1}}}]]),
    ?assertMatch({ok,_}, Result5),

    Result6=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                     [{read, {Key1, Type}}]]),
    {ok, {_, ReadSet6, _}}=Result6,
    ?assertEqual(3, hd(ReadSet6)),
    pass.

%% @doc The following function tests that ClockSI can run an interactive tx.
%%      that updates multiple partitions.
clocksi_test2(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Type = antidote_crdt_counter,
    Key1=clocksi_test2_key1,
    Key2=clocksi_test2_key2,
    Key3=clocksi_test2_key3,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 1}]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 1}, ReadResult),
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key2, Type, {increment, 1}]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key2, antidote_crdt_counter]),
    ?assertEqual({ok, 1}, ReadResult1),
    WriteResult2=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key3, Type, {increment, 1}]),
    ?assertEqual(ok, WriteResult2),
    ReadResult2=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key3, antidote_crdt_counter]),
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
clocksi_test3(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Key1=clocksi_test3_key1,
    Key2=clocksi_test3_key2,
    Key3=clocksi_test3_key3,
    Type = antidote_crdt_counter,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 1}]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 1}, ReadResult),
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key2, Type, {increment, 1}]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key2, antidote_crdt_counter]),
    ?assertEqual({ok, 1}, ReadResult1),
    WriteResult2=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key3, Type, {increment, 1}]),
    ?assertEqual(ok, WriteResult2),
    ReadResult2=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key3, antidote_crdt_counter]),
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
clocksi_test_prepare(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test prepare started"),
    Type = antidote_crdt_counter,

    Key1=clocksi_test_prepare_key1,
    Preflist = rpc:call(FirstNode,log_utilities,get_preflist_from_key,[aaa]),
    IndexNode = hd(Preflist),

    Key2 = find_key_same_node(FirstNode,IndexNode,1),

    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, increment]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 1}, ReadResult),
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),

    timer:sleep(3000),

    {ok,TxIdRead}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),

    timer:sleep(3000),

    {ok,TxId1}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId1, Key2, Type, {increment, 1}]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId1, Key2, antidote_crdt_counter]),
    ?assertEqual({ok, 1}, ReadResult1),
    CommitTime1=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),

    spawn(?MODULE, spawn_com, [FirstNode, TxId]),

    ReadResultR=rpc:call(FirstNode, antidote, clocksi_iread,
			 [TxIdRead, Key1, antidote_crdt_counter]),
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
clocksi_test5(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Type = antidote_crdt_counter,
    Key1=clocksi_test5_key1,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, increment]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 1}, ReadResult),
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key1, Type, increment]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 2}, ReadResult1),
    WriteResult2=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key1, Type, {increment, 1}]),
    ?assertEqual(ok, WriteResult2),
    ReadResult2=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key1, antidote_crdt_counter]),
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
clocksi_tx_noclock_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    Key = clocksi_tx_noclock_test_key1,
    Type = antidote_crdt_counter,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key, antidote_crdt_counter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult0=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key, Type, increment]),
    ?assertEqual(ok, WriteResult0),
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    End=rpc:call(FirstNode, antidote, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),
    ReadResult1 = rpc:call(FirstNode, antidote, clocksi_read,
                           [Key, antidote_crdt_counter]),
    {ok, {_, ReadSet1, _}}= ReadResult1,
    ?assertMatch([1], ReadSet1),

    FirstNode = hd(Nodes),
    WriteResult1 = rpc:call(FirstNode, antidote, clocksi_bulk_update,
                            [[{update, {Key, Type, increment}}]]),
    ?assertMatch({ok, _}, WriteResult1),
    ReadResult2= rpc:call(FirstNode, antidote, clocksi_read,
                          [Key, antidote_crdt_counter]),
    {ok, {_, ReadSet2, _}}=ReadResult2,
    ?assertMatch([2], ReadSet2),
    lager:info("clocksi_tx_noclock_test passed"),
    pass.

%% @doc The following function tests that ClockSI can run both a single
%%      read and a bulk-update tx.
clocksi_single_key_update_read_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("Test3 started"),
    FirstNode = hd(Nodes),
    Key = clocksi_single_key_update_read_test_key1,
    Type = antidote_crdt_counter,
    Result= rpc:call(FirstNode, antidote, clocksi_bulk_update,
                     [
                      [{update, {Key, Type, {increment, 1}}},
                       {update, {Key, Type, increment}}
                      ]]),
    ?assertMatch({ok, _}, Result),
    {ok,{_,_,CommitTime}} = Result,
    Result2= rpc:call(FirstNode, antidote, clocksi_read,
                      [CommitTime, Key, Type]),
    {ok, {_, ReadSet, _}}=Result2,
    ?assertMatch([2], ReadSet),
    lager:info("clocksi_single_key_update_read_test passed"),
    pass.

%% @doc Verify that multiple reads/writes are successful.
clocksi_multiple_key_update_read_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Firstnode = hd(Nodes),
    Type = antidote_crdt_counter,
    Key1 = clocksi_multiple_key_update_read_test_key1,
    Key2 = clocksi_multiple_key_update_read_test_key2,
    Key3 = clocksi_multiple_key_update_read_test_key3,
    Ops = [{update, {Key1, Type, {increment,1}}},
           {update, {Key2, Type, {increment,10}}},
           {update,{Key3, Type, increment}}
          ],
    Writeresult = rpc:call(Firstnode, antidote, clocksi_bulk_update,
                           [Ops]),
    ?assertMatch({ok,{_Txid, _Readset, _Committime}}, Writeresult),
    {ok,{_Txid, _Readset, Committime}} = Writeresult,
    {ok,{_,[ReadResult1],_}} = rpc:call(Firstnode, antidote, clocksi_read,
                                        [Committime, Key1, Type]),
    {ok,{_,[ReadResult2],_}} = rpc:call(Firstnode, antidote, clocksi_read,
                                        [Committime, Key2, Type]),
    {ok,{_,[ReadResult3],_}} = rpc:call(Firstnode, antidote, clocksi_read,
                                        [Committime, Key3, Type]),
    ?assertMatch(ReadResult1,1),
    ?assertMatch(ReadResult2,10),
    ?assertMatch(ReadResult3,1),
    pass.

%% @doc The following function tests that ClockSI can excute a
%%      read-only interactive tx.
clocksi_test4(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("Test4 started"),
    FirstNode = hd(Nodes),
    Key1 = clocksi_test4_key1,
    Type = antidote_crdt_counter,
    lager:info("Node1: ~p", [FirstNode]),
    {ok,TxId1}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),

    lager:info("Tx Started, id : ~p", [TxId1]),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId1, Key1, Type]),
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
clocksi_test_read_time(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Start a new tx,  perform an update over key abc, and send prepare.
    lager:info("Test read_time started"),
    Key1 = clocksi_test_read_time_key1,
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    Type = antidote_crdt_counter,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    %% start a different tx and try to read key read_time.
    {ok,TxId1}=rpc:call(LastNode, antidote, clocksi_istart_tx, []),

    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 1}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    lager:info("Tx1 sent prepare, got commitTime=..., id : ~p", [CommitTime]),
    %% try to read key read_time.

    lager:info("Tx2 Reading..."),
    ReadResult1=rpc:call(LastNode, antidote, clocksi_iread,
                         [TxId1, Key1, Type]),
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
clocksi_test_read_wait(Config) ->
    Nodes = proplists:get_value(nodes, Config),

    Key1 = clocksi_test_read_wait_key1,
    Type = antidote_crdt_counter,
    %% Start a new tx, update a key read_wait_test, and send prepare.

    FirstNode = hd(Nodes),
    lager:info("FirstNode: ~p", [FirstNode]),
    {ok, TxId1} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 started, id : ~p", [TxId1]),
    WriteResult = rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId1, Key1, Type, {increment, 1}]),

    lager:info("Tx1 writing..."),
    ?assertEqual(ok, WriteResult),
    {ok, CommitTime1} = rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId1]),
    lager:info("Tx1 sent prepare, assigned commitTime : ~p", [CommitTime1]),

    %% Start a different tx and try to read the key
    {ok, TxId2} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx2 started with id : ~p", [TxId2]),
    Pid = spawn(?MODULE, spawn_read, [FirstNode, TxId2, self(), Key1, Type]),

    %% Delay first transaction
    timer:sleep(2000),

    %% Commit the first tx.
    End1 = rpc:call(FirstNode, antidote, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx1 committed."),

    receive
        {Pid, ReadResult1} ->
            %%receive the read value
            ?assertMatch({ok, 1}, ReadResult1)
            end,

    %% Prepare and commit the second transaction.
    CommitTime2 = rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId2]),
    ?assertMatch({ok, _}, CommitTime2),
    lager:info("Tx2 sent prepare, got id : ~p", [CommitTime2]),
    End2 = rpc:call(FirstNode, antidote, clocksi_icommit, [TxId2]),
    ?assertMatch({ok, _}, End2),
    lager:info("Tx2 committed."),
    pass.

spawn_read(Node, TxId, Return, Key, Type) ->
    ReadResult = rpc:call(Node, antidote, clocksi_iread,
                        [TxId, Key, Type]),
    Return ! {self(), ReadResult}.

%% @doc The following function tests the certification check algorithm,
%%      when two concurrent txs modify a single object, one hast to abort.
clocksi_test_certification_check(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    case rpc:call(hd(Nodes), application, get_env, [antidote, txn_cert]) of
        {ok, true} ->
            clocksi_test_certification_check_run(Nodes);
        _ ->
            pass
    end.

clocksi_test_certification_check_run(Nodes) ->
    lager:info("clockSI_test_certification_check started"),
    Key1 = clockSI_test_certification_check_key1,
    Type = antidote_crdt_counter,

    FirstNode = hd(Nodes),
    LastNode = lists:last(Nodes),
    lager:info("FirstNode: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),

    %% Start a new tx on first node, perform an update on some key.
    {ok,TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 1}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),

    %% Start a new tx on last node, perform an update on the same key.
    {ok,TxId1} = rpc:call(LastNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult1 = rpc:call(LastNode, antidote, clocksi_iupdate,
                          [TxId1, Key1, Type, {increment, 1}]),
    lager:info("Tx2 Writing..."),
    ?assertEqual(ok, WriteResult1),
    lager:info("Tx1 finished concurrent write..."),

    %% Prepare and commit the second transaction.
    CommitTime1 = rpc:call(LastNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1 = rpc:call(LastNode, antidote, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),

    %% Commit the first tx.
    CommitTime = rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({aborted, TxId}, CommitTime),
    lager:info("Tx1 sent prepare, got message: ~p", [CommitTime]),
    lager:info("Tx1 aborted. Test passed!"),
    pass.

%% @doc The following function tests the certification check algorithm.
%%      when two concurrent txs modify a single object, one hast to abort.
%%      Besides, it updates multiple partitions.
clocksi_multiple_test_certification_check(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    case rpc:call(hd(Nodes), application, get_env, [antidote, txn_cert]) of
        {ok, true} ->
            clocksi_multiple_test_certification_check_run(Nodes);
        _ ->
            pass
    end.

clocksi_multiple_test_certification_check_run(Nodes) ->
    lager:info("clockSI_multiple_test_certification_check started"),

    Key1 = clocksi_multiple_test_certification_check_key1,
    Key2 = clocksi_multiple_test_certification_check_key2,
    Key3 = clocksi_multiple_test_certification_check_key3,
    Type = antidote_crdt_counter,

    FirstNode = hd(Nodes),
    LastNode = lists:last(Nodes),
    lager:info("FirstNode: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),

    %% Start a new tx,  perform an update on three keys.
    {ok,TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult = rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, Key1, Type, {increment, 1}]),
    lager:info("Tx1 Writing 1..."),
    ?assertEqual(ok, WriteResult),
    WriteResultb = rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key2, Type, {increment, 1}]),
    lager:info("Tx1 Writing 2..."),
    ?assertEqual(ok, WriteResultb),
    WriteResultc = rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key3, Type, {increment, 1}]),
    lager:info("Tx1 Writing 3..."),
    ?assertEqual(ok, WriteResultc),

    %% Start a new tx,  perform an update over key1.
    {ok,TxId1} = rpc:call(LastNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult1 = rpc:call(LastNode, antidote, clocksi_iupdate,
                          [TxId1, Key1, Type, {increment, 1}]),
    lager:info("Tx2 Writing..."),
    ?assertEqual(ok, WriteResult1),

    %% Prepare and commit the second transaction.
    CommitTime1 = rpc:call(LastNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1 = rpc:call(LastNode, antidote, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),

    %% Try to commit the first tx.
    CommitTime = rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({aborted, TxId}, CommitTime),
    lager:info("Tx1 sent prepare, got message: ~p", [CommitTime]),
    lager:info("Tx1 aborted. Test passed!"),
    pass.

%% @doc Read an update a key multiple times.
clocksi_multiple_read_update_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Key = get_random_key(),
    Type = antidote_crdt_counter,
    NTimes = 100,
    {ok,Result1} = rpc:call(Node, antidote, read,
                       [Key, Type]),
    lists:foreach(fun(_)->
                          read_update_test(Node, Key) end,
                  lists:seq(1,NTimes)),
    {ok,Result2} = rpc:call(Node, antidote, read,
                       [Key, Type]),
    ?assertEqual(Result1+NTimes, Result2),
    pass.

%% @doc Test updating prior to a read.
read_update_test(Node, Key) ->
    Type = antidote_crdt_counter,
    {ok,Result1} = rpc:call(Node, antidote, read,
                       [Key, Type]),
    {ok,_} = rpc:call(Node, antidote, clocksi_bulk_update,
                      [[{update, {Key, Type, {increment,1}}}]]),
    {ok,Result2} = rpc:call(Node, antidote, read,
                       [Key, Type]),
    ?assertEqual(Result1+1,Result2),
    pass.

get_random_key() ->
  rand_compat:seed(erlang:phash2([node()]),erlang:monotonic_time(),erlang:unique_integer()),
  rand_compat:uniform(1000).  % TODO use deterministic keys in testcase

%% @doc The following function tests how two concurrent transactions work
%%      when they are interleaved.
clocksi_concurrency_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("clockSI_concurrency_test started"),
    Node = hd(Nodes),
    %% read txn starts before the write txn's prepare phase,
    Key = clocksi_conc,
    Type = antidote_crdt_counter,
    {ok, TxId1} = rpc:call(Node, antidote, clocksi_istart_tx, []),
    rpc:call(Node, antidote, clocksi_iupdate,
             [TxId1, Key, Type, {increment, 1}]),
    rpc:call(Node, antidote, clocksi_iprepare, [TxId1]),
    {ok, TxId2} = rpc:call(Node, antidote, clocksi_istart_tx, []),
    Pid = self(),
    spawn( fun() ->
                   rpc:call(Node, antidote, clocksi_iupdate,
                            [TxId2, Key, Type, {increment, 1}]),
                   rpc:call(Node, antidote, clocksi_iprepare, [TxId2]),
                   {ok,_}= rpc:call(Node, antidote, clocksi_icommit, [TxId2]),
                   Pid ! ok
           end),

    {ok,_}= rpc:call(Node, antidote, clocksi_icommit, [TxId1]),
     receive
         ok ->
             Result= rpc:call(Node,
                              antidote, read, [Key, Type]),
             ?assertEqual({ok, 2}, Result),
             pass
     end.

%% doc The following test checks multiple updates/reads using
%% read/update_objects. It also checks that reads are returned
%% in the same order they were sent.
clocksi_parallel_ops_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bucket = test_bucket,
    Bound_object1 = {parallel_key1, antidote_crdt_counter, Bucket},
    Bound_object2 = {parallel_key2, antidote_crdt_counter, Bucket},
    Bound_object3 = {parallel_key3, antidote_crdt_counter, Bucket},
    Bound_object4 = {parallel_key4, antidote_crdt_counter, Bucket},
    Bound_object5 = {parallel_key5, antidote_crdt_counter, Bucket},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),

    %% update 5 different objects
    ok = rpc:call(Node, antidote, update_objects,
        [[{Bound_object1, increment, 1},
            {Bound_object2, increment, 2},
                {Bound_object3, increment, 3},
                    {Bound_object4, increment, 4},
                        {Bound_object5, increment, 5}],
                            TxId]),

    %% read the objects in the same transaction to see that the updates
    %% are seen.
    Res = rpc:call(Node, antidote, read_objects, [[Bound_object1,
        Bound_object2,Bound_object3,Bound_object4,Bound_object5], TxId]),
    ?assertMatch({ok, [1,2,3,4,5]}, Res),

    %% update 5 times the first object.
    ok = rpc:call(Node, antidote, update_objects,
        [[{Bound_object1, increment, 1},
            {Bound_object1, increment, 1},
            {Bound_object1, increment, 1},
            {Bound_object1, increment, 1},
            {Bound_object1, increment, 1}],
            TxId]),
    %% see that these updates are seen too.
    Res1 = rpc:call(Node, antidote, read_objects, [[Bound_object1], TxId]),
    ?assertMatch({ok, [6]}, Res1),
    {ok, _CT} = rpc:call(Node, antidote, commit_transaction, [TxId]),

    %% start a new transaction that reads the updated objects.
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    Res2 = rpc:call(Node, antidote, read_objects, [[Bound_object1,
        Bound_object2,Bound_object3,Bound_object4,Bound_object5], TxId2]),
    ?assertMatch({ok, [6,2,3,4,5]}, Res2),
    {ok, _CT2} = rpc:call(Node, antidote, commit_transaction, [TxId2]).

%% doc The following test tests sending multiple updates in a single
%% update_objects call.
%% it also tests that the coordinator StaysAlive after the first transaction,
%% and serves the request from the second one.
clocksi_static_parallel_writes_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bucket = test_bucket,
    Bound_object1 = {parallel_key6, antidote_crdt_counter, Bucket},
    Bound_object2 = {parallel_key7, antidote_crdt_counter, Bucket},
    Bound_object3 = {parallel_key8, antidote_crdt_counter, Bucket},
    Bound_object4 = {parallel_key9, antidote_crdt_counter, Bucket},
    Bound_object5 = {parallel_key10, antidote_crdt_counter, Bucket},
    %% update 5 different objects
    {ok, CT} = rpc:call(Node, antidote, update_objects, [ignore, [],
        [{Bound_object1, increment, 1},
            {Bound_object2, increment, 2},
            {Bound_object3, increment, 3},
            {Bound_object4, increment, 4},
            {Bound_object5, increment, 5}], true]),

    lager:info("updated 5 objects no problem"),

    {ok, Res, CT1} = rpc:call(Node, antidote, read_objects, [CT, [], [Bound_object1,
        Bound_object2,Bound_object3,Bound_object4,Bound_object5], true]),
    ?assertMatch([1,2,3,4,5], Res),

    lager:info("read 5 objects no problem"),

    %% update 5 times the first object.
    {ok, CT2} = rpc:call(Node, antidote, update_objects, [CT1, [],
        [{Bound_object1, increment, 1},
            {Bound_object1, increment, 1},
            {Bound_object1, increment, 1},
            {Bound_object1, increment, 1},
            {Bound_object1, increment, 1}], true]),

    lager:info("updated 5 times the sabe object, no problem"),

    {ok, Res1, _CT4} = rpc:call(Node, antidote, read_objects, [CT2, [], [Bound_object1], true]),
    ?assertMatch([6], Res1),
    lager:info("result is correct after reading those updates. Test passed."),
    pass.
