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
-module(physics_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([
    physics_read_nonupdated_key_test/1,
    physics_test1/1,
    physics_test2/1,
    physics_test3/1,
    physics_test5/1,
    physics_test_read_wait/1,
    physics_test4/1,
    physics_test_read_time/1,
    physics_test_prepare/1,
    physics_tx_noclock_test/1,
    physics_single_key_update_read_test/1,
    physics_multiple_key_update_read_test/1,
    physics_test_certification_check/1,
    physics_multiple_test_certification_check/1,
    physics_multiple_read_update_test/1,
    physics_concurrency_test/1,
    physics_snapshot_test/1,
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
    
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
            [antidote, txn_prot, physics]) end, Nodes),
    
    %Check that indeed clocksi is running
    {ok, Prot} = rpc:call(hd(Nodes), application, get_env, [antidote, txn_prot]),
    ?assertMatch(physics, Prot),
    
    %test_utils:connect_dcs(Nodes),
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [
             physics_read_nonupdated_key_test,
             physics_test1,
             physics_test2,
             physics_test3,
             physics_test5,
             physics_test_read_wait,
             physics_test4,
             physics_test_read_time,
             physics_test_prepare,
             physics_tx_noclock_test,
             physics_single_key_update_read_test,
             physics_multiple_key_update_read_test,
             physics_test_certification_check,
             physics_multiple_test_certification_check,
             physics_multiple_read_update_test,
             physics_concurrency_test,
    physics_snapshot_test
].

%% @doc The following function tests that ClockSI can run a non-interactive tx
%%      that updates multiple partitions.


physics_read_nonupdated_key_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    
    FirstNode = hd(Nodes),
    Key1=physics_test_key,
    Type=antidote_crdt_lwwreg,
    BoundObject = {Key1, Type, bucket},
    {ok, TxId} = rpc:call(FirstNode, antidote, start_transaction, [ignore, []]),
    {ok, ReadResult} = rpc:call(FirstNode, antidote, read_objects, [[BoundObject], TxId]),
    {ok, _CT} = rpc:call(FirstNode, antidote, commit_transaction, [TxId]),
    ?assertMatch([<<>>], ReadResult),
    pass.

physics_test1(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    
    FirstNode = hd(Nodes),
    lager:info("Test1 started"),
    Key1=physics_test1_key1,
    Key2=physics_test1_key2,
    Type = antidote_crdt_counter,
    %% Empty transaction works,
    lager:info("Running empty transaction"),
    Result0=rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[]]),
    ?assertMatch({ok, _}, Result0),
    lager:info("Done"),
    
    
    % A simple read returns empty
    lager:info("check that A simple read returns empty"),
    Result11=rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [
            [{read, {Key1, Type}}]]),
    ?assertMatch({ok, _}, Result11),
    {ok, {_, ReadSet11, _}}=Result11,
    ?assertMatch([0], ReadSet11),
    
    lager:info("done"),
    
    lager:info("check that you read what you wrote in a transaction"),
    %% Read what you wrote
    Result2=rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [
            [{read, {Key1, Type}},
                {update, {Key1, Type, {increment, 1}}},
                {update, {Key2, Type, {increment, 1}}},
                {read, {Key1, Type}}]]),
    lager:info("executed transaction"),
    ?assertMatch({ok, _}, Result2),
    {ok, {_, ReadSet2, _}}=Result2,
    ?assertMatch([0,0], ReadSet2),
    
    lager:info("ok..."),
    
    lager:info("now read in a subsequent transaction"),
    Result3=rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [
            [{read, {Key1, Type}},
                {read, {Key2, Type}}]]),
    ?assertMatch({ok, _}, Result3),
    {ok, {_, ReadSet3, _}}=Result3,
    ?assertEqual([1,1], ReadSet3),
    
    lager:info("done"),
    
    lager:info(" Multiple updates to a key in a transaction works ?"),
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
    lager:info("done"),
    pass.

%% @doc The following function tests that ClockSI can run an interactive tx.
%%      that updates multiple partitions.
physics_test2(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Type = antidote_crdt_counter,
    Key1=physics_test2_key1,
    Key2=physics_test2_key2,
    Key3=physics_test2_key3,
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
physics_test3(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Key1=physics_test3_key1,
    Key2=physics_test3_key2,
    Key3=physics_test3_key3,
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
    lager:info("got causal snapshot ~p",[CausalSnapshot]),
    
    ReadResult3 = rpc:call(FirstNode, antidote, clocksi_read,
        [CausalSnapshot, Key1, Type]),
    {ok, {_,[ReadVal],_}} = ReadResult3,
    ?assertEqual(ReadVal, 1),
    lager:info("Test3 passed"),
    pass.

%% @doc This test makes sure NOT TO block pending reads when a prepare is in progress
%% that could violate atomicity if not blocked
physics_test_prepare(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test prepare started"),
    Type = antidote_crdt_counter,
    
    Key1=physics_test_prepare_key1,
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
    
%%    timer:sleep(3000),
    
    {ok,TxIdRead}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    
%%    timer:sleep(3000),
    
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
    ?assertEqual({ok, 0}, ReadResultR),
    
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
physics_test5(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Type = antidote_crdt_counter,
    Key1=physics_test5_key1,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Started Tx. got id: ~p",[TxId]),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
        [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 0}, ReadResult0),
    
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
        [TxId, Key1, Type, increment]),
    ?assertEqual(ok, WriteResult),
    lager:info("wrote key  ok. "),
    
    ReadResult=rpc:call(FirstNode, antidote, clocksi_iread,
        [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 1}, ReadResult),
    lager:info("Read key ~p, got value: ~p ",[Key1, ReadResult]),
    
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
        [TxId, Key1, Type, increment]),
    ?assertEqual(ok, WriteResult1),
    lager:info("wrote key , ok. "),
    
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
        [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 2}, ReadResult1),
    lager:info("Read key ~p, got value: ~p ",[Key1, ReadResult1]),
    WriteResult2=rpc:call(FirstNode, antidote, clocksi_iupdate,
        [TxId, Key1, Type, {increment, 1}]),
    ?assertEqual(ok, WriteResult2),
    ReadResult2=rpc:call(FirstNode, antidote, clocksi_iread,
        [TxId, Key1, antidote_crdt_counter]),
    ?assertEqual({ok, 3}, ReadResult2),
    lager:info("Read key ~p, got value: ~p ",[Key1, ReadResult2]),
    End=rpc:call(FirstNode, antidote, clocksi_full_icommit, [TxId]),
    lager:info("Running clocksi_full_icommit "),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End),
    lager:info("Got response: ~p ",[End]),
    {ok,{_Txid, CausalSnapshot}} = End,
    
    
    lager:info("Reading causally after previous txn. "),
    
    ReadResult3 = rpc:call(FirstNode, antidote, clocksi_read,
        [CausalSnapshot, Key1, Type]),
    {ok, {_,[ReadVal],_}} = ReadResult3,
    lager:info("Read key ~p, got value: ~p ",[Key1, ReadResult3]),
    ?assertEqual(ReadVal, 3),
    lager:info("Test5 passed"),
    pass.


%% @doc Test to execute transaction with out explicit clock time
physics_tx_noclock_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    Key = physics_tx_noclock_test_key1,
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
    
    {ok,TxId1}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
        [TxId1, Key, Type, increment]),
    ?assertEqual(ok, WriteResult1),
    End1=rpc:call(FirstNode, antidote, commit_transaction, [TxId1]),
    ?assertMatch({ok, _}, End1),
    
    
    ReadResult2 = rpc:call(FirstNode, antidote, clocksi_read,
        [Key, antidote_crdt_counter]),
    {ok, {_, ReadSet2, _}}= ReadResult2,
    ?assertMatch([2], ReadSet2),
    lager:info("physics_tx_noclock_test passed"),
    pass.

%% @doc The following function tests that ClockSI can run both a single
%%      read and a bulk-update tx.
physics_single_key_update_read_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("Test3 started"),
    FirstNode = hd(Nodes),
    Key = physics_single_key_update_read_test_key1,
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
    lager:info("physics_single_key_update_read_test passed"),
    pass.

%% @doc Verify that multiple reads/writes are successful.
physics_multiple_key_update_read_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    Type = antidote_crdt_counter,
    Key1 = physics_multiple_key_update_read_test_key1,
    Key2 = physics_multiple_key_update_read_test_key2,
    Key3 = physics_multiple_key_update_read_test_key3,
    
    Objs = [{Key1, Type, bucket}, {Key2, Type, bucket}, {Key3, Type, bucket}],
    
    Ops = [{{Key1, Type, bucket}, {increment,1}},
        {{Key2, Type, bucket}, {increment,10}},
        {{Key3, Type, bucket}, increment, 1}
    ],
    {ok,TxId1}=rpc:call(FirstNode, antidote, start_transaction, [ignore, []]),
    ok = rpc:call(FirstNode, antidote, update_objects,
        [Ops, TxId1]),
    {ok, CommitTime}=rpc:call(FirstNode, antidote, commit_transaction, [TxId1]),
    
    
    {ok,TxId2}=rpc:call(FirstNode, antidote, start_transaction, [CommitTime, []]),
    {ok, FinalReadSet} = rpc:call(FirstNode, antidote, read_objects, [Objs, TxId2]),
    {ok, _CT2}=rpc:call(FirstNode, antidote, commit_transaction, [TxId2]),
    
    ?assertMatch(FinalReadSet,[1,10,1]),
    pass.

%% @doc The following function tests that ClockSI can excute a
%%      read-only interactive tx.
physics_test4(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("Test4 started"),
    FirstNode = hd(Nodes),
    Key1 = physics_test4_key1,
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

%% @doc The following function tests that Physics does not waits, when reading,
%%      for a tx that has updated an element that it wants to read and
%%      has a smaller TxId, but has not yet committed.
physics_test_read_time(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Start a new tx,  perform an update over key abc, and send prepare.
    lager:info("Test read_time started"),
    Key1 = physics_test_read_time_key1,
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
physics_test_read_wait(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    
    Key1 = physics_test_read_wait_key1,
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
            ?assertMatch({ok, 0}, ReadResult1)
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
physics_test_certification_check(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    case rpc:call(hd(Nodes), application, get_env, [antidote, txn_cert]) of
        {ok, true} ->
            physics_test_certification_check_run(Nodes);
        _ ->
            pass
    end.

physics_test_certification_check_run(Nodes) ->
    lager:info("physics_test_certification_check started"),
    Key1 = physics_test_certification_check_key1,
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
physics_multiple_test_certification_check(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    case rpc:call(hd(Nodes), application, get_env, [antidote, txn_cert]) of
        {ok, true} ->
            physics_multiple_test_certification_check_run(Nodes);
        _ ->
            pass
    end.

physics_multiple_test_certification_check_run(Nodes) ->
    lager:info("physics_multiple_test_certification_check started"),
    
    Key1 = physics_multiple_test_certification_check_key1,
    Key2 = physics_multiple_test_certification_check_key2,
    Key3 = physics_multiple_test_certification_check_key3,
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
physics_multiple_read_update_test(Config) ->
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
    {ok,_} = rpc:call(Node, antidote, append,
        [Key, Type, {increment,1}]),
    {ok,Result2} = rpc:call(Node, antidote, read,
        [Key, Type]),
    ?assertEqual(Result1+1,Result2),
    pass.

get_random_key() ->
    random:seed(now()),
    random:uniform(1000).

%% @doc The following function tests how two concurrent transactions work
%%      when they are interleaved.
%% in this case, PhysiCS performs different from ClockSI, as it does not wait for prepared
%% transactions to commit. Therefore, when Tx2 wants to commit, it can't as Tx1 has committed,
%% which generates a write-write conflict.
physics_concurrency_test(Config) ->
	Nodes = proplists:get_value(nodes, Config),
	lager:info("physics_concurrency_test started"),
	Node = hd(Nodes),
	%% read txn starts before the write txn's prepare phase,
	Key = physics_conc,
	Type = antidote_crdt_counter,
	Bucket = physics_test,
	Bound_object = {Key, Type, Bucket},
	
	{ok, TxId1} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
	ok = rpc:call(Node, antidote, update_objects, [[{Bound_object, {increment, 1}}], TxId1]),
	rpc:call(Node, antidote, clocksi_iprepare, [TxId1]),
	
	{ok, TxId2} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
	
	
	Pid = self(),
	spawn( fun() ->
		ok = rpc:call(Node, antidote, update_objects, [[{Bound_object, {increment, 1}}], TxId2]),
        CommitResult = rpc:call(Node, antidote, commit_transaction, [TxId2]),
        ?assertMatch({error,{aborted,TxId2}}, CommitResult),
		Pid ! ok
	end),
	
	{ok,_}= rpc:call(Node, antidote, clocksi_icommit, [TxId1]),
	receive
		ok ->
			{ok, TxId3} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
			Res = rpc:call(Node, antidote, read_objects, [[Bound_object], TxId3]),
			rpc:call(Node, antidote, commit_transaction, [TxId3]),
			?assertMatch({ok, [1]}, Res),
			pass
	end.

%% this test shows how a long fork, forward freshness, and causal snapshots work.
physics_snapshot_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("physics_snapshot_test started"),
    Node = hd(Nodes),
    %% read txn starts before the write txn's prepare phase,
    Key = physics_snapshot_test,
    Key2 = physics_snapshot_test2,
    Type1 = antidote_crdt_counter,
    Bucket1 = physics_test,
    
    Bound_object1= {Key, Type1, Bucket1},
    Bound_object2 = {Key2, Type1, Bucket1},
    
    
    %% first, create initial versions of the two objects.
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    ok = rpc:call(Node, antidote, update_objects, [[{Bound_object1, {increment, 1}}, {Bound_object2, {increment, 1}}], TxId]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId]),
    
    %% now, start two transactions, one reading each of created versions.
    {ok, TxId1} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    
    Res1 = rpc:call(Node, antidote, read_objects, [[Bound_object1], TxId1]),
    Res1 = rpc:call(Node, antidote, read_objects, [[Bound_object2], TxId2]),
    
    ?assertMatch({ok, [1]}, Res1),
    
    %% now, a transaction creates new versions of the objects, which should depend on the previous transaction.
    
    {ok, TxId3} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [1,1]} = rpc:call(Node, antidote, read_objects, [[Bound_object1, Bound_object2], TxId3]),
    ok = rpc:call(Node, antidote, update_objects, [[{Bound_object1, {increment, 1}}, {Bound_object2, {increment, 1}}], TxId3]),
    rpc:call(Node, antidote, commit_transaction, [TxId3]),
    
    {ok, TxId4} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [2,2]} = rpc:call(Node, antidote, read_objects, [[Bound_object1, Bound_object2], TxId4]),
    ok = rpc:call(Node, antidote, update_objects, [[{Bound_object1, {increment, 1}}, {Bound_object2, {increment, 1}}], TxId4]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId4]),
    
    {ok, TxId5} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [3,3]} = rpc:call(Node, antidote, read_objects, [[Bound_object1, Bound_object2], TxId5]),
    ok = rpc:call(Node, antidote, update_objects, [[{Bound_object1, {increment, 1}}, {Bound_object2, {increment, 1}}], TxId5]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId5]),
    
    %% now, the reading transactions want to read the object they haven't read yet.
    
    Res2 = rpc:call(Node, antidote, read_objects, [[Bound_object2], TxId1]),
    Res2 = rpc:call(Node, antidote, read_objects, [[Bound_object1], TxId2]),
    
    %% just finish the two transactions.
    
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId1]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId2]),
    
    %% check that the result is 2, because
    ?assertMatch({ok, [2]}, Res2),
    
    %% just verify that all updates were applied.
    {ok, TxId6} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, FinalResult} = rpc:call(Node, antidote, read_objects, [[Bound_object1, Bound_object2], TxId6]),
    ?assertMatch([4,4], FinalResult),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId6]),
    pass.
    
    

