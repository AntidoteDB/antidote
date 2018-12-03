%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(clocksi_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0
]).

%% tests
-export([
    clocksi_test1/1,
    clocksi_test2/1,
    clocksi_test4/1,
    clocksi_test5/1,
    clocksi_multiple_updates_per_txn_test/1,
    clocksi_read_write_write_txn_test/1,
    clocksi_test_read_wait/1,
    clocksi_single_key_update_read_test/1,
    clocksi_multiple_key_update_read_test/1,
    clocksi_test_no_update_property/1,
    clocksi_multiple_read_update_test/1,
    clocksi_concurrency_test/1,
    clocksi_parallel_ops_test/1,
    clocksi_static_parallel_writes_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(clocksi_bucket)).

init_per_suite(Config) ->
    test_utils:init_single_dc(?MODULE, Config).

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    clocksi_test1,
    clocksi_test2,
    clocksi_test4,
    clocksi_test5,
    clocksi_multiple_updates_per_txn_test,
    clocksi_test_read_wait,
    clocksi_single_key_update_read_test,
    clocksi_multiple_key_update_read_test,
    clocksi_test_no_update_property,
    clocksi_multiple_read_update_test,
    clocksi_concurrency_test,
    clocksi_parallel_ops_test,
    clocksi_static_parallel_writes_test
].


clocksi_test1(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key1 = clocksi_test1_key1,
    Key2 = clocksi_test1_key2,

    %% Empty transaction works,
    antidote_utils:update_counters(Node, [], [], ignore, static, Bucket),
    antidote_utils:update_counters(Node, [], [], ignore, static, Bucket),

    %% A read before an update returns empty
    antidote_utils:check_read_key(Node, Key1, antidote_crdt_counter_pn, 0, ignore, static, Bucket),

    %% Read what you wrote
    antidote_utils:update_counters(Node, [Key1, Key2], [1, 1], ignore, static, Bucket),
    antidote_utils:check_read_keys(Node, [Key1, Key2], antidote_crdt_counter_pn, [1, 1], ignore, static, Bucket),

    %% Multiple updates to a key in a transaction works
    antidote_utils:update_counters(Node, [Key1, Key1], [1, 1], ignore, static, Bucket),
    antidote_utils:check_read_key(Node, Key1, antidote_crdt_counter_pn, 3, ignore, static, Bucket),
    pass.


%% @doc The following function tests that ClockSI can run an interactive tx.
clocksi_test2(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key1=clocksi_test2_key1,
    Key2=clocksi_test2_key2,
    Key3=clocksi_test2_key3,

    {ok, TxId} = rpc:call(Node, cure, start_transaction, [ignore, []]),
    antidote_utils:check_read_key(Node, Key1, antidote_crdt_counter_pn, 0, ignore, TxId, Bucket),

    antidote_utils:update_counters(Node, [Key1], [1], ignore, TxId, Bucket),
    antidote_utils:check_read_key(Node, Key1, antidote_crdt_counter_pn, 1, ignore, TxId, Bucket),

    antidote_utils:update_counters(Node, [Key2], [1], ignore, TxId, Bucket),
    antidote_utils:check_read_key(Node, Key2, antidote_crdt_counter_pn, 1, ignore, TxId, Bucket),

    antidote_utils:update_counters(Node, [Key3], [1], ignore, TxId, Bucket),
    antidote_utils:check_read_key(Node, Key3, antidote_crdt_counter_pn, 1, ignore, TxId, Bucket),

    {ok, CausalSnapshot} = rpc:call(Node, cure, commit_transaction, [TxId]),
    antidote_utils:check_read_key(Node, Key1, antidote_crdt_counter_pn, 1, CausalSnapshot, static, Bucket),
    pass.


%% @doc The following function tests that ClockSI can execute a
%%      read-only interactive tx.
clocksi_test4(Config) ->
    Bucket = ?BUCKET,
    FirstNode = proplists:get_value(node, Config),
    Key = clocksi_test4_key1,

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    antidote_utils:check_read_key(FirstNode, Key, antidote_crdt_counter_pn, 0, ignore, TxId, Bucket),

    {ok, _CT} = rpc:call(FirstNode, cure, commit_transaction, [TxId]),
    pass.


%% @doc The following function tests that ClockSI can run an interactive tx.
%%      that updates only one partition. This type of txs use an only-one phase
%%      commit.
clocksi_test5(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = clocksi_test5_key1,

    {ok, TxId} = rpc:call(Node, cure, start_transaction, [ignore, []]),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_set_aw, [], ignore, TxId, Bucket),

    antidote_utils:update_sets(Node, [Key], [{add, a}], TxId, Bucket),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_set_aw, [a], ignore, TxId, Bucket),

    antidote_utils:update_sets(Node, [Key], [{add, b}], TxId, Bucket),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_set_aw, [a, b], ignore, TxId, Bucket),

    antidote_utils:update_sets(Node, [Key], [{remove, a}], TxId, Bucket),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_set_aw, [b], ignore, TxId, Bucket),

    {ok, CausalSnapshot} = rpc:call(Node, cure, commit_transaction, [TxId]),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_set_aw, [b], CausalSnapshot, static, Bucket),
    pass.


%% @doc The following function tests an interactive tx.
%%      that executes multiple updates on same key and check the reads include
%%      updates in correct order.
clocksi_multiple_updates_per_txn_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = clocksi_multiple_updates_per_txn_key1,
    BoundObj = {Key, antidote_crdt_register_mv, Bucket},

    {ok, TxId} = rpc:call(Node, cure, start_transaction, [ignore, []]),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_register_mv, [], ignore, TxId, Bucket),

    ok = rpc:call(Node, cure, update_objects, [[{BoundObj, assign, <<"a">>}], TxId]),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_register_mv, [<<"a">>], ignore, TxId, Bucket),

    ok = rpc:call(Node, cure, update_objects, [[{BoundObj, assign, <<"b">>}], TxId]),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_register_mv, [<<"b">>], ignore, TxId, Bucket),

    ok = rpc:call(Node, cure, update_objects, [[{BoundObj, assign, <<"c">>}], TxId]),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_register_mv, [<<"c">>], ignore, TxId, Bucket),

    {ok, CausalSnapshot} = rpc:call(Node, cure, commit_transaction, [TxId]),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_register_mv, [<<"c">>], CausalSnapshot, static, Bucket),
    pass.

clocksi_read_write_write_txn_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    Key1=clocksi_read_write_write_txn_key1,
    BoundObj = {Key1, antidote_crdt_register_mv, ?BUCKET},

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    antidote_utils:check_read_key(FirstNode, Key1, antidote_crdt_register_mv, [], ignore, TxId),

    ok = rpc:call(FirstNode, cure, update_objects, [[{BoundObj, assign, <<"a">>}], TxId]),
    ok = rpc:call(FirstNode, cure, update_objects, [[{BoundObj, assign, <<"b">>}], TxId]),
    ok = rpc:call(FirstNode, cure, update_objects, [[{BoundObj, assign, <<"c">>}], TxId]),

    antidote_utils:check_read_key(FirstNode, Key1, antidote_crdt_register_mv, [<<"c">>], ignore, TxId),

    End = rpc:call(FirstNode, cure, commit_transaction, [TxId]),
    ?assertMatch({ok, _CausalSnapshot}, End),
    {ok, CausalSnapshot} = End,
    antidote_utils:check_read_key(FirstNode, Key1, antidote_crdt_register_mv, [<<"c">>], CausalSnapshot, static),

    pass.

%% @doc The following function tests that ClockSI can run both a single
%%      read and a bulk-update tx.
clocksi_single_key_update_read_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = clocksi_single_key_update_read_test_key1,

    {ok, CommitTime} = antidote_utils:update_counters(Node, [Key, Key], [1, 1], ignore, static, Bucket),
    antidote_utils:check_read_key(Node, Key, antidote_crdt_counter_pn, 2, CommitTime, static, Bucket),
    pass.


%% @doc Verify that multiple reads/writes are successful.
clocksi_multiple_key_update_read_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key1 = clocksi_multiple_key_update_read_test_key1,
    Key2 = clocksi_multiple_key_update_read_test_key2,
    Key3 = clocksi_multiple_key_update_read_test_key3,

    {ok, CommitTime} = antidote_utils:update_counters(Node, [Key1, Key2, Key3], [1, 10, 1], ignore, static, Bucket),

    antidote_utils:check_read_key(Node, Key1, antidote_crdt_counter_pn, 1, CommitTime, static, Bucket),
    antidote_utils:check_read_key(Node, Key2, antidote_crdt_counter_pn, 10, CommitTime, static, Bucket),
    antidote_utils:check_read_key(Node, Key3, antidote_crdt_counter_pn, 1, CommitTime, static, Bucket),
    pass.



%% @doc The following function tests that ClockSI does not read values
%%      inserted by a tx with higher commit timestamp than the snapshot time
%%      of the reading tx.
clocksi_test_read_wait(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = clocksi_test_read_wait_key1,
    Type = antidote_crdt_counter_pn,

    %% Start a new tx, update a key read_wait_test, and send prepare.
    {ok, TxId1} = rpc:call(Node, cure, start_transaction, [ignore, []]),
    antidote_utils:update_counters(Node, [Key], [1], ignore, TxId1, Bucket),

    {ok, CommitTime1} = rpc:call(Node, cure, clocksi_iprepare, [TxId1]),
    ct:log("Tx1 sent prepare, assigned commitTime : ~p", [CommitTime1]),

    %% Start a different tx and try to read the key
    {ok, TxId2} = rpc:call(Node, cure, start_transaction, [ignore, []]),
    ct:log("Tx2 started with id : ~p", [TxId2]),
    Pid = spawn(antidote_utils, spawn_read, [Node, TxId2, self(), Key, Type, Bucket]),

    %% Delay first transaction
    timer:sleep(500),

    %% Commit the first tx.
    {ok, _} = rpc:call(Node, cure, clocksi_icommit, [TxId1]),
    ct:log("Tx1 committed."),

    receive
        {Pid, ReadResult1} ->
            %%receive the read value
            ?assertMatch({ok, 1}, ReadResult1)
    end,

    %% Prepare and commit the second transaction.
    _CommitTime2 = rpc:call(Node, cure, commit_transaction, [TxId2]),
    ct:log("Tx2 committed."),
    pass.


clocksi_test_no_update_property(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = clockSI_test_no_update_property_key1,
    Type = antidote_crdt_counter_pn,

    {ok, _} = rpc:call(Node, antidote, update_objects,
                      [ignore, [], [{{Key, Type, Bucket}, increment, 1}]]),

    pass.


%% @doc Read an update a key multiple times.
clocksi_multiple_read_update_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = antidote_utils:get_random_key(),
    Type = antidote_crdt_counter_pn,
    NTimes = 100,
    Obj = {Key, Type, Bucket},

    {ok, [Result1], _} = rpc:call(Node, cure, read_objects, [ignore, [], [Obj]]),
    lists:foreach(fun(_)->
                          read_update_run(Node, Key, Bucket) end,
                  lists:seq(1, NTimes)),
    {ok, [Result2], _} = rpc:call(Node, cure, read_objects, [ignore, [], [Obj]]),

    ?assertEqual(Result1+NTimes, Result2),
    pass.

%% @doc Test updating prior to a read.
read_update_run(Node, Key, Bucket) ->
    Type = antidote_crdt_counter_pn,
    Obj = {Key, Type, Bucket},

    {ok, [Result1], _} = rpc:call(Node, cure, read_objects, [ignore, [], [Obj]]),
    antidote_utils:update_counters(Node, [Key], [1], ignore, static, Bucket),
    {ok, [Result2], _} = rpc:call(Node, cure, read_objects, [ignore, [], [Obj]]),
    ?assertEqual(Result1+1, Result2),
    pass.


%% @doc The following function tests how two concurrent transactions work
%%      when they are interleaved.
clocksi_concurrency_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = clocksi_conc,
    Type = antidote_crdt_counter_pn,

    %% read txn starts before the write txn's prepare phase,
    {ok, TxId1} = rpc:call(Node, cure, start_transaction, [ignore, []]),
    antidote_utils:update_counters(Node, [Key], [1], ignore, TxId1, Bucket),
    rpc:call(Node, cure, clocksi_iprepare, [TxId1]),
    {ok, TxId2} = rpc:call(Node, cure, start_transaction, [ignore, []]),
    Pid = self(),
    spawn( fun() ->
                   antidote_utils:update_counters(Node, [Key], [1], ignore, TxId2, Bucket),
                   rpc:call(Node, cure, clocksi_iprepare, [TxId2]),
                   {ok, _}= rpc:call(Node, cure, clocksi_icommit, [TxId2]),
                   Pid ! ok
           end),

    {ok, _}= rpc:call(Node, cure, clocksi_icommit, [TxId1]),
    receive
        ok ->
            antidote_utils:check_read_key(Node, Key, Type, 2, ignore, static, Bucket),
            pass
    end.


%% @doc The following test checks multiple updates/reads using
%% read/update_objects. It also checks that reads are returned
%% in the same order they were sent.
clocksi_parallel_ops_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    BoundObject1 = {parallel_key1, antidote_crdt_counter_pn, Bucket},
    BoundObject2 = {parallel_key2, antidote_crdt_counter_pn, Bucket},
    BoundObject3 = {parallel_key3, antidote_crdt_counter_pn, Bucket},
    BoundObject4 = {parallel_key4, antidote_crdt_counter_pn, Bucket},
    BoundObject5 = {parallel_key5, antidote_crdt_counter_pn, Bucket},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),

    %% update 5 different objects
    ok = rpc:call(Node, antidote, update_objects,
                  [[{BoundObject1, increment, 1},
                    {BoundObject2, increment, 2},
                    {BoundObject3, increment, 3},
                    {BoundObject4, increment, 4},
                    {BoundObject5, increment, 5}],
                   TxId]),

    %% read the objects in the same transaction to see that the updates
    %% are seen.
    Res = rpc:call(Node, antidote, read_objects, [[BoundObject1,
                                                   BoundObject2, BoundObject3, BoundObject4, BoundObject5], TxId]),
    ?assertMatch({ok, [1, 2, 3, 4, 5]}, Res),

    %% update 5 times the first object.
    ok = rpc:call(Node, antidote, update_objects,
                  [[{BoundObject1, increment, 1},
                    {BoundObject1, increment, 1},
                    {BoundObject1, increment, 1},
                    {BoundObject1, increment, 1},
                    {BoundObject1, increment, 1}],
                   TxId]),
    %% see that these updates are seen too.
    Res1 = rpc:call(Node, antidote, read_objects, [[BoundObject1], TxId]),
    ?assertMatch({ok, [6]}, Res1),
    {ok, _CT} = rpc:call(Node, antidote, commit_transaction, [TxId]),

    %% start a new transaction that reads the updated objects.
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    Res2 = rpc:call(Node, antidote, read_objects, [[BoundObject1,
                                                    BoundObject2, BoundObject3, BoundObject4, BoundObject5], TxId2]),
    ?assertMatch({ok, [6, 2, 3, 4, 5]}, Res2),
    {ok, _CT2} = rpc:call(Node, antidote, commit_transaction, [TxId2]).


%% @doc The following test tests sending multiple updates in a single
%% update_objects call.
%% it also tests that the coordinator StaysAlive after the first transaction,
%% and serves the request from the second one.
clocksi_static_parallel_writes_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    BoundObject1 = {parallel_key6, antidote_crdt_counter_pn, Bucket},
    BoundObject2 = {parallel_key7, antidote_crdt_counter_pn, Bucket},
    BoundObject3 = {parallel_key8, antidote_crdt_counter_pn, Bucket},
    BoundObject4 = {parallel_key9, antidote_crdt_counter_pn, Bucket},
    BoundObject5 = {parallel_key10, antidote_crdt_counter_pn, Bucket},
    %% update 5 different objects
    {ok, CT} = rpc:call(Node, cure, update_objects,
                        [ignore, [],
                         [{BoundObject1, increment, 1},
                          {BoundObject2, increment, 2},
                          {BoundObject3, increment, 3},
                          {BoundObject4, increment, 4},
                          {BoundObject5, increment, 5}]
                        , true
                        ]),

    ct:log("updated 5 objects no problem"),

    {ok, Res, CT1} = rpc:call(Node, cure, obtain_objects,
                              [CT, [], [BoundObject1,
                                        BoundObject2, BoundObject3,
                                        BoundObject4, BoundObject5],
                              true, object_value
                              ]),
    ?assertMatch([1, 2, 3, 4, 5], Res),

    ct:log("read 5 objects no problem"),

    %% update 5 times the first object.
    {ok, CT2} = rpc:call(Node, cure, update_objects,
                         [CT1, [],
                          [{BoundObject1, increment, 1},
                           {BoundObject1, increment, 1},
                           {BoundObject1, increment, 1},
                           {BoundObject1, increment, 1},
                           {BoundObject1, increment, 1}]
                         , true
                         ]),

    ct:log("updated 5 objects concurrently"),

    {ok, Res1, _CT4} = rpc:call(Node, cure, obtain_objects,
                                [CT2, [], [BoundObject1], true, object_value]),
    ?assertMatch([6], Res1),
    pass.
