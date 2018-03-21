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
         clocksi_test5/1,
         clocksi_multiple_updates_per_txn_test/1,
         clocksi_test_read_wait/1,
         clocksi_test4/1,
         clocksi_test_read_time/1,
         clocksi_test_prepare/1,
         clocksi_single_key_update_read_test/1,
         clocksi_multiple_key_update_read_test/1,
         clocksi_test_cert_property/1,
         clocksi_test_no_update_property/1,
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

-define(BUCKET, "clocksi_bucket").

init_per_suite(Config) ->
    lager_common_test_backend:bounce(debug),
    test_utils:at_init_testsuite(),

    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = hd(Clusters),
    %% This test needs only one cluster
    %% Nodes = test_utils:pmap(fun(N) ->
    %%                 test_utils:start_suite(N, Config)
    %%         end, [dev1, dev2, dev3]),
    %% ok = test_utils:join_cluster(Nodes),
    %% Check that the clocksi protocol is tested
    {ok, Prot} = rpc:call(hd(Nodes), application, get_env, [antidote, txn_prot]),
    ?assertMatch(clocksi, Prot),

    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [clocksi_test1,
         clocksi_test2,
         clocksi_test5,
         clocksi_multiple_updates_per_txn_test,
         clocksi_test_read_wait,
         clocksi_test4,
         clocksi_test_read_time,
         clocksi_test_prepare,
         clocksi_single_key_update_read_test,
         clocksi_multiple_key_update_read_test,
         clocksi_test_cert_property,
         clocksi_test_no_update_property,
         clocksi_test_certification_check,
         clocksi_multiple_test_certification_check,
         clocksi_multiple_read_update_test,
         clocksi_concurrency_test,
         clocksi_parallel_ops_test,
         clocksi_static_parallel_writes_test].

check_read_key(Node, Key, Type, Expected, Clock, TxId) ->
    check_read(Node, [{Key, Type, ?BUCKET}], [Expected], Clock, TxId).

check_read_keys(Node, Keys, Type, Expected, Clock, TxId) ->
    Objects = lists:map(fun(Key) ->
                                {Key, Type, ?BUCKET}
                        end,
                        Keys
                       ),
    check_read(Node, Objects, Expected, Clock, TxId).

check_read(Node, Objects, Expected, Clock, TxId) ->
    case TxId of
        static ->
            {ok, Res, CT} = rpc:call(Node, cure, read_objects, [Clock, [], Objects]),
            ?assertEqual(Expected, Res),
            {ok, Res, CT};
        _ ->
            {ok, Res} = rpc:call(Node, cure, read_objects, [Objects, TxId]),
            ?assertEqual(Expected, Res),
            {ok, Res}
    end.

update_counters(Node, Keys, IncValues, Clock, TxId) ->
    Updates = lists:map(fun({Key, Inc}) ->
                                {{Key, antidote_crdt_counter_pn, ?BUCKET}, increment, Inc}
                        end,
                        lists:zip(Keys, IncValues)
                       ),

    case TxId of
        static ->
            {ok, CT} = rpc:call(Node, cure, update_objects, [Clock, [], Updates]),
            {ok, CT};
        _->
            ok = rpc:call(Node, cure, update_objects, [Updates, TxId]),
            ok
    end.

update_sets(Node, Keys, Ops, TxId) ->
    Updates = lists:map(fun({Key, {Op, Param}}) ->
                                {{Key, antidote_crdt_set_aw, ?BUCKET}, Op, Param}
                        end,
                        lists:zip(Keys, Ops)
                       ),
    ok = rpc:call(Node, antidote, update_objects, [Updates, TxId]),
    ok.

%% @doc The following function tests that ClockSI can run a non-interactive tx
%%      that updates multiple partitions.
clocksi_test1(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test1 started"),

    Key1=clocksi_test1_key1,
    Key2=clocksi_test1_key2,
    %% Empty transaction works,
    update_counters(FirstNode, [], [], ignore, static),
    update_counters(FirstNode, [], [], ignore, static),

    %% A read before an update returns empty
    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 0, ignore, static),

    %% Read what you wrote
    update_counters(FirstNode, [Key1, Key2], [1, 1], ignore, static),
    check_read_keys(FirstNode, [Key1, Key2], antidote_crdt_counter_pn, [1, 1], ignore, static),

    %% Multiple updates to a key in a transaction works
    update_counters(FirstNode, [Key1, Key1], [1, 1], ignore, static),
    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 3, ignore, static),
    pass.

%% @doc The following function tests that ClockSI can run an interactive tx.
%%      that updates multiple partitions.
clocksi_test2(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),

    Key1=clocksi_test2_key1,
    Key2=clocksi_test2_key2,
    Key3=clocksi_test2_key3,
    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 0, ignore, TxId),

    update_counters(FirstNode, [Key1], [1], ignore, TxId),
    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 1, ignore, TxId),

    update_counters(FirstNode, [Key2], [1], ignore, TxId),
    check_read_key(FirstNode, Key2, antidote_crdt_counter_pn, 1, ignore, TxId),

    update_counters(FirstNode, [Key3], [1], ignore, TxId),
    check_read_key(FirstNode, Key3, antidote_crdt_counter_pn, 1, ignore, TxId),

    CommitResult = rpc:call(FirstNode, cure, commit_transaction, [TxId]),
    ?assertMatch({ok, _}, CommitResult),
    {ok, CausalSnapshot} = CommitResult,

    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 1, CausalSnapshot, static),
    lager:info("Test2 passed"),
    pass.

%% @doc This test makes sure to block pending reads when a prepare is in progress
%% that could violate atomicity if not blocked
clocksi_test_prepare(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test prepare started"),

    Key1=clocksi_test_prepare_key1,
    Preflist = rpc:call(FirstNode, log_utilities, get_preflist_from_key, [aaa]),
    IndexNode = hd(Preflist),

    Key2 = find_key_same_node(FirstNode, IndexNode, 1),

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 0, ignore, TxId),

    update_counters(FirstNode, [Key1], [1], ignore, TxId),
    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 1, ignore, TxId),

    %% Start 2 phase commit, but do not commit
    CommitTime=rpc:call(FirstNode, cure, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),

    timer:sleep(3000),

    {ok, TxIdRead} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),

    timer:sleep(3000),
    %% start another transaction that updates the same partition
    {ok, TxId1 } = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    update_counters(FirstNode, [Key2], [1], ignore, TxId1),
    check_read_key(FirstNode, Key2, antidote_crdt_counter_pn, 1, ignore, TxId1),

    %% Start 2 phase commit for second transaction, but do not commit.
    CommitTime1 = rpc:call(FirstNode, cure, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),

    %% Commit transaction after a delay, 1 in parallel,
    spawn(?MODULE, spawn_com, [FirstNode, TxId]),

    %% Read should return the value committed by transaction 1. But should not be
    %% blocked by the transaction 2 which is in commit phase, because it updates a different key.
    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 1, ignore, TxIdRead),

    End1 = rpc:call(FirstNode, cure, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End1),

    lager:info("Test prepare passed"),
    pass.

find_key_same_node(FirstNode, IndexNode, Num) ->
    NewKey = list_to_atom(atom_to_list(aaa) ++ integer_to_list(Num)),
    Preflist = rpc:call(FirstNode, log_utilities, get_preflist_from_key, [aaa]),
    case hd(Preflist) == IndexNode of
        true ->
            NewKey;
        false ->
            find_key_same_node(FirstNode, IndexNode, Num+1)
    end.

spawn_com(FirstNode, TxId) ->
    timer:sleep(3000),
    End1 = rpc:call(FirstNode, cure, clocksi_icommit, [TxId]),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End1).


%% @doc The following function tests that ClockSI can run an interactive tx.
%%      that updates only one partition. This type of txs use an only-one phase
%%      commit.
clocksi_test5(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Key1=clocksi_test5_key1,

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    check_read_key(FirstNode, Key1, antidote_crdt_set_aw, [], ignore, TxId),

    update_sets(FirstNode, [Key1], [{add, a}], TxId),
    check_read_key(FirstNode, Key1, antidote_crdt_set_aw, [a], ignore, TxId),

    update_sets(FirstNode, [Key1], [{add, b}], TxId),
    check_read_key(FirstNode, Key1, antidote_crdt_set_aw, [a, b], ignore, TxId),

    update_sets(FirstNode, [Key1], [{remove, a}], TxId),
    check_read_key(FirstNode, Key1, antidote_crdt_set_aw, [b], ignore, TxId),

    End = rpc:call(FirstNode, cure, commit_transaction, [TxId]),
    ?assertMatch({ok, _CausalSnapshot}, End),
    {ok, CausalSnapshot} = End,
    check_read_key(FirstNode, Key1, antidote_crdt_set_aw, [b], CausalSnapshot, static),
    lager:info("Test5 passed"),
    pass.

%% @doc The following function tests an interactive tx.
%%      that executes multiple updates on same key and check the reads include
%%      updates in correct order.
clocksi_multiple_updates_per_txn_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Test6 started"),
    Key1=clocksi_multiple_updates_per_txn_key1,
    BoundObj = {Key1, antidote_crdt_register_mv, ?BUCKET},

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    check_read_key(FirstNode, Key1, antidote_crdt_register_mv, [], ignore, TxId),

    ok = rpc:call(FirstNode, cure, update_objects, [[{BoundObj, assign, <<"a">>}], TxId]),
    check_read_key(FirstNode, Key1, antidote_crdt_register_mv, [<<"a">>], ignore, TxId),

    ok = rpc:call(FirstNode, cure, update_objects, [[{BoundObj, assign, <<"b">>}], TxId]),
    check_read_key(FirstNode, Key1, antidote_crdt_register_mv, [<<"b">>], ignore, TxId),

    ok = rpc:call(FirstNode, cure, update_objects, [[{BoundObj, assign, <<"c">>}], TxId]),
    check_read_key(FirstNode, Key1, antidote_crdt_register_mv, [<<"c">>], ignore, TxId),

    End = rpc:call(FirstNode, cure, commit_transaction, [TxId]),
    ?assertMatch({ok, _CausalSnapshot}, End),
    {ok, CausalSnapshot} = End,
    check_read_key(FirstNode, Key1, antidote_crdt_register_mv, [<<"c">>], CausalSnapshot, static),

    lager:info("Test6 passed"),
    pass.

%% @doc The following function tests that ClockSI can run both a single
%%      read and a bulk-update tx.
clocksi_single_key_update_read_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("Test3 started"),
    FirstNode = hd(Nodes),
    Key = clocksi_single_key_update_read_test_key1,
    {ok, CommitTime} = update_counters(FirstNode, [Key, Key], [1, 1], ignore, static),
    check_read_key(FirstNode, Key, antidote_crdt_counter_pn, 2, CommitTime, static),

    lager:info("clocksi_single_key_update_read_test passed"),
    pass.

%% @doc Verify that multiple reads/writes are successful.
clocksi_multiple_key_update_read_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    Key1 = clocksi_multiple_key_update_read_test_key1,
    Key2 = clocksi_multiple_key_update_read_test_key2,
    Key3 = clocksi_multiple_key_update_read_test_key3,
    {ok, CommitTime} = update_counters(FirstNode, [Key1, Key2, Key3], [1, 10, 1], ignore, static),

    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 1, CommitTime, static),
    check_read_key(FirstNode, Key2, antidote_crdt_counter_pn, 10, CommitTime, static),
    check_read_key(FirstNode, Key3, antidote_crdt_counter_pn, 1, CommitTime, static),

    pass.

%% @doc The following function tests that ClockSI can excute a
%%      read-only interactive tx.
clocksi_test4(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("Test4 started"),
    FirstNode = hd(Nodes),
    Key1 = clocksi_test4_key1,
    {ok, TxId1} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 0, ignore, TxId1),

    {ok, _CT} = rpc:call(FirstNode, cure, commit_transaction, [TxId1]),
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

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    %% start a different tx and try to read key read_time.
    {ok, TxId1} = rpc:call(LastNode, cure, start_transaction, [ignore, []]),

    lager:info("Tx2 Started, id : ~p", [TxId1]),
    update_counters(FirstNode, [Key1], [1], ignore, TxId),
    CommitTime = rpc:call(FirstNode, cure, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    lager:info("Tx1 sent prepare, got commitTime=..., id : ~p", [CommitTime]),
    %% try to read key read_time.
    %% commit the first tx.
    End = rpc:call(FirstNode, cure, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),
    lager:info("Tx1 Committed."),

    lager:info("Tx2 Reading..."),
    check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 0, ignore, TxId1),

    %% prepare and commit the second transaction.
    {ok, _CommitTime} = rpc:call(FirstNode, cure, commit_transaction, [TxId1]),
    pass.

%% @doc The following function tests that ClockSI does not read values
%%      inserted by a tx with higher commit timestamp than the snapshot time
%%      of the reading tx.
clocksi_test_read_wait(Config) ->
    Nodes = proplists:get_value(nodes, Config),

    Key1 = clocksi_test_read_wait_key1,
    Type = antidote_crdt_counter_pn,
    %% Start a new tx, update a key read_wait_test, and send prepare.

    FirstNode = hd(Nodes),
    {ok, TxId1} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    update_counters(FirstNode, [Key1], [1], ignore, TxId1),

    {ok, CommitTime1} = rpc:call(FirstNode, cure, clocksi_iprepare, [TxId1]),
    lager:info("Tx1 sent prepare, assigned commitTime : ~p", [CommitTime1]),

    %% Start a different tx and try to read the key
    {ok, TxId2} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    lager:info("Tx2 started with id : ~p", [TxId2]),
    Pid = spawn(?MODULE, spawn_read, [FirstNode, TxId2, self(), Key1, Type]),

    %% Delay first transaction
    timer:sleep(2000),

    %% Commit the first tx.
    End1 = rpc:call(FirstNode, cure, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx1 committed."),

    receive
        {Pid, ReadResult1} ->
            %%receive the read value
            ?assertMatch({ok, 1}, ReadResult1)
    end,

    %% Prepare and commit the second transaction.
    _CommitTime2 = rpc:call(FirstNode, cure, commit_transaction, [TxId2]),
    lager:info("Tx2 committed."),
    pass.

spawn_read(Node, TxId, Return, Key, Type) ->
    {ok, [Res]} = check_read_key(Node, Key, Type, 1, ignore, TxId),
    Return ! {self(), {ok, Res}}.

%% @doc The following function tests the certification check algorithm,
%%      when two concurrent txs modify a single object, one hast to abort.
clocksi_test_certification_check(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    case rpc:call(hd(Nodes), application, get_env, [antidote, txn_cert]) of
        {ok, true} ->
            clocksi_test_certification_check_run(Nodes, false);
        _ ->
            pass
    end.

%% @doc The following function tests the certification check algorithm,
%%      with the property set to disable the certification
%%      when two concurrent txs modify a single object, they both must commit.
clocksi_test_cert_property(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    case rpc:call(hd(Nodes), application, get_env, [antidote, txn_cert]) of
        {ok, true} ->
            clocksi_test_certification_check_run(Nodes, true);
        _ ->
            pass
    end.

clocksi_test_certification_check_run(Nodes, DisableCert) ->
    lager:info("clockSI_test_certification_check started"),
    Key1 = clockSI_test_certification_check_key1,

    FirstNode = hd(Nodes),
    LastNode = lists:last(Nodes),
    lager:info("FirstNode: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),

    %% Start a new tx on first node, perform an update on some key.
    Properties = case DisableCert of
             true -> [{certify, dont_certify}];
             false -> []
         end,

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, Properties, false]),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    update_counters(FirstNode, [Key1], [1], ignore, TxId),

    %% Start a new tx on last node, perform an update on the same key.
    {ok, TxId1} = rpc:call(LastNode, cure, start_transaction, [ignore, []]),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    update_counters(FirstNode, [Key1], [1], ignore, TxId1),

    {ok, _CT}= rpc:call(LastNode, cure, commit_transaction, [TxId1]),

    %% Commit the first tx.
    CommitTime = rpc:call(FirstNode, cure, clocksi_iprepare, [TxId]),
    case DisableCert of
    false ->
        ?assertMatch({aborted, TxId}, CommitTime),
        lager:info("Tx1 sent prepare, got message: ~p", [CommitTime]),
        lager:info("Tx1 aborted. Test passed!");
    true ->
        ?assertMatch({ok, _}, CommitTime),
        lager:info("Tx1 sent prepare, got message: ~p", [CommitTime]),
        End2 = rpc:call(FirstNode, cure, clocksi_icommit, [TxId]),
        ?assertMatch({ok, _}, End2),
        lager:info("Tx1 committed. Test passed!")
    end,
    pass.

clocksi_test_no_update_property(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    _LastNode = lists:last(Nodes),
    Key =
    clockSI_test_no_update_property_key1,
    Bucket = bucket,
    Type = antidote_crdt_counter_pn,
    {ok, _} = rpc:call(FirstNode, antidote, update_objects,
                      [ignore, [], [{{Key, Type, Bucket}, increment, 1}]]),

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

    FirstNode = hd(Nodes),
    LastNode = lists:last(Nodes),
    lager:info("FirstNode: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),

    %% Start a new tx,  perform an update on three keys.

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    update_counters(FirstNode, [Key1], [1], ignore, TxId),
    update_counters(FirstNode, [Key2], [1], ignore, TxId),
    update_counters(FirstNode, [Key3], [1], ignore, TxId),

    %% Start a new tx,  perform an update over key1.

    {ok, TxId1} = rpc:call(LastNode, cure, start_transaction, [ignore, []]),
    update_counters(LastNode, [Key1], [2], ignore, TxId1),
    {ok, _CT} = rpc:call(LastNode, cure, commit_transaction, [TxId1]),

    %% Try to commit the first tx.
    CommitResult = rpc:call(FirstNode, cure, commit_transaction, [TxId]),
    ?assertMatch({error, {aborted, _}}, CommitResult),
    pass.

%% @doc Read an update a key multiple times.
clocksi_multiple_read_update_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Key = get_random_key(),
    Type = antidote_crdt_counter_pn,
    NTimes = 100,
    Obj = {Key, Type, ?BUCKET},
    {ok, [Result1], _} = rpc:call(Node, cure, read_objects, [ignore, [], [Obj]]),
    lists:foreach(fun(_)->
                          read_update_test(Node, Key) end,
                  lists:seq(1, NTimes)),
    {ok, [Result2], _} = rpc:call(Node, cure, read_objects, [ignore, [], [Obj]]),
    ?assertEqual(Result1+NTimes, Result2),
    pass.

%% @doc Test updating prior to a read.
read_update_test(Node, Key) ->
    Type = antidote_crdt_counter_pn,
    Obj = {Key, Type, ?BUCKET},
    {ok, [Result1], _} = rpc:call(Node, cure, read_objects, [ignore, [], [Obj]]),
    update_counters(Node, [Key], [1], ignore, static),
    {ok, [Result2], _} = rpc:call(Node, cure, read_objects, [ignore, [], [Obj]]),
    ?assertEqual(Result1+1, Result2),
    pass.

get_random_key() ->
    rand_compat:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
    rand_compat:uniform(1000).  % TODO use deterministic keys in testcase

%% @doc The following function tests how two concurrent transactions work
%%      when they are interleaved.
clocksi_concurrency_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("clockSI_concurrency_test started"),
    Node = hd(Nodes),
    %% read txn starts before the write txn's prepare phase,
    Key = clocksi_conc,
    Type = antidote_crdt_counter_pn,
    {ok, TxId1} = rpc:call(Node, cure, start_transaction, [ignore, []]),
    update_counters(Node, [Key], [1], ignore, TxId1),
    rpc:call(Node, cure, clocksi_iprepare, [TxId1]),
    {ok, TxId2} = rpc:call(Node, cure, start_transaction, [ignore, []]),
    Pid = self(),
    spawn( fun() ->
                   update_counters(Node, [Key], [1], ignore, TxId2),
                   rpc:call(Node, cure, clocksi_iprepare, [TxId2]),
                   {ok, _}= rpc:call(Node, cure, clocksi_icommit, [TxId2]),
                   Pid ! ok
           end),

    {ok, _}= rpc:call(Node, cure, clocksi_icommit, [TxId1]),
    receive
        ok ->
            check_read_key(Node, Key, Type, 2, ignore, static),
            pass
    end.

%% doc The following test checks multiple updates/reads using
%% read/update_objects. It also checks that reads are returned
%% in the same order they were sent.
clocksi_parallel_ops_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bucket = test_bucket,
    Bound_object1 = {parallel_key1, antidote_crdt_counter_pn, Bucket},
    Bound_object2 = {parallel_key2, antidote_crdt_counter_pn, Bucket},
    Bound_object3 = {parallel_key3, antidote_crdt_counter_pn, Bucket},
    Bound_object4 = {parallel_key4, antidote_crdt_counter_pn, Bucket},
    Bound_object5 = {parallel_key5, antidote_crdt_counter_pn, Bucket},
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
                                                   Bound_object2, Bound_object3, Bound_object4, Bound_object5], TxId]),
    ?assertMatch({ok, [1, 2, 3, 4, 5]}, Res),

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
                                                    Bound_object2, Bound_object3, Bound_object4, Bound_object5], TxId2]),
    ?assertMatch({ok, [6, 2, 3, 4, 5]}, Res2),
    {ok, _CT2} = rpc:call(Node, antidote, commit_transaction, [TxId2]).

%% doc The following test tests sending multiple updates in a single
%% update_objects call.
%% it also tests that the coordinator StaysAlive after the first transaction,
%% and serves the request from the second one.
clocksi_static_parallel_writes_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bucket = test_bucket,
    Bound_object1 = {parallel_key6, antidote_crdt_counter_pn, Bucket},
    Bound_object2 = {parallel_key7, antidote_crdt_counter_pn, Bucket},
    Bound_object3 = {parallel_key8, antidote_crdt_counter_pn, Bucket},
    Bound_object4 = {parallel_key9, antidote_crdt_counter_pn, Bucket},
    Bound_object5 = {parallel_key10, antidote_crdt_counter_pn, Bucket},
    %% update 5 different objects
    {ok, CT} = rpc:call(Node, cure, update_objects,
                        [ignore, [],
                         [{Bound_object1, increment, 1},
                          {Bound_object2, increment, 2},
                          {Bound_object3, increment, 3},
                          {Bound_object4, increment, 4},
                          {Bound_object5, increment, 5}]
                        , true
                        ]),

    lager:info("updated 5 objects no problem"),

    {ok, Res, CT1} = rpc:call(Node, cure, obtain_objects,
                              [CT, [], [Bound_object1,
                                        Bound_object2, Bound_object3,
                                        Bound_object4, Bound_object5],
                              true, object_value
                              ]),
    ?assertMatch([1, 2, 3, 4, 5], Res),

    lager:info("read 5 objects no problem"),

    %% update 5 times the first object.
    {ok, CT2} = rpc:call(Node, cure, update_objects,
                         [CT1, [],
                          [{Bound_object1, increment, 1},
                           {Bound_object1, increment, 1},
                           {Bound_object1, increment, 1},
                           {Bound_object1, increment, 1},
                           {Bound_object1, increment, 1}]
                         , true
                         ]),

    lager:info("updated 5 times the sabe object, no problem"),

    {ok, Res1, _CT4} = rpc:call(Node, cure, obtain_objects,
                                [CT2, [], [Bound_object1], true, object_value]),
    ?assertMatch([6], Res1),
    lager:info("result is correct after reading those updates. Test passed."),
    pass.
