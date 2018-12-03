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
    clocksi_read_time_test/1,
    clocksi_prepare_test/1,
    clocksi_cert_property_test/1,
    clocksi_certification_test/1,
    clocksi_multiple_certification_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(BUCKET, test_utils:bucket(clocksi_bucket)).

init_per_suite(Config) ->
    test_utils:init_multi_dc(?MODULE, Config).

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    clocksi_read_time_test,
    clocksi_prepare_test,
    clocksi_cert_property_test,
    clocksi_certification_test,
    clocksi_multiple_certification_test
].


%% @doc The following function tests the certification check algorithm.
%%      when two concurrent txs modify a single object, one hast to abort.
%%      Besides, it updates multiple partitions.
clocksi_multiple_certification_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    case rpc:call(hd(Nodes), application, get_env, [antidote, txn_cert]) of
        {ok, true} ->
            clocksi_multiple_test_certification_check_run(Nodes);
        _ ->
            pass
    end.

clocksi_multiple_test_certification_check_run(Nodes) ->
    Bucket = ?BUCKET,
    Key1 = clocksi_multiple_test_certification_check_key1,
    Key2 = clocksi_multiple_test_certification_check_key2,
    Key3 = clocksi_multiple_test_certification_check_key3,
    FirstNode = hd(Nodes),
    LastNode = lists:last(Nodes),
    ct:log("FirstNode: ~p", [FirstNode]),
    ct:log("LastNode: ~p", [LastNode]),

    %% Start a new tx,  perform an update on three keys.

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    antidote_utils:update_counters(FirstNode, [Key1], [1], ignore, TxId, Bucket),
    antidote_utils:update_counters(FirstNode, [Key2], [1], ignore, TxId, Bucket),
    antidote_utils:update_counters(FirstNode, [Key3], [1], ignore, TxId, Bucket),

    %% Start a new tx,  perform an update over key1.

    {ok, TxId1} = rpc:call(LastNode, cure, start_transaction, [ignore, []]),
    antidote_utils:update_counters(LastNode, [Key1], [2], ignore, TxId1, Bucket),
    {ok, _CT} = rpc:call(LastNode, cure, commit_transaction, [TxId1]),

    %% Try to commit the first tx.
    CommitResult = rpc:call(FirstNode, cure, commit_transaction, [TxId]),
    ?assertMatch({error, {aborted, _}}, CommitResult),
    pass.


%% @doc This test makes sure to block pending reads when a prepare is in progress
%% that could violate atomicity if not blocked
clocksi_prepare_test(Config) ->
    Bucket = ?BUCKET,
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    Key = clocksi_test_prepare_key1,
    Preflist = rpc:call(FirstNode, log_utilities, get_preflist_from_key, [aaa]),
    IndexNode = hd(Preflist),

    Key2 = antidote_utils:find_key_same_node(FirstNode, IndexNode, 1),

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    antidote_utils:check_read_key(FirstNode, Key, antidote_crdt_counter_pn, 0, ignore, TxId, Bucket),

    antidote_utils:update_counters(FirstNode, [Key], [1], ignore, TxId, Bucket),
    antidote_utils:check_read_key(FirstNode, Key, antidote_crdt_counter_pn, 1, ignore, TxId, Bucket),

    %% Start 2 phase commit, but do not commit
    CommitTime=rpc:call(FirstNode, cure, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),

    timer:sleep(3000),

    {ok, TxIdRead} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),

    timer:sleep(3000),
    %% start another transaction that updates the same partition
    {ok, TxId1 } = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    antidote_utils:update_counters(FirstNode, [Key2], [1], ignore, TxId1, Bucket),
    antidote_utils:check_read_key(FirstNode, Key2, antidote_crdt_counter_pn, 1, ignore, TxId1, Bucket),

    %% Start 2 phase commit for second transaction, but do not commit.
    CommitTime1 = rpc:call(FirstNode, cure, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),

    %% Commit transaction after a delay, 1 in parallel,
    spawn(antidote_utils, spawn_com, [FirstNode, TxId]),

    %% Read should return the value committed by transaction 1. But should not be
    %% blocked by the transaction 2 which is in commit phase, because it updates a different key.
    antidote_utils:check_read_key(FirstNode, Key, antidote_crdt_counter_pn, 1, ignore, TxIdRead, Bucket),

    End1 = rpc:call(FirstNode, cure, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End1),

    pass.



%% @doc The following function tests that ClockSI waits, when reading,
%%      for a tx that has updated an element that it wants to read and
%%      has a smaller TxId, but has not yet committed.
clocksi_read_time_test(Config) ->
    Bucket = ?BUCKET,
    Nodes = proplists:get_value(nodes, Config),
    %% Start a new tx,  perform an update over key abc, and send prepare.
    Key1 = clocksi_test_read_time_key1,
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, []]),
    ct:log("Tx1 Started, id : ~p", [TxId]),
    %% start a different tx and try to read key read_time.
    {ok, TxId1} = rpc:call(LastNode, cure, start_transaction, [ignore, []]),

    ct:log("Tx2 Started, id : ~p", [TxId1]),
    antidote_utils:update_counters(FirstNode, [Key1], [1], ignore, TxId, Bucket),
    CommitTime = rpc:call(FirstNode, cure, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    ct:log("Tx1 sent prepare, got commitTime=..., id : ~p", [CommitTime]),
    %% try to read key read_time.
    %% commit the first tx.
    End = rpc:call(FirstNode, cure, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),
    ct:log("Tx1 Committed."),

    ct:log("Tx2 Reading..."),
    antidote_utils:check_read_key(FirstNode, Key1, antidote_crdt_counter_pn, 0, ignore, TxId1, Bucket),

    %% prepare and commit the second transaction.
    {ok, _CommitTime} = rpc:call(FirstNode, cure, commit_transaction, [TxId1]),
    pass.


%% @doc The following function tests the certification check algorithm,
%%      when two concurrent txs modify a single object, one hast to abort.
clocksi_certification_test(Config) ->
    Bucket = ?BUCKET,
    Nodes = proplists:get_value(nodes, Config),
    case rpc:call(hd(Nodes), application, get_env, [antidote, txn_cert]) of
        {ok, true} ->
            clocksi_test_certification_check_run(Nodes, false, Bucket);
        _ ->
            pass
    end.

%% @doc The following function tests the certification check algorithm,
%%      with the property set to disable the certification
%%      when two concurrent txs modify a single object, they both must commit.
clocksi_cert_property_test(Config) ->
    Bucket = ?BUCKET,
    Nodes = proplists:get_value(nodes, Config),
    case rpc:call(hd(Nodes), application, get_env, [antidote, txn_cert]) of
        {ok, true} ->
            clocksi_test_certification_check_run(Nodes, true, Bucket);
        _ ->
            pass
    end.

clocksi_test_certification_check_run(Nodes, DisableCert, Bucket) ->
    Key1 = clockSI_test_certification_check_key1,

    FirstNode = hd(Nodes),
    LastNode = lists:last(Nodes),
    ct:log("FirstNode: ~p", [FirstNode]),
    ct:log("LastNode: ~p", [LastNode]),

    %% Start a new tx on first node, perform an update on some key.
    Properties = case DisableCert of
                     true -> [{certify, dont_certify}];
                     false -> []
                 end,

    {ok, TxId} = rpc:call(FirstNode, cure, start_transaction, [ignore, Properties, false]),
    ct:log("Tx1 Started, id : ~p", [TxId]),
    antidote_utils:update_counters(FirstNode, [Key1], [1], ignore, TxId, Bucket),

    %% Start a new tx on last node, perform an update on the same key.
    {ok, TxId1} = rpc:call(LastNode, cure, start_transaction, [ignore, []]),
    ct:log("Tx2 Started, id : ~p", [TxId1]),
    antidote_utils:update_counters(FirstNode, [Key1], [1], ignore, TxId1, Bucket),

    {ok, _CT}= rpc:call(LastNode, cure, commit_transaction, [TxId1]),

    %% Commit the first tx.
    CommitTime = rpc:call(FirstNode, cure, clocksi_iprepare, [TxId]),
    case DisableCert of
        false ->
            ?assertMatch({aborted, TxId}, CommitTime),
            ct:log("Tx1 sent prepare, got message: ~p", [CommitTime]),
            ct:log("Tx1 aborted. Test passed!");
        true ->
            ?assertMatch({ok, _}, CommitTime),
            ct:log("Tx1 sent prepare, got message: ~p", [CommitTime]),
            End2 = rpc:call(FirstNode, cure, clocksi_icommit, [TxId]),
            ?assertMatch({ok, _}, End2),
            ct:log("Tx1 committed. Test passed!")
    end,
    pass.
