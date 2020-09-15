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

-module(bcountermgr_SUITE).

%% common_test callbacks
-export([init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([test_dec_success/1,
    test_dec_fail/1,
    test_dec_multi_success0/1,
    test_dec_multi_success1/1,
    conditional_write_test_run/1,
    parallel_increment_decrement/1,
    two_nodes_want_everything/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(bcountermgr_bucket)).
-define(RETRY_COUNT, 10).


init_per_suite(InitialConfig) ->
    Config = test_utils:init_multi_dc(?MODULE, InitialConfig),
    Clusters = proplists:get_value(clusters, Config),
    Nodes = lists:flatten(Clusters),

    % Ensure that write operations are certified
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env, [antidote, txn_cert, true])
                    end, Nodes),

    % Check that indeed transactions certification is turned on
    {ok, true} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_cert]),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    test_dec_success,
    test_dec_fail,
    test_dec_multi_success0,
    test_dec_multi_success1,
    conditional_write_test_run,
    parallel_increment_decrement,
    two_nodes_want_everything
].

test_dec_success(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] = [hd(Cluster) || Cluster <- Clusters],
    Actor = dc,
    Key = bcounter1_mgr_multi,

    {ok, _} = antidote_utils:bcounter_update_single(Node1, Key, Bucket, {increment, {10, Actor}}),
    {ok, CommitTime} = antidote_utils:bcounter_update_single_retry(Node1, Key, Bucket, {decrement, {4, Actor}}, ?RETRY_COUNT),

    antidote_utils:bcounter_check_read_value(Node2, Key, Bucket, CommitTime, 6).

test_dec_fail(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] = [hd(Cluster) || Cluster <- Clusters],
    Actor = dc,
    Key = bcounter2_mgr_multi,

    {ok, CommitTime} = antidote_utils:bcounter_update_single(Node1, Key, Bucket, {increment, {10, Actor}}),
    _ForcePropagation = antidote_utils:bcounter_read_single(Node2, Key, Bucket, CommitTime),
    Result0 = antidote_utils:bcounter_update_single(Node2, Key, Bucket, {decrement, {5, Actor}}),
    ?assertEqual({error, no_permissions}, Result0).

test_dec_multi_success0(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] = [hd(Cluster) || Cluster <- Clusters],
    Actor = dc,
    Key = bcounter3_mgr_multi,
    {ok, _} = antidote_utils:bcounter_update_single(Node1, Key, Bucket, {increment, {10, Actor}}),
    {ok, CommitTime} = antidote_utils:bcounter_update_single_retry(Node2, Key, Bucket, {decrement, {5, Actor}}, ?RETRY_COUNT),
    antidote_utils:bcounter_check_read_value(Node1, Key, Bucket, CommitTime, 5).

test_dec_multi_success1(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] = [hd(Cluster) || Cluster <- Clusters],
    Actor = dc,
    Key = bcounter4_mgr_multi,
    {ok, _} = antidote_utils:bcounter_update_single_retry(Node1, Key, Bucket, {increment, {10, Actor}}, ?RETRY_COUNT),
    {ok, _} = antidote_utils:bcounter_update_single_retry(Node2, Key, Bucket, {decrement, {5, Actor}}, ?RETRY_COUNT),
    {error, no_permissions} = antidote_utils:bcounter_update_single_retry(Node1, Key, Bucket, {decrement, {6, Actor}}, ?RETRY_COUNT),
    antidote_utils:bcounter_check_read_value(Node1, Key, Bucket, 5).

conditional_write_test_run(Config) ->
    Bucket = ?BUCKET,
    Nodes = proplists:get_value(nodes, Config),
    [Node1, Node2 | _OtherNodes] = Nodes,
    Type = antidote_crdt_counter_b,
    Key = bcounter5_mgr_multi,
    BObj = {Key, Type, Bucket},
    {ok, AfterIncrement} = antidote_utils:bcounter_update_single_retry(Node1, Key, Bucket, {increment, {10, rc1}}, ?RETRY_COUNT),

    %% Start a transaction on the first node and perform a read operation.
    {ok, TxId1} = rpc:call(Node1, antidote, start_transaction, [AfterIncrement, []]),
    {ok, _} = rpc:call(Node1, antidote, read_objects, [[BObj], TxId1]),
    %% Execute a transaction on the last node which performs a write operation.
    {ok, TxId2} = rpc:call(Node2, antidote, start_transaction, [AfterIncrement, []]),
    ok = rpc:call(Node2, antidote, update_objects,
        [[{BObj, decrement, {3, r1}}], TxId2]),
    End1 = rpc:call(Node2, antidote, commit_transaction, [TxId2]),
    ?assertMatch({ok, _}, End1),
    {ok, AfterTxn2} = End1,
    %% Resume the first transaction and check that it fails.
    Result0 = rpc:call(Node1, antidote, update_objects,
        [[{BObj, decrement, {3, r1}}], TxId1]),
    ?assertEqual(ok, Result0),
    CommitResult = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
    ?assertMatch({error, aborted}, CommitResult),
    %% Test that the failed transaction didn't affect the `bcounter()'.
    antidote_utils:bcounter_check_read_value(Node1, Key, Bucket, AfterTxn2, 7).

parallel_increment_decrement(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    Nodes = lists:flatten(Clusters),
    FirstNode = hd(Nodes),
    LastNode = lists:last(Nodes),
    Key = bcounter6_mgr_multi,
    [_, CommitTime] =
        test_utils:pmap(
            fun
                (Node) when Node == FirstNode ->
                    Decrement = antidote_utils:bcounter_get_increment_op(Node, 5),
                    _ = antidote_utils:bcounter_update_single_retry(Node, Key, Bucket, Decrement, ?RETRY_COUNT);
                (Node) when Node == LastNode ->
                    Decrement = antidote_utils:bcounter_get_decrement_op(Node, 4),
                    {ok, FoundCommitTime} = antidote_utils:bcounter_update_single_retry(Node, Key, Bucket, Decrement, ?RETRY_COUNT),
                    FoundCommitTime
            end, [FirstNode, LastNode]),
    lists:foreach(
        fun(Node) ->
            {BCounter, _} = antidote_utils:bcounter_read_single(Node, Key, Bucket, CommitTime),
            ?assertEqual(1, antidote_crdt_counter_b:permissions(BCounter))
        end, Nodes).


%%Total credits (All Nodes * 10)
%% First wants half - 1 credits
%% LastNode wants half + 1 credits
%% Should be difficult
two_nodes_want_everything(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    Nodes = lists:flatten(Clusters),
    FirstNode = hd(Nodes),
    LastNode = lists:last(Nodes),
    NumberOfNodes = length(Nodes),
    HalfCredits = NumberOfNodes * 5,
    Key = bcounter7_mgr_multi,
    lists:foreach(
        fun(Node) ->
            Increment = antidote_utils:bcounter_get_increment_op(Node, 10),
            antidote_utils:bcounter_update_single(Node, Key, Bucket, Increment)
        end, Nodes),
    Commits = test_utils:pmap(
        fun
            (Node) when Node == FirstNode ->
                Decrement = antidote_utils:bcounter_get_decrement_op(Node, HalfCredits - 1),
                {ok, CommitTime} = antidote_utils:bcounter_update_single_retry(Node, Key, Bucket, Decrement, ?RETRY_COUNT),
                CommitTime;
            (Node) when Node == LastNode ->
                Decrement = antidote_utils:bcounter_get_decrement_op(Node, HalfCredits + 1),
                {ok, CommitTime} = antidote_utils:bcounter_update_single_retry(Node, Key, Bucket, Decrement, ?RETRY_COUNT),
                CommitTime
        end, [FirstNode, LastNode]),
    lists:foreach(
        fun(Node) ->
            {BCounter, _} = antidote_utils:bcounter_read_single(Node, Key, Bucket, vectorclock:max(Commits)),
            ?assertEqual(0, antidote_crdt_counter_b:permissions(BCounter))
        end, Nodes).
