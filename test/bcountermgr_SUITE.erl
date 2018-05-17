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
-module(bcountermgr_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([new_bcounter_test/1,
         test_dec_success/1,
         test_dec_fail/1,
         test_dec_multi_success0/1,
         test_dec_multi_success1/1,
         conditional_write_test_run/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(TYPE, antidote_crdt_counter_b).
-define(BUCKET, bcounter_bucket).
-define(RETRY_COUNT, 5).


init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = lists:flatten(Clusters),

    %Ensure that write operations are certified
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
        [antidote, txn_cert, true]) end, Nodes),

    %Check that indeed transactions certification is turned on
    {ok, true} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_cert]),

    [{nodes, Nodes},
     {clusters, Clusters}|Config].


end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [
         new_bcounter_test,
         test_dec_success,
         test_dec_fail,
         test_dec_multi_success0,
         test_dec_multi_success1,
         conditional_write_test_run
        ].

%% Tests creating a new `bcounter()'.
new_bcounter_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Key = bcounter1_mgr,
    check_read(Node1, Key, 0).

test_dec_success(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter2_mgr,
    {ok, _} = execute_op(Node1, increment, Key, 10, Actor),
    {ok, CommitTime} = execute_op(Node1, decrement, Key, 4, Actor),
    check_read(Node2, Key, 6, CommitTime).

test_dec_fail(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter3_mgr,
    {ok, CommitTime} = execute_op(Node1, increment, Key, 10, Actor),
    _ForcePropagation = read_si(Node2, Key, CommitTime),
    Result0 = execute_op_success(Node2, decrement, Key, 5, Actor, 0),
    ?assertEqual({error, no_permissions}, Result0).

test_dec_multi_success0(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter4_mgr,
    {ok, _} = execute_op(Node1, increment, Key, 10, Actor),
    {ok, CommitTime} = execute_op(Node2, decrement, Key, 5, Actor),
    check_read(Node1, Key, 5, CommitTime).

test_dec_multi_success1(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter5_mgr,
    {ok, _} = execute_op(Node1, increment, Key, 10, Actor),
    {ok, _} = execute_op(Node2, decrement, Key, 5, Actor),
    {error, no_permissions} = execute_op(Node1, decrement, Key, 6, Actor),
    check_read(Node1, Key, 5).

conditional_write_test_run(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    [Node1, Node2 | _OtherNodes] = Nodes,
    Type = antidote_crdt_counter_b,
    Key = bcounter6_mgr,
    BObj = {Key, Type, ?BUCKET},

    {ok, AfterIncrement} = execute_op(Node1, increment, Key, 10, r1),

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
    ?assertMatch({error, {aborted, _}}, CommitResult),
    %% Test that the failed transaction didn't affect the `bcounter()'.
    check_read(Node1, Key, 7, AfterTxn2).

execute_op(Node, Op, Key, Amount, Actor) ->
    execute_op_success(Node, Op, Key, Amount, Actor, ?RETRY_COUNT).

%%Auxiliary functions.
execute_op_success(Node, Op, Key, Amount, Actor, Try) ->
    ct:print("Execute OP ~p", [Key]),
    Result = rpc:call(Node, antidote, update_objects,
                      [ignore, [],
                       [{{Key, ?TYPE, ?BUCKET}, Op, {Amount, Actor} }]
                      ]
                     ),
    case Result of
        {ok, CommitTime} -> {ok, CommitTime};
        Error when Try == 0 -> Error;
        _ ->
            timer:sleep(1000),
            execute_op_success(Node, Op, Key, Amount, Actor, Try -1)
    end.

read_si(Node, Key, CommitTime) ->
    ct:print("Read si  ~p", [Key]),
    rpc:call(Node, antidote, read_objects, [CommitTime, [], [{Key, ?TYPE, ?BUCKET}]]).

check_read(Node, Key, Expected, CommitTime) ->
    {ok, [Obj], _CT} = read_si(Node, Key, CommitTime),
    ?assertEqual(Expected, ?TYPE:permissions(Obj)).

check_read(Node, Key, Expected) ->
  check_read(Node, Key, Expected, ignore).
