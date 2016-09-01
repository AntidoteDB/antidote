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

-define(TYPE, antidote_crdt_bcounter).
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
    {ok, Obj} = read(Node1, Key),
    ?assertEqual(0, ?TYPE:permissions(Obj)).

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
    {ok, Obj} = read(Node1, Key),
    ?assertEqual(5, ?TYPE:permissions(Obj)).

conditional_write_test_run(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    [Node1, Node2 | _OtherNodes] = Nodes,
    Type = antidote_crdt_bcounter,
    Key = bcounter6_mgr,

    {ok, {_,_,AfterIncrement}} = rpc:call(Node1, antidote, append,
        [Key, Type, {increment, {10, r1}}]),

    %% Start a transaction on the first node and perform a read operation.
    {ok, TxId1} = rpc:call(Node1, antidote, clocksi_istart_tx, [AfterIncrement]),
    {ok, _} = rpc:call(Node1, antidote, clocksi_iread, [TxId1, Key, Type]),
    %% Execute a transaction on the last node which performs a write operation.
    {ok, TxId2} = rpc:call(Node2, antidote, clocksi_istart_tx, [AfterIncrement]),
    ok = rpc:call(Node2, antidote, clocksi_iupdate,
             [TxId2, Key, Type, {decrement, {3, r1}}]),
    CommitTime1 = rpc:call(Node2, antidote, clocksi_iprepare, [TxId2]),
    ?assertMatch({ok, _}, CommitTime1),
    End1 = rpc:call(Node2, antidote, clocksi_icommit, [TxId2]),
    ?assertMatch({ok, _}, End1),
    {ok, {_,AfterTxn2}} = End1,
    %% Resume the first transaction and check that it fails.
    Result0 = rpc:call(Node1, antidote, clocksi_iupdate,
         [TxId1, Key, Type, {decrement, {3, r1}}]),
    ?assertEqual(ok, Result0),
    CommitTime2 = rpc:call(Node1, antidote, clocksi_iprepare, [TxId1]),
    ?assertEqual({aborted, TxId1}, CommitTime2),
    %% Test that the failed transaction didn't affect the `bcounter()'.
    Result1 = rpc:call(Node1, antidote, clocksi_read, [AfterTxn2, Key, Type]),
    {ok, {_, [Counter1], _}} = Result1,
    ?assertEqual(7, antidote_crdt_bcounter:permissions(Counter1)).

execute_op(Node, Op, Key, Amount, Actor) ->
    execute_op_success(Node, Op, Key, Amount, Actor, ?RETRY_COUNT).

%%Auxiliary functions.
execute_op_success(Node, Op, Key, Amount, Actor, Try) ->
    Result = rpc:call(Node, antidote, append,
                      [Key, ?TYPE, {Op, {Amount,Actor}}]),
    case Result of
        {ok, {_,_,CommitTime}} -> {ok, CommitTime};
        Error when Try == 0 -> Error;
        _ ->
            timer:sleep(1000),
            execute_op_success(Node, Op, Key, Amount, Actor, Try -1)
    end.

read(Node, Key) ->
    rpc:call(Node, antidote, read, [Key, ?TYPE]).

read_si(Node, Key, CommitTime) ->
    rpc:call(Node, antidote, clocksi_read, [CommitTime, Key, ?TYPE]).

check_read(Node, Key, Expected, CommitTime) ->
    {ok, {_, [Obj], _}} = read_si(Node, Key, CommitTime),
    ?assertEqual(Expected, ?TYPE:permissions(Obj)).

