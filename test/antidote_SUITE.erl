%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
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
%% @doc log_test: Test that perform NumWrites increments to the key:key1.
%%      Each increment is sent to a random node of the cluster.
%%      Test normal behavior of the logging layer
%%      Performs a read to the first node of the cluster to check whether all the
%%      increment operations where successfully applied.
%%  Variables:  N:  Number of nodes
%%              Nodes: List of the nodes that belong to the built cluster
%%

-module(antidote_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([dummy_test/1,
         static_txn_single_object/1,
         static_txn_single_object_clock/1,
         static_txn_multi_objects/1,
         static_txn_multi_objects_clock/1,
         interactive_txn/1,
         random_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = hd(Clusters),
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
                                                %application:stop(lager),
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() ->
    [
     dummy_test,
     static_txn_single_object,
     static_txn_single_object_clock,
     static_txn_multi_objects,
     static_txn_multi_objects_clock,
     interactive_txn,
     random_test
    ].

dummy_test(Config) ->
    [Node1, Node2 | _Nodes] = proplists:get_value(nodes, Config),
    ct:print("Test on ~p!", [Node1]),
    Key = antidote_key,
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Object = {Key, Type, Bucket},
    Update = {Object, increment, 1},

    {ok, _} = rpc:call(Node1, antidote, update_objects, [ignore, [], [Update]]),
    {ok, _} = rpc:call(Node1, antidote, update_objects, [ignore, [], [Update]]),
    {ok, _} = rpc:call(Node2, antidote, update_objects, [ignore, [], [Update]]),
    %% Propagation of updates
    F = fun() ->
                {ok, [Val], _CommitTime} = rpc:call(Node2, antidote, read_objects, [ignore, [], [Object]]),
                Val
        end,
    Delay = 100,
    Retry = 360000 div Delay, %wait for max 1 min
    ok = test_utils:wait_until_result(F, 3, Retry, Delay),

    ok.

static_txn_single_object(Config) ->
    [Node1 | _Nodes] = proplists:get_value(nodes, Config),
    Key = antidote_key_static1,
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Object = {Key, Type, Bucket},
    Update = {Object, increment, 1},

    {ok, _} = rpc:call(Node1, antidote, update_objects, [ignore, [], [Update]]),
    {ok, [Val], _} = rpc:call(Node1, antidote, read_objects, [ignore, [], [Object]]),
    ?assertEqual(1, Val).

static_txn_single_object_clock(Config) ->
    [Node1 | _Nodes] = proplists:get_value(nodes, Config),
    Key = antidote_key_static2,
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Object = {Key, Type, Bucket},
    Update = {Object, increment, 1},

    {ok, Clock1} = rpc:call(Node1, antidote, update_objects, [ignore, [], [Update]]),
    {ok, [Val1], Clock2} = rpc:call(Node1, antidote, read_objects, [Clock1, [], [Object]]),
    ?assertEqual(1, Val1),
    {ok, Clock3} = rpc:call(Node1, antidote, update_objects, [Clock2, [], [Update]]),
    {ok, [Val2], _Clock4} = rpc:call(Node1, antidote, read_objects, [Clock3, [], [Object]]),
    ?assertEqual(2, Val2).

static_txn_multi_objects(Config) ->
    [Node1 | _Nodes] = proplists:get_value(nodes, Config),
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Keys = [antidote_static_m1, antidote_static_m2, antidote_static_m3, antidote_static_m4],
    IncValues = [1, 2, 3, 4],
    Objects = lists:map(fun(Key) ->
                                {Key, Type, Bucket}
                        end, Keys
                       ),
    Updates = lists:map(fun({Object, IncVal}) ->
                                {Object, increment, IncVal}
                        end, lists:zip(Objects, IncValues)),

    {ok, _} = rpc:call(Node1, antidote, update_objects, [ignore, [], Updates]),
    {ok, Res, _} = rpc:call(Node1, antidote, read_objects, [ignore, [], Objects]),
    ?assertEqual([1, 2, 3, 4], Res).

static_txn_multi_objects_clock(Config) ->
    [Node1 | _Nodes] = proplists:get_value(nodes, Config),
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Keys = [antidote_static_mc1, antidote_static_mc2, antidote_static_mc3, antidote_static_mc4],
    IncValues = [1, 2, 3, 4],
    Objects = lists:map(fun(Key) ->
                                {Key, Type, Bucket}
                        end, Keys
                       ),
    Updates = lists:map(fun({Object, IncVal}) ->
                                {Object, increment, IncVal}
                        end, lists:zip(Objects, IncValues)),

    {ok, Clock1} = rpc:call(Node1, antidote, update_objects, [ignore, [], Updates]),
    {ok, Res1, Clock2} = rpc:call(Node1, antidote, read_objects, [Clock1, [], Objects]),
    ?assertEqual([1, 2, 3, 4], Res1),

    {ok, Clock3} = rpc:call(Node1, antidote, update_objects, [Clock2, [], Updates]),
    {ok, Res2, _} = rpc:call(Node1, antidote, read_objects, [Clock3, [], Objects]),
    ?assertEqual([2, 4, 6, 8], Res2).

interactive_txn(Config) ->
    [Node | _Nodes] = proplists:get_value(nodes, Config),
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Keys = [antidote_int_m1, antidote_int_m2, antidote_int_m3, antidote_int_m4],
    IncValues = [1, 2, 3, 4],
    Objects = lists:map(fun(Key) ->
                                {Key, Type, Bucket}
                        end, Keys
                       ),
    Updates = lists:map(fun({Object, IncVal}) ->
                                {Object, increment, IncVal}
                        end, lists:zip(Objects, IncValues)),
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    %% update objects one by one.
    txn_seq_update_check(Node, TxId, Updates),
    %% read objects one by one
    txn_seq_read_check(Node, TxId, Objects, [1, 2, 3, 4]),
    {ok, Clock} = rpc:call(Node, antidote, commit_transaction, [TxId]),

    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [Clock, []]),
    %% read objects all at once
    {ok, Res} = rpc:call(Node, antidote, read_objects, [Objects, TxId2]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertEqual([1, 2, 3, 4], Res).

txn_seq_read_check(Node, TxId, Objects, ExpectedValues) ->
    lists:map(fun({Object, Expected}) ->
                      {ok, [Val]} = rpc:call(Node, antidote, read_objects, [[Object], TxId]),
                      ?assertEqual(Expected, Val)
              end, lists:zip(Objects, ExpectedValues)).

txn_seq_update_check(Node, TxId, Updates) ->
    lists:map(fun(Update) ->
                      Res = rpc:call(Node, antidote, update_objects, [[Update], TxId]),
                      ?assertMatch(ok, Res)
              end, Updates).

%% Test that perform NumWrites increments to the key:key1.
%%      Each increment is sent to a random node of the cluster.
%%      Test normal behavior of the antidote
%%      Performs a read to the first node of the cluster to check whether all the
%%      increment operations where successfully applied.
%%  Variables:  N:  Number of nodes
%%              Nodes: List of the nodes that belong to the built cluster
random_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    N = length(Nodes),

                                                % Distribute the updates randomly over all DCs
    NumWrites = 100,
    ListIds = [rand_compat:uniform(N) || _ <- lists:seq(1, NumWrites)], % TODO avoid nondeterminism in tests

    Obj = {log_test_key1, antidote_crdt_counter_pn, antidote_bucket},
    F = fun(Elem) ->
                Node = lists:nth(Elem, Nodes),
                ct:print("Inc at node: ~p", [Node]),
                {ok, _} = rpc:call(Node, antidote, update_objects,
                                  [ignore, [], [{Obj, increment, 1}]])
        end,
    lists:foreach(F, ListIds),

    FirstNode = hd(Nodes),

    G = fun() ->
                {ok, [Res], _} = rpc:call(FirstNode, antidote, read_objects, [ignore, [], [Obj]]),
                Res
        end,
    Delay = 1000,
    Retry = 360000 div Delay, %wait for max 1 min
    ok = test_utils:wait_until_result(G, NumWrites, Retry, Delay),
    pass.
