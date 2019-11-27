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

%% @doc antidote_SUITE:
%%    Test the basic api of antidote on multiple dcs
%%    static and interactive transactions with single and multiple Objects
%%    interactive transaction with abort
-module(antidote_SUITE).

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
         dummy_test/1,
         random_test/1,
         shard_count/1,
         dc_count/1,
         meta_data_env_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(antidote_bucket)).

init_per_suite(Config) ->
    test_utils:init_multi_dc(?MODULE, Config).

end_per_suite(Config) ->
    Config.

init_per_testcase(_Name, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() ->
    [
     shard_count,
     dc_count,
     dummy_test,
     random_test,
     meta_data_env_test
    ].


dc_count(Config) ->
    [[Node1, Node2], [Node3], [Node4]] = proplists:get_value(clusters, Config),

    %% Check external DC count
    DCs1 = rpc:call(Node1, dc_meta_data_utilities, get_dc_descriptors, []),
    DCs2 = rpc:call(Node2, dc_meta_data_utilities, get_dc_descriptors, []),
    DCs3 = rpc:call(Node3, dc_meta_data_utilities, get_dc_descriptors, []),
    DCs4 = rpc:call(Node4, dc_meta_data_utilities, get_dc_descriptors, []),

    ?assertEqual({2,2,2,2}, {length(DCs1), length(DCs2), length(DCs3), length(DCs4)}),
    ok.


shard_count(Config) ->
    [[Node1, Node2], [Node3], [Node4]] = proplists:get_value(clusters, Config),

    %% Check sharding count
    Shards1 = rpc:call(Node1, dc_meta_data_utilities, get_dc_ids, [true]),
    Shards2 = rpc:call(Node2, dc_meta_data_utilities, get_dc_ids, [true]),
    Shards3 = rpc:call(Node3, dc_meta_data_utilities, get_dc_ids, [true]),
    Shards4 = rpc:call(Node4, dc_meta_data_utilities, get_dc_ids, [true]),

    ?assertEqual({2,2,1,1}, {length(Shards1), length(Shards2), length(Shards3), length(Shards4)}),
    ok.

dummy_test(Config) ->
    Bucket = ?BUCKET,
    [[Node1, Node2] | _] = proplists:get_value(clusters, Config),
    [Node1, Node2] = proplists:get_value(nodes, Config),
    Key = antidote_key,
    Type = antidote_crdt_counter_pn,
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
    ok = time_utils:wait_until_result(F, 3, Retry, Delay),

    ok.


%% Test that perform NumWrites increments to the key:key1.
%%      Each increment is sent to a random node of a random DC.
%%      Test normal behavior of the antidote
%%      Performs a read to the first node of the cluster to check whether all the
%%      increment operations where successfully applied.
%%  Variables:  N:  Number of nodes
%%              Nodes: List of the nodes that belong to the built cluster
random_test(Config) ->
    Bucket = ?BUCKET,
    Nodes = lists:flatten(proplists:get_value(clusters, Config)),
    N = length(Nodes),

    % Distribute the updates randomly over all DCs
    NumWrites = 100,
    ListIds = [rand:uniform(N) || _ <- lists:seq(1, NumWrites)], % TODO avoid non-determinism in tests

    Obj = {log_test_key1, antidote_crdt_counter_pn, Bucket},
    F = fun(Elem) ->
                Node = lists:nth(Elem, Nodes),
                ct:log("Increment at node: ~p", [Node]),
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
    ok = time_utils:wait_until_result(G, NumWrites, Retry, Delay),
    pass.


%% tests the meta data broadcasting mechanism for environment variables
meta_data_env_test(Config) ->
    [[Node1, Node2] | _] = proplists:get_value(clusters, Config),
    DC = [Node1, Node2],

    %% save old value, each node should have the same value
    OldValue = rpc:call(Node1, dc_meta_data_utilities, get_env_meta_data, [sync_log, undefined]),
    OldValue = rpc:call(Node2, dc_meta_data_utilities, get_env_meta_data, [sync_log, undefined]),

    %% turn on sync and check for each node if update was propagated
    ok = rpc:call(Node1, logging_vnode, set_sync_log, [true]),
    lists:foreach(fun(Node) ->
        time_utils:wait_until(fun() -> Value = rpc:call(Node, logging_vnode, is_sync_log, []), Value == true end)
                  end, DC),

    %% turn off sync and check for each node if update was propagated
    ok = rpc:call(Node2, logging_vnode, set_sync_log, [false]),
    lists:foreach(fun(Node) ->
        time_utils:wait_until(fun() -> Value = rpc:call(Node, logging_vnode, is_sync_log, []), Value == false end)
                  end, DC),

    %% restore sync and check for each node if update was propagated
    ok = rpc:call(Node1, logging_vnode, set_sync_log, [OldValue]),
    lists:foreach(fun(Node) ->
        time_utils:wait_until(fun() -> Value = rpc:call(Node, logging_vnode, is_sync_log, []), Value == OldValue end)
                  end, DC).
