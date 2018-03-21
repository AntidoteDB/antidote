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

-module(append_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([append_test/1,
         append_failure_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").
-define(TYPE, antidote_crdt_counter_pn).
-define(BUCKET, append_bucket).

init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    %lager_common_test_backend:bounce(debug),
    %% have the slave nodes monitor the runner node, so they can't outlive it
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = hd(Clusters),
    [{nodes, Nodes}|Config].


end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() ->
    [
    append_test,
    append_failure_test
    ].

append_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    ct:print("Starting write operation 1"),
    increment_counter(Node, append_key1),

    ct:print("Starting write operation 2"),
    increment_counter(Node, append_key2),

    ct:print("Starting read operation 1"),
    read_counter(Node, append_key1, 1),
    ct:print("Starting read operation 2"),
    read_counter(Node, append_key2, 1).

append_failure_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    N = hd(Nodes),
    Key = append_failure,

    %% Identify preference list for a given key.
    Preflist = rpc:call(N, log_utilities, get_preflist_from_key, [Key]),
    ct:print("Preference list: ~p", [Preflist]),

    NodeList = [Node || {_Index, Node} <- Preflist],
    ct:print("Responsible nodes for key: ~p", [NodeList]),

    {A, _} = lists:split(1, NodeList),
    First = hd(A),

    %% Perform successful write and read.
    increment_counter(First, Key),
    read_counter(First, Key, 1),

    %% Partition the network.
    lager:info("About to partition: ~p from: ~p", [A, Nodes -- A]),
    test_utils:partition_cluster(A, Nodes -- A),

    %% Heal the partition.
    test_utils:heal_cluster(A, Nodes -- A),

    %% Read after the partition has been healed.
    read_counter(First, Key, 1).

increment_counter(Node, Key) ->
    Obj = {Key, ?TYPE, ?BUCKET},
    WriteResult = rpc:call(Node,
                           antidote, update_objects,
                           [ignore, [], [{Obj, increment, 1}]]),
    ?assertMatch({ok, _}, WriteResult).

read_counter(Node, Key, ExpectedValue) ->
    Obj = {Key, ?TYPE, ?BUCKET},
    {ok, [Val], _} = rpc:call(Node,
                           antidote, read_objects,
                           [ignore, [], [Obj]]),
    ?assertEqual(ExpectedValue, Val).
