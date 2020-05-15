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
%%
%% Gonçalo Cabrita's test module for intra-dc chain replication


-module(intra_dc_repl_SUITE).

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
    replication_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(BUCKET, test_utils:bucket(intradc_replication_bucket)).

init_per_suite(Config) ->
	Suite = ?MODULE,
    ct:pal("[~p]", [Suite]),
    test_utils:at_init_testsuite(),
	Nodes = test_utils:set_up_multi_node_dc(Config, [dev1, dev2, dev3]),
    [{clusters, Nodes} | Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    replication_test
].

%% We have a data center of 3 physical nodes. In this test, we write a value to Node1, then kill it.
%% Then we read the same value from Node2. If replication is correct the two values should be the same.
%% After we recover Node1, we expect to read the same value from Node1. 
replication_test(Config) ->
	Bucket = ?BUCKET,
	ct:pal("intra-DC replication test starting", []),
    Cluster = proplists:get_value(clusters, Config),
    ct:pal("Cluster ~p", [Cluster]),
    
    Clusters = lists:map(fun(Node) ->
        rpc:call(Node, intra_dc_leader_elector, get_cluster, [])
    end, Cluster),
    [ClusterState | _] = Clusters,
    % Check that groups are consistent
    ?assertEqual(lists:all(fun(X) -> X == ClusterState end, Clusters), true),

    % Apply write
    Key1 = simple_replication_test_dc,
    Type = antidote_crdt_counter_pn,
    Object = {Key1, Type, Bucket},
    Update = {Object, increment, 1},
    WriteResult1 = rpc:call(hd(Cluster), antidote, update_objects, [ignore, [], [Update]]),
    ct:pal("WriteResult1 ~p", [WriteResult1]),
    ?assertMatch({ok, _}, WriteResult1),

    % Kill a node
    Victim = hd(Cluster),
    test_utils:kill_nodes([Victim]),
    ct:sleep(1000),
    
    Clusters2 = lists:map(fun(Node) ->
        rpc:call(Node, intra_dc_leader_elector, get_cluster, [])
    end, tl(Cluster)),
    [ClusterState2 | _] = Clusters2,
    % Check that groups are consistent
    ?assertEqual(lists:all(fun(X) -> X == ClusterState2 end, Clusters2), true),

    % Apply read
    SecondNode = hd(tl(Cluster)),
    {ok, [ReadResult], _} = rpc:call(SecondNode, antidote, read_objects, [ignore, [], [Object]]),
    ct:pal("ReadResult ~p must be 1", [ReadResult]),
    ?assertEqual(1, ReadResult),

    % Recover
    test_utils:restart_nodes([Victim], Config),
    ct:sleep(1500),

    Clusters3 = lists:map(fun(Node) ->
        rpc:call(Node, intra_dc_leader_elector, get_cluster, [])
    end, Cluster),
    [ClusterState3 | _] = Clusters3,
    ?assertEqual(lists:all(fun(X) -> X == ClusterState3 end, Clusters3), true),

	ct:pal("intra-DC replication test passed.", []),
    pass.