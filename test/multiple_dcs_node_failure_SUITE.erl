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

-module(multiple_dcs_node_failure_SUITE).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

-export([
         multiple_cluster_failure_test/1,
         cluster_failure_test/1,
         update_during_cluster_failure_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").


init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = lists:flatten(Clusters),
    
    %Ensure that the clocksi protocol is used
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
        [antidote, txn_prot, clocksi]) end, Nodes),

    %Check that indeed clocksi is running
    {ok, clocksi} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_prot]),
   
    [{clusters, Clusters}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> 
    [multiple_cluster_failure_test,
     cluster_failure_test,
     update_during_cluster_failure_test].

%% In this test there are 3 DCs each with 1 node
%% The test starts by performing some updates, ensuring they are propagated
%% The it kills the node of the first DC
%% Once restarted it checks that updates are still performed safely
%% and propagated to other DCs
cluster_failure_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Key1 = cluster_failure_test,
    Type = antidote_crdt_counter,

    
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [Key1, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [Key1, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [Key1, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,
    ReadResult = rpc:call(Node1, antidote, read,
                          [Key1, Type]),
    ?assertEqual({ok, 3}, ReadResult),

    %% Kill and restart a node an be sure everyhing works
    ct:print("Killing and restarting node ~w", [Node1]),
    [Node1] = test_utils:kill_and_restart_nodes([Node1],Config),

    ct:print("Done append in Node1"),
    ReadResult2 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [CommitTime, Key1, Type]),
    {ok, {_,[ReadSet1],_} }= ReadResult2,
    ?assertEqual(3, ReadSet1),
    ct:print("Done Read in Node3"),
    ReadResult3 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [CommitTime, Key1, Type]),
    {ok, {_,[ReadSet2],_} }= ReadResult3,
    ?assertEqual(3, ReadSet2),

    ct:print("Done first round of read, I am gonna append"),
    WriteResult4= rpc:call(Node2,
                           antidote, clocksi_bulk_update,
                           [ CommitTime,
                             [{update, {Key1, Type, {increment, 1}}}]]),
    ?assertMatch({ok, _}, WriteResult4),
    {ok,{_,_,CommitTime2}}=WriteResult4,
    ct:print("Done append in Node2"),
    WriteResult5= rpc:call(Node3,
                           antidote, clocksi_bulk_update,
                           [CommitTime2,
                            [{update, {Key1, Type, {increment, 1}}}]]),
    ?assertMatch({ok, _}, WriteResult5),
    {ok,{_,_,CommitTime3}}=WriteResult5,
    ct:print("Done append in Node3"),
    ct:print("Done waiting, I am gonna read"),

    SnapshotTime =
        CommitTime3,
    ReadResult4 = rpc:call(Node1,
                           antidote, clocksi_read,
                           [SnapshotTime, Key1, Type]),
    {ok, {_,[ReadSet4],_} }= ReadResult4,
    ?assertEqual(5, ReadSet4),
    ct:print("Done read in Node1"),
    ReadResult5 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [SnapshotTime,Key1, Type]),
    {ok, {_,[ReadSet5],_} }= ReadResult5,
    ?assertEqual(5, ReadSet5),
    ct:print("Done read in Node2"),
    ReadResult6 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [SnapshotTime,Key1, Type]),
    {ok, {_,[ReadSet6],_} }= ReadResult6,
    ?assertEqual(5, ReadSet6),
    pass.


%% In this test there are 2 DCs, the first has 2 nodes the second has 1
%% The test starts by performing some updates, ensuring they are propagated
%% The it kills one of the nodes in the 1st DC and restarts it
%% Once restarted it checks that updates are still performed safely
%% and propagated to other DCs
multiple_cluster_failure_test(Config) ->
    [Cluster1, Cluster2 | _Rest] = proplists:get_value(clusters, Config),
    [Node1,Node3|_] = Cluster1,
    Node2 = hd(Cluster2),

    Key1 = multiple_cluster_failure_test,
    Type = antidote_crdt_counter,
    
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [Key1, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [Key1, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [Key1, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,
    ReadResult = rpc:call(Node1, antidote, read,
                          [Key1, Type]),
    ?assertEqual({ok, 3}, ReadResult),

    %% Kill and restart a node an be sure everyhing works
    ct:print("Killing and restarting node ~w", [Node1]),
    [Node1] = test_utils:kill_and_restart_nodes([Node1],Config),

    ct:print("Done append in Node1"),
    ReadResult2 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [CommitTime, Key1, Type]),
    {ok, {_,[ReadSet1],_} }= ReadResult2,
    ?assertEqual(3, ReadSet1),
    ct:print("Done Read in Node3"),
    ReadResult3 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [CommitTime, Key1, Type]),
    {ok, {_,[ReadSet2],_} }= ReadResult3,
    ?assertEqual(3, ReadSet2),

    ct:print("Done first round of read, I am gonna append"),
    WriteResult4= rpc:call(Node2,
                           antidote, clocksi_bulk_update,
                           [ CommitTime,
                             [{update, {Key1, Type, {increment, 1}}}]]),
    ?assertMatch({ok, _}, WriteResult4),
    {ok,{_,_,CommitTime2}}=WriteResult4,
    ct:print("Done append in Node2"),
    WriteResult5= rpc:call(Node3,
                           antidote, clocksi_bulk_update,
                           [CommitTime2,
                            [{update, {Key1, Type, {increment, 1}}}]]),
    ?assertMatch({ok, _}, WriteResult5),
    {ok,{_,_,CommitTime3}}=WriteResult5,
    ct:print("Done append in Node3"),
    ct:print("Done waiting, I am gonna read"),

    SnapshotTime =
        CommitTime3,
    ReadResult4 = rpc:call(Node1,
                           antidote, clocksi_read,
                           [SnapshotTime, Key1, Type]),
    {ok, {_,[ReadSet4],_} }= ReadResult4,
    ?assertEqual(5, ReadSet4),
    ct:print("Done read in Node1"),
    ReadResult5 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [SnapshotTime,Key1, Type]),
    {ok, {_,[ReadSet5],_} }= ReadResult5,
    ?assertEqual(5, ReadSet5),
    ct:print("Done read in Node2"),
    ReadResult6 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [SnapshotTime,Key1, Type]),
    {ok, {_,[ReadSet6],_} }= ReadResult6,
    ?assertEqual(5, ReadSet6),
    pass.

%% In this test there are 3 DCs each with 1 node
%% The test starts by performing some updates, ensuring they are propagated
%% The it kills the node of the first DC
%% It then perofms an update and read in the other DCs
%% It then starts the killed node back up
%% Once restarted it checks that updates are still performed safely
%% and propagated to other DCs
update_during_cluster_failure_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Key1 = update_during_cluster_failure_test,
    Type = antidote_crdt_counter,
    
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [Key1, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [Key1, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [Key1, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,
    ReadResult = rpc:call(Node1, antidote, read,
                          [Key1, Type]),
    ?assertEqual({ok, 3}, ReadResult),
    ct:print("Done append in Node1"),

    %% Kill a node
    ct:print("Killing node ~w", [Node1]),
    [Node1] = test_utils:brutal_kill_nodes([Node1]),

    %% Be sure the other DC works while the node is down
    WriteResult3a = rpc:call(Node2,
                            antidote, append,
                            [Key1, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult3a),
    {ok,{_,_,CommitTime3a}}=WriteResult3a,
    ReadResult3a = rpc:call(Node2, antidote, read,
                          [Key1, Type]),
    ct:print("read result3a ~p", [ReadResult3a]),

    ReadResult3b = rpc:call(Node3,
                           antidote, clocksi_read,
                           [CommitTime3a, Key1, Type]),
    {ok, {_,[ReadSet3b],_} }= ReadResult3b,
    ct:print("read result3b ~p", [ReadSet3b]),

    %% Start the node back up and be sure everything works
    ct:print("Restarting node ~w", [Node1]),
    [Node1] = test_utils:restart_nodes([Node1],Config),

    %% Take the max of the commit times to be sure
    %% to read all updateds
    Time = dict:merge(fun(_K, T1,T2) ->
			      max(T1,T2)
		      end, CommitTime, CommitTime3a),
		      

    ReadResult2a = rpc:call(Node1,
                           antidote, clocksi_read,
                           [Time, Key1, Type]),
    {ok, {_,[ReadSet1a],_} }= ReadResult2a,
    ?assertEqual(4, ReadSet1a),
    ct:print("Done Read in Node3"),

    ReadResult2 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [Time, Key1, Type]),
    {ok, {_,[ReadSet1],_} }= ReadResult2,
    ?assertEqual(4, ReadSet1),
    ct:print("Done Read in Node3"),
    ReadResult3 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [Time, Key1, Type]),
    {ok, {_,[ReadSet2],_} }= ReadResult3,
    ?assertEqual(4, ReadSet2),

    ct:print("Done first round of read, I am gonna append"),
    WriteResult4= rpc:call(Node2,
                           antidote, clocksi_bulk_update,
                           [Time,
                             [{update, {Key1, Type, {increment, 1}}}]]),
    ?assertMatch({ok, _}, WriteResult4),
    {ok,{_,_,CommitTime2}}=WriteResult4,
    ct:print("Done append in Node2"),
    WriteResult5= rpc:call(Node3,
                           antidote, clocksi_bulk_update,
                           [CommitTime2,
                            [{update, {Key1, Type, {increment, 1}}}]]),
    ?assertMatch({ok, _}, WriteResult5),
    {ok,{_,_,CommitTime3}}=WriteResult5,
    ct:print("Done append in Node3"),
    ct:print("Done waiting, I am gonna read"),

    SnapshotTime =
        CommitTime3,
    ReadResult4 = rpc:call(Node1,
                           antidote, clocksi_read,
                           [SnapshotTime, Key1, Type]),
    {ok, {_,[ReadSet4],_} }= ReadResult4,
    ?assertEqual(6, ReadSet4),
    ct:print("Done read in Node1"),
    ReadResult5 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [SnapshotTime,Key1, Type]),
    {ok, {_,[ReadSet5],_} }= ReadResult5,
    ?assertEqual(6, ReadSet5),
    ct:print("Done read in Node2"),
    ReadResult6 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [SnapshotTime,Key1, Type]),
    {ok, {_,[ReadSet6],_} }= ReadResult6,
    ?assertEqual(6, ReadSet6),
    pass.
  
