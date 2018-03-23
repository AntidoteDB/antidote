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

%% If logging is disabled these tests will fail and some reads will
%% block as DCs will be waiting for missing messages, so add a
%% timeout to these calls so the test sutie can finish
-define(RPC_TIMEOUT, 10000).

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

-define(BUCKET, "multiple_dcs_node_failure").

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
    case rpc:call(Node1, application, get_env, [antidote, enable_logging]) of
        {ok, false} ->
            pass;
        _ ->
            Key = cluster_failure_test,
            Type = antidote_crdt_counter_pn,

            update_counters(Node1, [Key], [1], ignore, static),
            update_counters(Node1, [Key], [1], ignore, static),
            {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static),

            check_read_key(Node1, Key, Type, 3, ignore, static),

            %% Kill and restart a node and be sure everyhing works
            ct:print("Killing and restarting node ~w", [Node1]),
            [Node1] = test_utils:kill_and_restart_nodes([Node1], Config),

            ct:print("Done append in Node1"),
            check_read_key(Node3, Key, Type, 3, CommitTime, static),
            ct:print("Done Read in Node3"),
            check_read_key(Node2, Key, Type, 3, CommitTime, static),

            ct:print("Done first round of read, I am gonna append"),
            {ok, CommitTime2} = update_counters(Node2, [Key], [1], CommitTime, static),
            ct:print("Done append in Node2"),
            {ok, CommitTime3} = update_counters(Node3, [Key], [1], CommitTime2, static),
            ct:print("Done append in Node3"),
            ct:print("Done waiting, I am gonna read"),

            SnapshotTime = CommitTime3,
            check_read_key(Node1, Key, Type, 5, SnapshotTime, static),
            ct:print("Done read in Node1"),
            check_read_key(Node2, Key, Type, 5, SnapshotTime, static),
            ct:print("Done read in Node2"),
            check_read_key(Node3, Key, Type, 5, SnapshotTime, static),
            pass
    end.


%% In this test there are 2 DCs, the first has 2 nodes the second has 1
%% The test starts by performing some updates, ensuring they are propagated
%% The it kills one of the nodes in the 1st DC and restarts it
%% Once restarted it checks that updates are still performed safely
%% and propagated to other DCs
multiple_cluster_failure_test(Config) ->
    [Cluster1, Cluster2 | _Rest] = proplists:get_value(clusters, Config),
    [Node1, Node3|_] = Cluster1,
    case rpc:call(Node1, application, get_env, [antidote, enable_logging]) of
        {ok, false} ->
            pass;
        _ ->
            Node2 = hd(Cluster2),

            Key = multiple_cluster_failure_test,
            Type = antidote_crdt_counter_pn,

            update_counters(Node1, [Key], [1], ignore, static),
            update_counters(Node1, [Key], [1], ignore, static),
            {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static),
            check_read_key(Node1, Key, Type, 3, CommitTime, static),

            %% Kill and restart a node and be sure everyhing works
            ct:print("Killing and restarting node ~w", [Node1]),
            [Node1] = test_utils:kill_and_restart_nodes([Node1], Config),

            ct:print("Done append in Node1"),
            check_read_key(Node2, Key, Type, 3, CommitTime, static),
            check_read_key(Node3, Key, Type, 3, CommitTime, static),

            ct:print("Done first round of read, I am gonna append"),
            {ok, CommitTime2} = update_counters(Node2, [Key], [1], ignore, static),
            {ok, CommitTime3} = update_counters(Node3, [Key], [1], CommitTime2, static),
            ct:print("Done waiting, I am gonna read"),

            SnapshotTime = CommitTime3,
            check_read_key(Node1, Key, Type, 5, SnapshotTime, static),
            check_read_key(Node2, Key, Type, 5, SnapshotTime, static),
            check_read_key(Node3, Key, Type, 5, SnapshotTime, static),
            pass
    end.

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
    case rpc:call(Node1, application, get_env, [antidote, enable_logging]) of
        {ok, false} ->
            pass;
        _ ->

            Key = update_during_cluster_failure_test,
            Type = antidote_crdt_counter_pn,

            update_counters(Node1, [Key], [1], ignore, static),
            update_counters(Node1, [Key], [1], ignore, static),
            {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static),
            check_read_key(Node1, Key, Type, 3, CommitTime, static),
            ct:print("Done append in Node1"),

            %% Kill a node
            ct:print("Killing node ~w", [Node1]),
            [Node1] = test_utils:brutal_kill_nodes([Node1]),

            %% Be sure the other DC works while the node is down
            {ok, CommitTime3a} = update_counters(Node2, [Key], [1], ignore, static),

            %% Start the node back up and be sure everything works
            ct:print("Restarting node ~w", [Node1]),
            [Node1] = test_utils:restart_nodes([Node1], Config),

            %% Take the max of the commit times to be sure
            %% to read all updateds
            Time = dict:merge(fun(_K, T1, T2) ->
                max(T1, T2)
            end, CommitTime, CommitTime3a),

            check_read_key(Node1, Key, Type, 4, Time, static),
            ct:print("Done Read in Node1"),

            check_read_key(Node3, Key, Type, 4, Time, static),
            ct:print("Done Read in Node3"),
            check_read_key(Node2, Key, Type, 4, Time, static),
            ct:print("Done first round of read, I am gonna append"),

            {ok, CommitTime2} = update_counters(Node2, [Key], [1], Time, static),
            {ok, CommitTime3} = update_counters(Node3, [Key], [1], CommitTime2, static),

            SnapshotTime = CommitTime3,
            check_read_key(Node1, Key, Type, 6, SnapshotTime, static),
            check_read_key(Node2, Key, Type, 6, SnapshotTime, static),
            check_read_key(Node3, Key, Type, 6, SnapshotTime, static),
            pass
    end.

check_read_key(Node, Key, Type, Expected, Clock, TxId) ->
    check_read(Node, [{Key, Type, ?BUCKET}], [Expected], Clock, TxId).

check_read(Node, Objects, Expected, Clock, TxId) ->
    case TxId of
        static ->
            {ok, Res, CT} = rpc:call(Node, cure, read_objects, [Clock, [], Objects], ?RPC_TIMEOUT),
            ?assertEqual(Expected, Res),
            {ok, Res, CT};
        _ ->
            {ok, Res} = rpc:call(Node, cure, read_objects, [Objects, TxId], ?RPC_TIMEOUT),
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
            {ok, CT} = rpc:call(Node, cure, update_objects, [Clock, [], Updates], ?RPC_TIMEOUT),
            {ok, CT};
        _->
            ok = rpc:call(Node, cure, update_objects, [Updates, TxId], ?RPC_TIMEOUT),
            ok
    end.
