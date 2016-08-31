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

-module(multiple_dcs_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

-export([multiple_writes/3,
	 replicated_set_test/1,
         simple_replication_test/1,
         failure_test/1,
	 blocking_test/1,
         parallel_writes_test/1]).

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
    [simple_replication_test,
     failure_test,
     blocking_test,
     parallel_writes_test,
     replicated_set_test].

simple_replication_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Key1 = simple_replication_test_dc,
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

    lager:info("Done append in Node1"),
    ReadResult2 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [CommitTime, Key1, Type]),
    {ok, {_,[ReadSet1],_} }= ReadResult2,
    ?assertEqual(3, ReadSet1),
    lager:info("Done Read in Node3"),
    ReadResult3 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [CommitTime, Key1, Type]),
    {ok, {_,[ReadSet2],_} }= ReadResult3,
    ?assertEqual(3, ReadSet2),

    lager:info("Done first round of read, I am gonna append"),
    WriteResult4= rpc:call(Node2,
                           antidote, clocksi_bulk_update,
                           [ CommitTime,
                             [{update, {Key1, Type, {increment, 1}}}]]),
    ?assertMatch({ok, _}, WriteResult4),
    {ok,{_,_,CommitTime2}}=WriteResult4,
    lager:info("Done append in Node2"),
    WriteResult5= rpc:call(Node3,
                           antidote, clocksi_bulk_update,
                           [CommitTime2,
                            [{update, {Key1, Type, {increment, 1}}}]]),
    ?assertMatch({ok, _}, WriteResult5),
    {ok,{_,_,CommitTime3}}=WriteResult5,
    lager:info("Done append in Node3"),
    lager:info("Done waiting, I am gonna read"),

    SnapshotTime =
        CommitTime3,
    ReadResult4 = rpc:call(Node1,
                           antidote, clocksi_read,
                           [SnapshotTime, Key1, Type]),
    {ok, {_,[ReadSet4],_} }= ReadResult4,
    ?assertEqual(5, ReadSet4),
    lager:info("Done read in Node1"),
    ReadResult5 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [SnapshotTime,Key1, Type]),
    {ok, {_,[ReadSet5],_} }= ReadResult5,
    ?assertEqual(5, ReadSet5),
    lager:info("Done read in Node2"),
    ReadResult6 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [SnapshotTime,Key1, Type]),
    {ok, {_,[ReadSet6],_} }= ReadResult6,
    ?assertEqual(5, ReadSet6),
    pass.

parallel_writes_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Key = parallel_writes_test,
    Type = antidote_crdt_counter,
    Pid = self(),
    %% WriteFun = fun(A,B,C) ->
    %%                    multiple_writes(A,B,C)
    %%            end,
    spawn(?MODULE, multiple_writes,[Node1,Key,Pid]),
    spawn(?MODULE, multiple_writes,[Node2,Key,Pid]),
    spawn(?MODULE, multiple_writes,[Node3,Key,Pid]),
    Result = receive
        {ok, CT1} ->
            receive
                {ok, CT2} ->
                receive
                    {ok, CT3} ->
                        Time = dict:merge(fun(_K, T1,T2) ->
                                                  max(T1,T2)
                                          end,
                                          CT3, dict:merge(
                                                 fun(_K, T1,T2) ->
                                                         max(T1,T2)
                                                 end,
                                                 CT1, CT2)),
                        ReadResult1 = rpc:call(Node1,
                           antidote, clocksi_read,
                           [Time, Key, Type]),
                        {ok, {_,[ReadSet1],_} }= ReadResult1,
                        ?assertEqual(15, ReadSet1),
                        ReadResult2 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [Time, Key, Type]),
                        {ok, {_,[ReadSet2],_} }= ReadResult2,
                        ?assertEqual(15, ReadSet2),
                        ReadResult3 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [Time, Key, Type]),
                        {ok, {_,[ReadSet3],_} }= ReadResult3,
                        ?assertEqual(15, ReadSet3),
                        lager:info("Parallel reads passed"),
                        pass
                    end
            end
    end,
    ?assertEqual(Result, pass),
    pass.

multiple_writes(Node, Key, ReplyTo) ->
    Type = antidote_crdt_counter,
    WriteResult1 = rpc:call(Node,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult3),
    WriteResult4 = rpc:call(Node,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult4),
    WriteResult5 = rpc:call(Node,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult5),
    {ok,{_,_,CommitTime}}=WriteResult5,
    ReplyTo ! {ok, CommitTime}.

%% Test: when a DC is disconnected for a while and connected back it should
%%  be able to read the missing updates. This should not affect the causal 
%%  dependency protocol
failure_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Type = antidote_crdt_counter,
    Key = failure_test,
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult1),

    %% Simulate failure of NODE3 by stoping the receiver
    {ok, D1} = rpc:call(Node1, inter_dc_manager, get_descriptor, []),
    {ok, D2} = rpc:call(Node2, inter_dc_manager, get_descriptor, []),

    ok = rpc:call(Node3, inter_dc_manager, forget_dcs, [[D1, D2]]),

    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult2),
    %% Induce some delay
    rpc:call(Node3, antidote, read,
             [Key, Type]),

    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,
    ReadResult = rpc:call(Node1, antidote, read,
                          [Key, Type]),
    ?assertEqual({ok, 3}, ReadResult),
    lager:info("Done append in Node1"),

    %% NODE3 comes back
    [ok, ok] = rpc:call(Node3, inter_dc_manager, observe_dcs_sync, [[D1, D2]]),
    ReadResult3 = rpc:call(Node2,
                           antidote, clocksi_read,
                           [CommitTime, Key, Type]),
    {ok, {_,[ReadSet2],_} }= ReadResult3,
    ?assertEqual(3, ReadSet2),
    lager:info("Done read from Node2"),
    ReadResult2 = rpc:call(Node3,
                           antidote, clocksi_read,
                           [CommitTime, Key, Type]),
    {ok, {_,[ReadSet1],_} }= ReadResult2,
    ?assertEqual(3, ReadSet1),
    lager:info("Done Read in Node3"),
    pass.
   
%% This is to test a situation where interDC transactions
%% can be blocked depending on the timing of transactions
%% going between 3 DCs
blocking_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Type = antidote_crdt_counter,
    Key = blocking_test,

    %% Drop the heartbeat messages at DC3, allowing its
    %% stable time to get old
    ok = rpc:call(Node3, inter_dc_manager, drop_ping, [true]),
    timer:sleep(5000),

    %% Perform some transactions at DC1 and DC2
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult1),
    {ok,{_,_,CommitTime1}}=WriteResult1,
    WriteResult2 = rpc:call(Node2,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult2),
    {ok,{_,_,CommitTime2}}=WriteResult2,
    
    %% Besure you can read the updates at DC1 and DC2
    CommitTime3 = vectorclock:max([CommitTime1,CommitTime2]),
    ReadResult = rpc:call(Node1,
                          antidote, clocksi_read,
                          [CommitTime3, Key, Type]),
    {ok, {_,[ReadSet],_} }= ReadResult,
    ?assertEqual(2, ReadSet),
    ReadResult2 = rpc:call(Node2,
                          antidote, clocksi_read,
                          [CommitTime3, Key, Type]),
    {ok, {_,[ReadSet2],_} }= ReadResult2,
    ?assertEqual(2, ReadSet2),


    timer:sleep(1000),

    %% Allow heartbeat pings to be received at DC3 again
    ok = rpc:call(Node3, inter_dc_manager, drop_ping, [false]),
    timer:sleep(5000),

    %% Check that the updates are visible at DC3
    ReadResult3 = rpc:call(Node3,
                          antidote, clocksi_read,
                          [CommitTime3, Key, Type]),
    {ok, {_,[ReadSet3],_} }= ReadResult3,
    ?assertEqual(2, ReadSet3),

    lager:info("Blocking test passed!").


replicated_set_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Key1 = replicated_set_test,
    Type = antidote_crdt_orset,

    lager:info("Writing 100 elements to set!!!"),

    %% add 100 elements to the set on Node 1 while simultaneously reading on Node2
    CommitTimes = lists:map(fun(N) ->
				    lager:info("Writing ~p to set", [N]),
				    WriteResult1 = rpc:call(Node1, antidote, clocksi_execute_tx,
							    [[{update, {Key1, Type, {add, N}}}]]),
				    ?assertMatch({ok, _}, WriteResult1),
				    {ok, {_, _, CommitTime}} = WriteResult1,
				    
				    {ok, {_, [SetValue], _}} = rpc:call(Node2,
									antidote, clocksi_read,
									[ignore, Key1, Type]),
				    lager:info("Read value ~p", [SetValue]),
				    case length(SetValue) > 0 of
					true ->
					    ?assertEqual(lists:seq(1,lists:max(SetValue)), SetValue);
					false ->
					    ok
				    end,
				    timer:sleep(200),
				    CommitTime
			    end, lists:seq(1, 100)),
    
    LastCommitTime = lists:last(CommitTimes),
    lager:info("last commit time was ~p.", [LastCommitTime]),
    
    %% now read on Node2
    ReadResult = rpc:call(Node2,
			  antidote, clocksi_read,
			  [LastCommitTime, Key1, Type]),
    lager:info("Read value ~p.", [ReadResult]),
    {ok, {_, [SetValue], _}} = ReadResult,
    %% expecting to read values 1-100
    ?assertEqual(lists:seq(1, 100), SetValue),
    
    pass.
