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

-module(partial_repli_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([simple_external_read_test/1,
	 parallel_external_read_test/1,
	 to_log_external_read_test/1
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").


init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    NumClusters = lists:zip(lists:seq(1,length(Clusters)),Clusters),
    Buckets = 
	lists:map(fun({Num,Cluster}) ->
			  {Cluster,[list_to_binary("bucket-partial-suite" ++ integer_to_list(Num))]}
		  end, NumClusters),
    NewConfig = [{clusters, Clusters}, {buckets, Buckets} | Config],
    test_utils:finish_cluster_setup(NewConfig),

    Nodes = lists:flatten(Clusters),

    %Ensure that the clocksi protocol is used
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
        [antidote, txn_prot, clocksi]) end, Nodes),

    %Check that indeed clocksi is running
    {ok, clocksi} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_prot]),
   
    NewConfig.

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [simple_external_read_test,
	  parallel_external_read_test,
	  to_log_external_read_test].


%% This will perform a read and update to a key that is not replicated
%% at the DC that the client is connected to
%% The update is performed first so that when the read is performed
%% the update is included as part of the read request (the update
%% is included so that the read is not blocked waiting for the
%% snapshot at the external DC to catch up to the snapshot used
%% for the read [the snapshot will be behind at the external DC
%% because the sabilisation mechanism takes some time])
simple_external_read_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Key = <<"external_simple_read">>,
    Bucket = <<"bucket-partial-suite2">>,
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [{Key,Bucket}, antidote_crdt_counter, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [{Key,Bucket}, antidote_crdt_counter, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult2),
    Result = rpc:call(Node1, antidote, read,
                      [{Key,Bucket}, antidote_crdt_counter]),
    ?assertEqual({ok, 2}, Result),

    lager:info("Simple external read test passed!"),
    pass.

%% This is the same as simple_external_read_test
%% except is uses the transactional parrallel
%% read path
parallel_external_read_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Key1 = <<"external_parallel_read1">>,
    Key2 = <<"external_parallel_read2">>,
    Bucket = <<"bucket-partial-suite2">>,
    BObj1 = {Key1, antidote_crdt_counter, Bucket},
    BObj2 = {Key2, antidote_crdt_counter, Bucket},
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [{Key1,Bucket}, antidote_crdt_counter, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [{Key2,Bucket}, antidote_crdt_counter, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult2),
    {ok,{_,_,CommitTime}}=WriteResult2,
    Result = rpc:call(Node1, antidote, read_objects,
                      [CommitTime, [], [BObj1, BObj2]]),
    lager:info("the read obj results ~p", [Result]),
    ?assertMatch({ok,[1,1],_},Result),

    lager:info("Parallel external read test passed"),    
    pass.

%% This will perform a lot of reads and updates to a key
%% that is not replicated at the DC the client is connected
%% to.  At the end a read will be performed with an old
%% snapshot, so the operation will have to check the log
%% (instead of the materializer cache like in the previous tests)
%% for any updates that happened locally so that the read
%% will not be blocked at the external DC
to_log_external_read_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [FirstNode | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Type = antidote_crdt_counter,
    Key = <<"external_log_read">>,
    Bucket = <<"bucket-partial-suite2">>,
    increment_counter(FirstNode, {Key,Bucket}, 10),
    {ok, TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    increment_counter(FirstNode, {Key,Bucket}, 100),
    %% old read value is 10
    {ok, ReadResult1} = rpc:call(FirstNode,
        antidote, clocksi_iread, [TxId, {Key,Bucket}, Type]),
    ?assertEqual(10, ReadResult1),
    %% most recent read value is 15
    {ok, {_, [ReadResult2], _}} = rpc:call(FirstNode,
        antidote, clocksi_read, [{Key,Bucket}, Type]),
    ?assertEqual(110, ReadResult2),
    lager:info("External read to log test passed"),
    pass.

%% Auxiliary method to increment a counter N times.
increment_counter(_FirstNode, _Key, 0) ->
    ok;
increment_counter(FirstNode, Key, N) ->
    WriteResult = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, antidote_crdt_counter, {increment, 1}}}]]),
    ?assertMatch({ok, _}, WriteResult),
    increment_counter(FirstNode, Key, N - 1).
