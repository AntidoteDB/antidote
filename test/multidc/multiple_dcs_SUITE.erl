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

-module(multiple_dcs_SUITE).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

-export([multiple_writes/4,
         replicated_set_test/1,
         simple_replication_test/1,
         failure_test/1,
         blocking_test/1,
         parallel_writes_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(multiple_dcs_bucket)).

init_per_suite(InitialConfig) ->
    Config = test_utils:init_multi_dc(?MODULE, InitialConfig),
    Nodes = proplists:get_value(nodes, Config),
    Clusters = proplists:get_value(clusters, Config),

    %Ensure that the clocksi protocol is used
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
        [antidote, txn_prot, clocksi]) end, Nodes),

    %Check that indeed clocksi is running
    {ok, clocksi} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_prot]),

    Config.


end_per_suite(Config) ->
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.


all() -> [
    simple_replication_test,
    failure_test,
    blocking_test,
    parallel_writes_test,
    replicated_set_test
].


simple_replication_test(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Key = simple_replication_test_dc,
    Type = antidote_crdt_counter_pn,

    update_counters(Node1, [Key], [1], ignore, static, Bucket),
    update_counters(Node1, [Key], [1], ignore, static, Bucket),
    {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static, Bucket),

    check_read_key(Node1, Key, Type, 3, CommitTime, static, Bucket),
    ct:log("Done append in Node1"),

    check_read_key(Node3, Key, Type, 3, CommitTime, static, Bucket),
    check_read_key(Node2, Key, Type, 3, CommitTime, static, Bucket),
    ct:log("Done first round of read, I am gonna append"),

    {ok, CommitTime2} = update_counters(Node2, [Key], [1], CommitTime, static, Bucket),

    {ok, CommitTime3} = update_counters(Node3, [Key], [1], CommitTime2, static, Bucket),
    ct:log("Done append in Node3"),
    ct:log("Done waiting, I am gonna read"),

    SnapshotTime = CommitTime3,
    check_read_key(Node1, Key, Type, 5, SnapshotTime, static, Bucket),
    check_read_key(Node2, Key, Type, 5, SnapshotTime, static, Bucket),
    check_read_key(Node3, Key, Type, 5, SnapshotTime, static, Bucket),
    pass.


parallel_writes_test(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Key = parallel_writes_test,
    Type = antidote_crdt_counter_pn,
    Pid = self(),

    spawn(?MODULE, multiple_writes, [Node1, Key, Pid, Bucket]),
    spawn(?MODULE, multiple_writes, [Node2, Key, Pid, Bucket]),
    spawn(?MODULE, multiple_writes, [Node3, Key, Pid, Bucket]),

    Result = receive
        {ok, CT1} ->
            receive
                {ok, CT2} ->
                receive
                    {ok, CT3} ->
                        Time = dict:merge(fun(_K, T1, T2) ->
                                                  max(T1, T2)
                                          end,
                                          CT3, dict:merge(
                                                 fun(_K, T1, T2) ->
                                                         max(T1, T2)
                                                 end,
                                                 CT1, CT2)),

                        check_read_key(Node1, Key, Type, 15, Time, static, Bucket),
                        check_read_key(Node2, Key, Type, 15, Time, static, Bucket),
                        check_read_key(Node3, Key, Type, 15, Time, static, Bucket),
                        ct:log("Parallel reads passed"),
                        pass
                    end
            end
    end,
    ?assertEqual(Result, pass),
    pass.


multiple_writes(Node, Key, ReplyTo, Bucket) ->
    update_counters(Node, [Key], [1], ignore, static, Bucket),
    update_counters(Node, [Key], [1], ignore, static, Bucket),
    update_counters(Node, [Key], [1], ignore, static, Bucket),
    update_counters(Node, [Key], [1], ignore, static, Bucket),
    {ok, CommitTime} = update_counters(Node, [Key], [1], ignore, static, Bucket),
    ReplyTo ! {ok, CommitTime}.


%% Test: when a DC is disconnected for a while and connected back it should
%%  be able to read the missing updates. This should not affect the causal
%%  dependency protocol
failure_test(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    case rpc:call(Node1, application, get_env, [antidote, enable_logging]) of
        {ok, false} ->
            pass;
        _ ->
            Type = antidote_crdt_counter_pn,
            Key = multiplde_dc_failure_test,

            update_counters(Node1, [Key], [1], ignore, static, Bucket),

            %% Simulate failure of NODE3 by stopping the receiver
            {ok, D1} = rpc:call(Node1, inter_dc_manager, get_descriptor, []),
            {ok, D2} = rpc:call(Node2, inter_dc_manager, get_descriptor, []),

            ok = rpc:call(Node3, inter_dc_manager, forget_dcs, [[D1, D2]]),

            update_counters(Node1, [Key], [1], ignore, static, Bucket),
            %% Induce some delay
            rpc:call(Node3, antidote, read_objects,
                 [ignore, [], [{Key, Type, ?BUCKET}]]),

            {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static, Bucket),
            check_read_key(Node1, Key, Type, 3, CommitTime, static, Bucket),
            ct:log("Done append in Node1"),

            %% NODE3 comes back
            [ok, ok] = rpc:call(Node3, inter_dc_manager, observe_dcs_sync, [[D1, D2]]),
            check_read_key(Node2, Key, Type, 3, CommitTime, static, Bucket),
            ct:log("Done read from Node2"),
            check_read_key(Node3, Key, Type, 3, CommitTime, static, Bucket),
            ct:log("Done Read in Node3"),
            pass
    end.


%% This is to test a situation where interDC transactions
%% can be blocked depending on the timing of transactions
%% going between 3 DCs
blocking_test(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Type = antidote_crdt_counter_pn,
    Key = blocking_test,

    %% Drop the heartbeat messages at DC3, allowing its
    %% stable time to get old
    ok = rpc:call(Node3, inter_dc_manager, drop_ping, [true]),
    timer:sleep(5000),

    %% Perform some transactions at DC1 and DC2
    {ok, CommitTime1} = update_counters(Node1, [Key], [1], ignore, static, Bucket),
    {ok, CommitTime2} = update_counters(Node2, [Key], [1], ignore, static, Bucket),

    %% Be sure you can read the updates at DC1 and DC2
    CommitTime3 = vectorclock:max([CommitTime1, CommitTime2]),
    check_read_key(Node1, Key, Type, 2, CommitTime3, static, Bucket),
    check_read_key(Node2, Key, Type, 2, CommitTime3, static, Bucket),

    timer:sleep(1000),

    %% Allow heartbeat pings to be received at DC3 again
    ok = rpc:call(Node3, inter_dc_manager, drop_ping, [false]),
    timer:sleep(5000),

    check_read_key(Node3, Key, Type, 2, CommitTime3, static, Bucket),
    ct:log("Blocking test passed!").


replicated_set_test(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Key1 = replicated_set_test,
    Type = antidote_crdt_set_aw,

    ct:log("Writing 100 elements to set"),

    %% add 100 elements to the set on Node 1 while simultaneously reading on Node2
    CommitTimes = lists:map(fun(N) ->
                                ct:log("Writing ~p to set", [N]),
                                {ok, CommitTime} = update_sets(Node1, [Key1], [{add, N}], ignore, Bucket),
                                timer:sleep(200),
                                CommitTime
                            end, lists:seq(1, 100)),

    LastCommitTime = lists:last(CommitTimes),
    ct:log("last commit time was ~p.", [LastCommitTime]),

    %% now read on Node2
    check_read_key(Node2, Key1, Type, lists:seq(1, 100), LastCommitTime, static, Bucket),
    pass.

%% internal
check_read_key(Node, Key, Type, Expected, Clock, TxId, Bucket) ->
    check_read(Node, [{Key, Type, Bucket}], [Expected], Clock, TxId).

check_read(Node, Objects, Expected, Clock, TxId) ->
    case TxId of
        static ->
            {ok, Res, CT} = rpc:call(Node, cure, read_objects, [Clock, [], Objects]),
            ?assertEqual(Expected, Res),
            {ok, Res, CT};
        _ ->
            {ok, Res} = rpc:call(Node, cure, read_objects, [Objects, TxId]),
            ?assertEqual(Expected, Res),
            {ok, Res}
    end.

update_counters(Node, Keys, IncValues, Clock, TxId, Bucket) ->
    Updates = lists:map(fun({Key, Inc}) ->
                                {{Key, antidote_crdt_counter_pn, Bucket}, increment, Inc}
                        end,
                        lists:zip(Keys, IncValues)
                       ),

    case TxId of
        static ->
            {ok, CT} = rpc:call(Node, cure, update_objects, [Clock, [], Updates]),
            {ok, CT};
        _->
            ok = rpc:call(Node, cure, update_objects, [Updates, TxId]),
            ok
    end.

update_sets(Node, Keys, Ops, Clock, Bucket) ->
    Updates = lists:map(fun({Key, {Op, Param}}) ->
                                {{Key, antidote_crdt_set_aw, Bucket}, Op, Param}
                        end,
                        lists:zip(Keys, Ops)
                       ),
    {ok, CT} = rpc:call(Node, antidote, update_objects, [Clock, [], Updates]),
    {ok, CT}.
