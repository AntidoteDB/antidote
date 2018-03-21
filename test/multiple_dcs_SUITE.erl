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

-define(BUCKET, "multiple_dcs").

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

    Key = simple_replication_test_dc,
    Type = antidote_crdt_counter_pn,

    update_counters(Node1, [Key], [1], ignore, static),
    update_counters(Node1, [Key], [1], ignore, static),
    {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static),

    check_read_key(Node1, Key, Type, 3, CommitTime, static),
    lager:info("Done append in Node1"),

    check_read_key(Node3, Key, Type, 3, CommitTime, static),
    check_read_key(Node2, Key, Type, 3, CommitTime, static),
    lager:info("Done first round of read, I am gonna append"),

    {ok, CommitTime2} = update_counters(Node2, [Key], [1], CommitTime, static),

    {ok, CommitTime3} = update_counters(Node3, [Key], [1], CommitTime2, static),
    lager:info("Done append in Node3"),
    lager:info("Done waiting, I am gonna read"),

    SnapshotTime = CommitTime3,
    check_read_key(Node1, Key, Type, 5, SnapshotTime, static),
    check_read_key(Node2, Key, Type, 5, SnapshotTime, static),
    check_read_key(Node3, Key, Type, 5, SnapshotTime, static),
    pass.

parallel_writes_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Key = parallel_writes_test,
    Type = antidote_crdt_counter_pn,
    Pid = self(),
    spawn(?MODULE, multiple_writes, [Node1, Key, Pid]),
    spawn(?MODULE, multiple_writes, [Node2, Key, Pid]),
    spawn(?MODULE, multiple_writes, [Node3, Key, Pid]),
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

                        check_read_key(Node1, Key, Type, 15, Time, static),
                        check_read_key(Node2, Key, Type, 15, Time, static),
                        check_read_key(Node3, Key, Type, 15, Time, static),
                        lager:info("Parallel reads passed"),
                        pass
                    end
            end
    end,
    ?assertEqual(Result, pass),
    pass.

multiple_writes(Node, Key, ReplyTo) ->
    update_counters(Node, [Key], [1], ignore, static),
    update_counters(Node, [Key], [1], ignore, static),
    update_counters(Node, [Key], [1], ignore, static),
    update_counters(Node, [Key], [1], ignore, static),
    {ok, CommitTime} = update_counters(Node, [Key], [1], ignore, static),
    ReplyTo ! {ok, CommitTime}.

%% Test: when a DC is disconnected for a while and connected back it should
%%  be able to read the missing updates. This should not affect the causal
%%  dependency protocol
failure_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    case rpc:call(Node1, application, get_env, [antidote, enable_logging]) of
        {ok, false} ->
            pass;
        _ ->
            Type = antidote_crdt_counter_pn,
            Key = multiplde_dc_failure_test,

            update_counters(Node1, [Key], [1], ignore, static),

            %% Simulate failure of NODE3 by stoping the receiver
            {ok, D1} = rpc:call(Node1, inter_dc_manager, get_descriptor, []),
            {ok, D2} = rpc:call(Node2, inter_dc_manager, get_descriptor, []),

            ok = rpc:call(Node3, inter_dc_manager, forget_dcs, [[D1, D2]]),

            update_counters(Node1, [Key], [1], ignore, static),
            %% Induce some delay
            rpc:call(Node3, antidote, read_objects,
                 [ignore, [], [{Key, Type, ?BUCKET}]]),

            {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static),
            check_read_key(Node1, Key, Type, 3, CommitTime, static),
            lager:info("Done append in Node1"),

            %% NODE3 comes back
            [ok, ok] = rpc:call(Node3, inter_dc_manager, observe_dcs_sync, [[D1, D2]]),
            check_read_key(Node2, Key, Type, 3, CommitTime, static),
            lager:info("Done read from Node2"),
            check_read_key(Node3, Key, Type, 3, CommitTime, static),
            lager:info("Done Read in Node3"),
            pass
    end.


%% This is to test a situation where interDC transactions
%% can be blocked depending on the timing of transactions
%% going between 3 DCs
blocking_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Type = antidote_crdt_counter_pn,
    Key = blocking_test,

    %% Drop the heartbeat messages at DC3, allowing its
    %% stable time to get old
    ok = rpc:call(Node3, inter_dc_manager, drop_ping, [true]),
    timer:sleep(5000),

    %% Perform some transactions at DC1 and DC2
    {ok, CommitTime1} = update_counters(Node1, [Key], [1], ignore, static),
    {ok, CommitTime2} = update_counters(Node2, [Key], [1], ignore, static),

    %% Besure you can read the updates at DC1 and DC2
    CommitTime3 = vectorclock:max([CommitTime1, CommitTime2]),
    check_read_key(Node1, Key, Type, 2, CommitTime3, static),
    check_read_key(Node2, Key, Type, 2, CommitTime3, static),

    timer:sleep(1000),

    %% Allow heartbeat pings to be received at DC3 again
    ok = rpc:call(Node3, inter_dc_manager, drop_ping, [false]),
    timer:sleep(5000),

    check_read_key(Node3, Key, Type, 2, CommitTime3, static),
    lager:info("Blocking test passed!").


replicated_set_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Key1 = replicated_set_test,
    Type = antidote_crdt_set_aw,

    lager:info("Writing 100 elements to set!!!"),

    %% add 100 elements to the set on Node 1 while simultaneously reading on Node2
    CommitTimes = lists:map(fun(N) ->
                                lager:info("Writing ~p to set", [N]),
                                {ok, CommitTime} = update_sets(Node1, [Key1], [{add, N}], ignore),
                                timer:sleep(200),
                                CommitTime
                            end, lists:seq(1, 100)),

    LastCommitTime = lists:last(CommitTimes),
    lager:info("last commit time was ~p.", [LastCommitTime]),

    %% now read on Node2
    check_read_key(Node2, Key1, Type, lists:seq(1, 100), LastCommitTime, static),
    pass.

%% internal
check_read_key(Node, Key, Type, Expected, Clock, TxId) ->
    check_read(Node, [{Key, Type, ?BUCKET}], [Expected], Clock, TxId).

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

update_counters(Node, Keys, IncValues, Clock, TxId) ->
    Updates = lists:map(fun({Key, Inc}) ->
                                {{Key, antidote_crdt_counter_pn, ?BUCKET}, increment, Inc}
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

update_sets(Node, Keys, Ops, Clock) ->
    Updates = lists:map(fun({Key, {Op, Param}}) ->
                                {{Key, antidote_crdt_set_aw, ?BUCKET}, Op, Param}
                        end,
                        lists:zip(Keys, Ops)
                       ),
    {ok, CT} = rpc:call(Node, antidote, update_objects, [Clock, [], Updates]),
    {ok, CT}.
