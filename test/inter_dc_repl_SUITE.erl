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

-module(inter_dc_repl_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([simple_replication_test/1,
         multiple_keys_test/1,
         causality_test/1,
         atomicity_test/1
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(BUCKET, "inter_dc_repl").

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

all() -> [simple_replication_test,
         multiple_keys_test,
         causality_test,
         atomicity_test].

simple_replication_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Key = simple_replication_test,
    Type = antidote_crdt_counter_pn,
    update_counters(Node1, [Key], [1], ignore, static),
    update_counters(Node1, [Key], [1], ignore, static),
    {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static),
    check_read_key(Node1, Key, Type, 3, CommitTime, static),

    check_read_key(Node2, Key, Type, 3, CommitTime, static),
    lager:info("Simple replication test passed!"),
    pass.

multiple_keys_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Type = antidote_crdt_counter_pn,
    Key = multiple_keys_test,
    lists:foreach( fun(_) ->
                           multiple_writes(Node1, Key, Type, 1, 10, rpl)
                   end,
                   lists:seq(1, 10)),
    {ok, CommitTime} = update_counters(Node1, [Key], [1], ignore, static),

    multiple_reads(Node1, Key, Type, 1, 10, 10, CommitTime),
    multiple_reads(Node2, Key, Type, 1, 10, 10, CommitTime),
    lager:info("Multiple key read-write test passed!"),
    pass.

multiple_writes(Node, PreKey, _Type, Start, End, _Actor)->
    F = fun(N, Acc) ->
                Key = list_to_atom(atom_to_list(PreKey) ++ [N]),
                case update_counters(Node, [Key], [1], ignore, static) of
                    {ok, _} ->
                        Acc;
                    Other ->
                        [{Key, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

multiple_reads(Node, PreKey, Type, Start, End, Total, CommitTime) ->
    F = fun(N, Acc) ->
                Key = list_to_atom(atom_to_list(PreKey) ++ [N]),
                check_read_key(Node, Key, Type, Total, CommitTime, static),
                Acc
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

causality_test(Config) ->
    %% add element e to orset in one DC
    %% remove element e from other DC
    %% result set should not contain e
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Type = antidote_crdt_set_aw,
    Key = causality_test,
    {ok, CommitTime1} = update_sets(Node1, [Key], [{add, first}], ignore),
    {ok, CommitTime2} = update_sets(Node1, [Key], [{add, second}], CommitTime1),

    {ok, CommitTime3} = update_sets(Node2, [Key], [{remove, first}], CommitTime2),
    %% Read result
    check_read_key(Node2, Key, Type, [second], CommitTime3, static),
    lager:info("Causality test passed!"),
    pass.

%% This tests checks reads are atomic when replicated to other DCs
%% TODO: need more deterministic test
atomicity_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Key1 = atomicity_test1,
    Key2 = atomicity_test2,
    Key3 = atomicity_test3,
    Type = antidote_crdt_counter_pn,

    Caller = self(),
    ContWrite = fun() ->
                        lists:foreach(
                          fun(_) ->
                                  atomic_write_txn(Node1, Key1, Key2, Key3, Type)
                          end, lists:seq(1, 10)),
                        Caller ! writedone,
                        lager:info("Atomic writes done")
                end,
    ContRead = fun() ->
                       %% Read until all writes are read and make sure reads see atomic snapshots
                       Delay = 100,
                       Retry = 360000 div Delay, %wait for max 1 min
                       ok = test_utils:wait_until_result(fun() ->
                                                      atomic_read_txn(Node2, Key1, Key2, Key3, Type)
                                                    end,
                                                    10,
                                                    Retry,
                                                    Delay),
                       Caller ! readdone,
                       lager:info("Atomic reads done")
               end,
    spawn(ContWrite),
    spawn(ContRead),
    receive
        writedone ->
            receive
                readdone ->
                    pass
            end,
            pass
    end.

atomic_write_txn(Node, Key1, Key2, Key3, _Type) ->
    update_counters(Node, [Key1, Key2, Key3], [1, 1, 1], ignore, static).

atomic_read_txn(Node, Key1, Key2, Key3, Type) ->
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [R1]} = rpc:call(Node, antidote, read_objects,
                        [[{Key1, Type, ?BUCKET}], TxId]),
    {ok, [R2]} = rpc:call(Node, antidote, read_objects,
                        [[{Key2, Type, ?BUCKET}], TxId]),
    {ok, [R3]} = rpc:call(Node, antidote, read_objects,
                        [[{Key3, Type, ?BUCKET}], TxId]),
    rpc:call(Node, antidote, commit_transaction, [TxId]),
    ?assertEqual(R1, R2),
    ?assertEqual(R2, R3),
    R1.

check_read_key(Node, Key, Type, Expected, Clock, TxId) ->
    check_read(Node, [{Key, Type, ?BUCKET}], [Expected], Clock, TxId).

check_read(Node, Objects, Expected, Clock, TxId) ->
    case TxId of
        static ->
            {ok, Res, CT} = rpc:call(Node, antidote, read_objects, [Clock, [], Objects]),
            ?assertEqual(Expected, Res),
            {ok, Res, CT};
        _ ->
            {ok, Res} = rpc:call(Node, antidote, read_objects, [Objects, TxId]),
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
            {ok, CT} = rpc:call(Node, antidote, update_objects, [Clock, [], Updates]),
            {ok, CT};
        _->
            ok = rpc:call(Node, antidote, update_objects, [Updates, TxId]),
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
