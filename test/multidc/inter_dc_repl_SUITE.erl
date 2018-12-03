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

-module(inter_dc_repl_SUITE).

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
    simple_replication_test/1,
    multiple_keys_test/1,
    causality_test/1,
    atomicity_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(inter_dc_repl_bucket)).


init_per_suite(InitialConfig) ->
    Config = test_utils:init_multi_dc(?MODULE, InitialConfig),
    Clusters = proplists:get_value(clusters, Config),
    Nodes = proplists:get_value(nodes, Config),

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
    multiple_keys_test,
    causality_test,
    atomicity_test
].


simple_replication_test(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Key = simple_replication_test,
    Type = antidote_crdt_counter_pn,

    antidote_utils:update_counters(Node1, [Key], [1], ignore, static, Bucket, antidote),
    antidote_utils:update_counters(Node1, [Key], [1], ignore, static, Bucket, antidote),
    {ok, CommitTime} = antidote_utils:update_counters(Node1, [Key], [1], ignore, static, Bucket, antidote),

    antidote_utils:check_read_key(Node1, Key, Type, 3, CommitTime, static, Bucket),
    antidote_utils:check_read_key(Node2, Key, Type, 3, CommitTime, static, Bucket),
    pass.


multiple_keys_test(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Type = antidote_crdt_counter_pn,
    Key = multiple_keys_test,

    lists:foreach( fun(_) ->
                           multiple_writes(Node1, Key, Type, 1, 10, rpl, Bucket)
                   end,
                   lists:seq(1, 10)),
    {ok, CommitTime} = antidote_utils:update_counters(Node1, [Key], [1], ignore, static, Bucket, antidote),

    multiple_reads(Node1, Key, Type, 1, 10, 10, CommitTime, Bucket),
    multiple_reads(Node2, Key, Type, 1, 10, 10, CommitTime, Bucket),
    pass.



causality_test(Config) ->
    Bucket = ?BUCKET,
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Type = antidote_crdt_set_aw,
    Key = causality_test,

    %% add element e to orset in one DC
    %% remove element e from other DC
    %% result set should not contain e
    ct:log("Adding and removing elements"),
    {ok, CommitTime1} = antidote_utils:update_sets_clock(Node1, [Key], [{add, first}], ignore, Bucket),
    {ok, CommitTime2} = antidote_utils:update_sets_clock(Node1, [Key], [{add, second}], CommitTime1, Bucket),
    {ok, CommitTime3} = antidote_utils:update_sets_clock(Node2, [Key], [{remove, first}], CommitTime2, Bucket),

    ct:log("Read result"),
    antidote_utils:check_read_key(Node2, Key, Type, [second], CommitTime3, static, Bucket, antidote),
    pass.


%% This tests checks reads are atomic when replicated to other DCs
%% TODO: need more deterministic test
atomicity_test(Config) ->
    Bucket = ?BUCKET,
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
                                  antidote_utils:atomic_write_txn(Node1, Key1, Key2, Key3, Type, Bucket)
                          end, lists:seq(1, 10)),
                        Caller ! writedone,
                        ct:log("Atomic writes done")
                end,
    ContRead = fun() ->
                       %% Read until all writes are read and make sure reads see atomic snapshots
                       Delay = 100,
                       Retry = 360000 div Delay, %wait for max 1 min
                       ok = time_utils:wait_until_result(fun() ->
                                                      antidote_utils:atomic_read_txn(Node2, Key1, Key2, Key3, Type, Bucket)
                                                    end,
                                                    10,
                                                    Retry,
                                                    Delay),
                       Caller ! readdone,
                       ct:log("Atomic reads done")
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


multiple_writes(Node, PreKey, _Type, Start, End, _Actor, Bucket)->
    F = fun(N, Acc) ->
        Key = list_to_atom(atom_to_list(PreKey) ++ [N]),
        case update_counters(Node, [Key], [1], ignore, static, Bucket) of
            {ok, _} ->
                Acc;
            Other ->
                [{Key, Other} | Acc]
        end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).


multiple_reads(Node, PreKey, Type, Start, End, Total, CommitTime, Bucket) ->
    F = fun(N, Acc) ->
        Key = list_to_atom(atom_to_list(PreKey) ++ [N]),
        antidote_utils:check_read_key(Node, Key, Type, Total, CommitTime, static, Bucket, antidote),
        Acc
        end,
    lists:foldl(F, [], lists:seq(Start, End)).


update_counters(Node, Keys, IncValues, Clock, TxId, Bucket) ->
    Updates = lists:map(fun({Key, Inc}) ->
        {{Key, antidote_crdt_counter_pn, Bucket}, increment, Inc}
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
