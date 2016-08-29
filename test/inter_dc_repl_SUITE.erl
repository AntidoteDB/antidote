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
    Type = antidote_crdt_counter,
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [Key, Type, increment]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [Key, Type, increment]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,
    Result = rpc:call(Node1, antidote, read,
                      [Key, Type]),
    ?assertEqual({ok, 3}, Result),

    ReadResult = rpc:call(Node2,
                          antidote, clocksi_read,
                          [CommitTime, Key, Type]),
    {ok, {_,[ReadSet],_} }= ReadResult,
    ?assertEqual(3, ReadSet),
    lager:info("Simple replication test passed!"),
    pass.

multiple_keys_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Type = antidote_crdt_counter,
    Key = multiple_keys_test,
    lists:foreach( fun(_) ->
                           multiple_writes(Node1, Key, Type, 1, 10, rpl)
                   end,
                   lists:seq(1,10)),
    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [Key, Type, {increment, 1}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}} = WriteResult3,

    Result1 = multiple_reads(Node1, Key, Type, 1, 10, 10,CommitTime),
    ?assertEqual(length(Result1), 0),
    Result2 = multiple_reads(Node2, Key, Type, 1, 10, 10, CommitTime),
    ?assertEqual(length(Result2), 0),
    lager:info("Multiple key read-write test passed!"),
    pass.

multiple_writes(Node, PreKey, Type, Start, End, _Actor)->
    F = fun(N, Acc) ->
                Key = list_to_atom(atom_to_list(PreKey) ++ [N]),
                case rpc:call(Node, antidote, append,
                              [Key, Type,
                               {increment, 1}]) of
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
                case rpc:call(Node, antidote, clocksi_read, [CommitTime, Key, Type]) of
                    {error, _} ->
                        [{Key, error} | Acc];
                    {ok, {_,[Value],_}} ->
                        ?assertEqual(Value, Total),
                        Acc
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

causality_test(Config) ->
    %% add element e to orset in one DC
    %% remove element e from other DC
    %% result set should not contain e
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Type = antidote_crdt_orset,
    Key = causality_test,
    %% Add two elements in DC1
    AddResult1 = rpc:call(Node1,
                          antidote, append,
                          [Key, Type, {add, first}]),
    ?assertMatch({ok, _}, AddResult1),
    AddResult2 = rpc:call(Node1,
                          antidote, append,
                          [Key, Type, {add, second}]),
    ?assertMatch({ok, _}, AddResult2),
    {ok,{_,_,CommitTime}} = AddResult2,

    %% Remove one element from D2C
    RemoveResult = rpc:call(Node2,
                            antidote, clocksi_bulk_update,
                            [CommitTime,
                             [{update, {Key, Type, {remove, first}}}]]),
    ?assertMatch({ok, _}, RemoveResult),
    %% Read result
    Result = rpc:call(Node2, antidote, read,
                      [Key, Type]),
    ?assertMatch({ok, [second]}, Result),
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
    Type = antidote_crdt_counter,

    Caller = self(),
    ContWrite = fun() ->
                        lists:foreach(
                          fun(_) ->
                                  atomic_write_txn(Node1, Key1, Key2, Key3, Type)
                          end, lists:seq(1,10)),
                        Caller ! writedone,
                        lager:info("Atomic writes done")
                end,
    ContRead = fun() ->
                       lists:foreach(
                         fun(_) ->
                                 atomic_read_txn(Node2, Key1, Key2, Key3, Type)
                         end, lists:seq(1,20)),
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

atomic_write_txn(Node, Key1, Key2, Key3, Type) ->
    Result= rpc:call(Node, antidote, clocksi_bulk_update,
                     [
                      [{update, {Key1, Type, {increment, 1}}},
                       {update, {Key2, Type, {increment, 1}}},
                       {update, {Key3, Type, {increment, 1}}}
                      ]]),
    ?assertMatch({ok, _}, Result).

atomic_read_txn(Node, Key1, Key2, Key3, Type) ->
    {ok,TxId} = rpc:call(Node, antidote, clocksi_istart_tx, []),
    {ok, R1} = rpc:call(Node, antidote, clocksi_iread,
                        [TxId, Key1, Type]),
    {ok, R2} = rpc:call(Node, antidote, clocksi_iread,
                        [TxId, Key2, Type]),
    {ok, R3} = rpc:call(Node, antidote, clocksi_iread,
                        [TxId, Key3, Type]),
    ?assertEqual(R1,R2),
    ?assertEqual(R2,R3).
