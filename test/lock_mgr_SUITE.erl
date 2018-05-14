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
-module(lock_mgr_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([simple_transaction_tests_with_locks/1,
		locks_required_by_another_transaction_1/1,
		locks_required_by_another_transaction_2/1,
         lock_aquisition_test/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").


-define(BUCKET, bcounter_bucket).



init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    %Nodes = hd(Clusters),
    [{nodes, Clusters}|Config].

end_per_suite(Config) ->
                                                %application:stop(lager),
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [
         simple_transaction_tests_with_locks,
         locks_required_by_another_transaction_1,
         locks_required_by_another_transaction_2,
         lock_aquisition_test
        ].


simple_transaction_tests_with_locks(Config) ->
    Node = hd(hd(proplists:get_value(nodes, Config))),
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Keys = [lock1, lock2, lock3, lock4],
    IncValues = [1, 2, 3, 4],
    Objects = lists:map(fun(Key) ->
                                {Key, Type, Bucket}
                        end, Keys
                       ),
    Updates = lists:map(fun({Object, IncVal}) ->
                                {Object, increment, IncVal}
                        end, lists:zip(Objects, IncValues)),
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, [locks,Keys]]),
    %% update objects one by one.
    txn_seq_update_check(Node, TxId, Updates),
    %% read objects one by one
    txn_seq_read_check(Node, TxId, Objects, [1, 2, 3, 4]),
    {ok, Clock} = rpc:call(Node, antidote, commit_transaction, [TxId]),

    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [Clock, []]),
    %% read objects all at once
    {ok, Res} = rpc:call(Node, antidote, read_objects, [Objects, TxId2]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId2]),
    Lock_Info2 = rpc:call(Node, lock_mgr, local_locks_info, []),
    false = lists:keyfind(TxId,1,Lock_Info2),
    ?assertEqual([1, 2, 3, 4], Res).
   
locks_required_by_another_transaction_1(Config) ->
    Node = hd(hd(proplists:get_value(nodes, Config))),
    Keys = [lock5, lock6, lock7, lock8],
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, [{locks,Keys}]]),
    Lock_Info1 = rpc:call(Node, lock_mgr, local_locks_info, []),
    {_,{using,Used_Keys}} = lists:keyfind(TxId,1,Lock_Info1),
    ?assertEqual(length(Keys),length(Used_Keys)),
    ?assertEqual([],Keys--Used_Keys),
    {error,{error,[{_TxId,Missing_Keys}]}} = rpc:call(Node, antidote, start_transaction, [ignore, [{locks,Keys}]]),
    ?assertEqual(length(Keys),length(Missing_Keys)),
    ?assertEqual([],Keys -- Missing_Keys),
    
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    IncValues = [1, 2, 3, 4],
    Objects = lists:map(fun(Key) ->
                                {Key, Type, Bucket}
                        end, Keys
                       ),
    Updates = lists:map(fun({Object, IncVal}) ->
                                {Object, increment, IncVal}
                        end, lists:zip(Objects, IncValues)),
    %% update objects one by one.
    txn_seq_update_check(Node, TxId, Updates),
    %% read objects one by one
    txn_seq_read_check(Node, TxId, Objects, [1, 2, 3, 4]),
    {ok, Clock} = rpc:call(Node, antidote, commit_transaction, [TxId]),
    
    
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [Clock, []]),
    %% read objects all at once
    {ok, _Res} = rpc:call(Node, antidote, read_objects, [Objects, TxId2]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId2]),
    
    
    
    
    
    timer:sleep(500),
    Lock_Info2 = rpc:call(Node, lock_mgr, local_locks_info, []),
    false = lists:keyfind(TxId,1,Lock_Info2),
    ok.
    
locks_required_by_another_transaction_2(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node2 = hd(tl(hd(Nodes))),
    Node3 = hd(tl(Nodes)),
    Node4 = hd(tl(tl(Nodes))),
    Keys = [lock14, lock13, lock12, lock11],
    {ok, TxId} = rpc:call(Node1, antidote, start_transaction, [ignore, [{locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr, local_locks_info, []),
    {_,{using,Used_Keys}} = lists:keyfind(TxId,1,Lock_Info1),
    ?assertEqual(length(Keys),length(Used_Keys)),
    ?assertEqual([],Keys--Used_Keys),
    {error,{error,[{_TxId,Missing_Keys0}]}} = rpc:call(Node1, antidote, start_transaction, [ignore, [{locks,Keys}]]),
    {error,{error,[{_TxId,Missing_Keys1}]}} = rpc:call(Node2, antidote, start_transaction, [ignore, [{locks,[hd(Keys)]}]]),
    {error,{error,[{_TxId,Missing_Keys2}]}} = rpc:call(Node3, antidote, start_transaction, [ignore, [{locks,[hd(tl(Keys))]}]]),
    {error,{error,[{_TxId,Missing_Keys3}]}} = rpc:call(Node4, antidote, start_transaction, [ignore, [{locks,tl(Keys)}]]),
    ?assertEqual(length(Keys),length(Missing_Keys0)),
    ?assertEqual([],Keys -- Missing_Keys0),
    ?assertEqual(length([hd(Keys)]),length(Missing_Keys1)),
    ?assertEqual([],[hd(Keys)] -- Missing_Keys1),
    ?assertEqual(length([hd(tl(Keys))]),length(Missing_Keys2)),
    ?assertEqual([],[hd(tl(Keys))] -- Missing_Keys2),
    ?assertEqual(length(tl(Keys)),length(Missing_Keys3)),
    ?assertEqual([],tl(Keys) -- Missing_Keys3),
    {ok, _Clock} = rpc:call(Node1, antidote, commit_transaction, [TxId]),
    timer:sleep(500),
    Lock_Info2 = rpc:call(Node1, lock_mgr, local_locks_info, []),
    false = lists:keyfind(TxId,1,Lock_Info2),
    ok.

lock_aquisition_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Keys = [lock21, lock22, lock23, lock24],
    {ok, TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr, local_locks_info, []),
    {_,{using,Used_Keys}} = lists:keyfind(TxId1,1,Lock_Info1),
    ?assertEqual(length(Keys),length(Used_Keys)),
    ?assertEqual([],Keys--Used_Keys),
    {ok, _Clock} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
    timer:wait(500),
    {ok, TxId2} = rpc:call(Node1, antidote, start_transaction, [ignore, [{locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr, local_locks_info, []),
    {_,{using,Used_Keys}} = lists:keyfind(TxId2,1,Lock_Info1),
    ?assertEqual(length(Keys),length(Used_Keys)),
    ?assertEqual([],Keys--Used_Keys),
    {ok, _Clock} = rpc:call(Node1, antidote, commit_transaction, [TxId2]),
    
    {ok, TxId3} = rpc:call(Node1, antidote, start_transaction, [ignore, [{locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr, local_locks_info, []),
    {_,{using,Used_Keys}} = lists:keyfind(TxId3,1,Lock_Info1),
    ?assertEqual(length(Keys),length(Used_Keys)),
    ?assertEqual([],Keys--Used_Keys),
    {ok, _Clock} = rpc:call(Node1, antidote, commit_transaction, [TxId3]),
    
    
    timer:sleep(500),
    Lock_Info2 = rpc:call(Node1, lock_mgr, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2),
    false = lists:keyfind(TxId2,1,Lock_Info2),
    false = lists:keyfind(TxId3,1,Lock_Info2),
    ok.

txn_seq_read_check(Node, TxId, Objects, ExpectedValues) ->
    lists:map(fun({Object, Expected}) ->
                      {ok, [Val]} = rpc:call(Node, antidote, read_objects, [[Object], TxId]),
                      ?assertEqual(Expected, Val)
              end, lists:zip(Objects, ExpectedValues)).

txn_seq_update_check(Node, TxId, Updates) ->
    lists:map(fun(Update) ->
                      Res = rpc:call(Node, antidote, update_objects, [[Update], TxId]),
                      ?assertMatch(ok, Res)
              end, Updates).



