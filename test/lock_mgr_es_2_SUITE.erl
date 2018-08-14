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
-module(lock_mgr_es_2_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([
         single_dc_sl_test1/1,
         single_dc_sl_test2/1,
         lock_already_exclusive_test1/1,
         lock_already_exclusive_test2/1,
         lock_already_exclusive_test3/1,
         lock_already_shared_test1/1,
         lock_already_shared_test2/1,
         lock_already_shared_test3/1,
         lock_shared_and_exclusive_test1/1,
         lock_shared_and_exclusive_test2/1,
         lock_shared_and_exclusive_test3/1,
         lock_shared_and_exclusive_test4/1,
         lock_acquisition_in_sequence_test1/1,
         lock_acquisition_in_sequence_test2/1,
         shared_locks_acquisition_in_sequence_test1/1,
         shared_locks_acquisition_in_sequence_test2/1,
         shared_locks_asynchroneous_test1/1,
         shared_locks_asynchroneous_test2/1,
         shared_locks_asynchroneous_test3/1,
         shared_locks_asynchroneous_test4/1,
         different_key_types_test1/1,
         locks_and_exclusive_locks_together_test1/1
        ]).

-export([
        transaction_asynchroneous_helper2/6,
        transaction_asynchroneous_in_sequence_helper2/8
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").
-include("../include/antidote.hrl").
-include("../include/inter_dc_repl.hrl").





init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Node1 = hd(hd(Clusters)),
    Node2 = hd(hd(tl(Clusters))),
    Node3 = hd(hd(tl(tl(Clusters)))),
    [{nodes, [Node1,Node2,Node3]}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
         single_dc_sl_test1,
         single_dc_sl_test2,
         lock_already_exclusive_test1,
         lock_already_exclusive_test2,
         lock_already_exclusive_test3,
         lock_already_shared_test1,
         lock_already_shared_test2,
         lock_already_shared_test3,
         lock_shared_and_exclusive_test1,
         lock_shared_and_exclusive_test2,
         lock_shared_and_exclusive_test3,
         lock_shared_and_exclusive_test4,
         lock_acquisition_in_sequence_test1,
         lock_acquisition_in_sequence_test2,
         shared_locks_acquisition_in_sequence_test1,
         shared_locks_acquisition_in_sequence_test2,
         shared_locks_asynchroneous_test1,
         shared_locks_asynchroneous_test2,
         shared_locks_asynchroneous_test3,
         shared_locks_asynchroneous_test4,
         different_key_types_test1,
         locks_and_exclusive_locks_together_test1
        ].
% One dc (leader) tries to get the shared lock of a new lock
single_dc_sl_test1(Config) ->
    [Node1 | _Nodes] = proplists:get_value(nodes, Config),
    Keys =[single_dc_sl_test1_key_1],
    {ok, TxId} = rpc:call(Node1, antidote, start_transaction, [ignore, [{shared_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    {TxId,{using,Keys,[]}} = lists:keyfind(TxId,1,Lock_Info1),
    {ok, _Clock} = rpc:call(Node1, antidote, commit_transaction, [TxId]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId,1,Lock_Info2).

% One dc (not leader) tries to get the shared lock of a new lock
single_dc_sl_test2(Config) ->
    [_Node1, Node2 | _Nodes] = proplists:get_value(nodes, Config),
    Keys =[single_dc_sl_test2_key_1],
    {ok, TxId} = rpc:call(Node2, antidote, start_transaction, [ignore, [{shared_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    {TxId,{using,Keys,[]}} = lists:keyfind(TxId,1,Lock_Info1),
    {ok, _Clock} = rpc:call(Node2, antidote, commit_transaction, [TxId]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId,1,Lock_Info2).

% One dc (leader) gets the exclusive lock and then tries to get the shared lock while holding the exclusive one
lock_already_exclusive_test1(Config) ->
    [Node1 | _Nodes] = proplists:get_value(nodes, Config),
    Keys =[lock_already_exclusive_test1_key_1],
    {ok, TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    {TxId1,{using,[],Keys}} = lists:keyfind(TxId1,1,Lock_Info1),
    {error,{error,{[{TxId1,Missing_Keys0}],[]}}} = rpc:call(Node1, antidote, start_transaction, [ignore, [{shared_locks,Keys}]]),
    Missing_Keys0=Keys,
    {ok, _Clock} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2).


% One dc (not leader) gets the exclusive lock and then tries to get the shared lock while holding the exclusive one
lock_already_exclusive_test2(Config) ->
    [_Node1,Node2 | _Nodes] = proplists:get_value(nodes, Config),
    Keys =[lock_already_exclusive_test2_key_1],
    {ok, TxId1} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    {TxId1,{using,[],Keys}} = lists:keyfind(TxId1,1,Lock_Info1),
    {error,{error,{[{TxId1,Keys}],[]}}} = rpc:call(Node2, antidote, start_transaction, [ignore, [{shared_locks,Keys}]]),
    {ok, _Clock} = rpc:call(Node2, antidote, commit_transaction, [TxId1]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2).

% One dc (not leader) gets the exclusive lock and then another dc (not leader) tries to get the shared lock
lock_already_exclusive_test3(Config) ->
    [_Node1,Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Keys =[lock_already_exclusive_test3_key_1],
    {ok, TxId1} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node3, lock_mgr_es, local_locks_info, []),
    {TxId1,{using,[],Keys}} = lists:keyfind(TxId1,1,Lock_Info1),
    {error,{error,{Keys,[]}}} = rpc:call(Node2, antidote, start_transaction, [ignore, [{shared_locks,Keys}]]),
    {ok, _Clock} = rpc:call(Node3, antidote, commit_transaction, [TxId1]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node3, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2).

% One dc (leader) gets the shared lock and then tries to get the exclusive lock
lock_already_shared_test1(Config) ->
    [Node1 | _Nodes] = proplists:get_value(nodes, Config),
    Keys =[lock_already_shared_test1_key_1],
    {ok, TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{shared_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    {TxId1,{using,Keys,[]}} = lists:keyfind(TxId1,1,Lock_Info1),
    {error,{error,{[],[{TxId1,Missing_Keys0}]}}} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Missing_Keys0=Keys,
    {ok, _Clock} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2).


% One dc (not leader) gets the shared lock and then tries to get the exclusive lock
lock_already_shared_test2(Config) ->
    [_Node1,Node2 | _Nodes] = proplists:get_value(nodes, Config),
    Keys =[lock_already_shared_test2_key_1],
    {ok, TxId1} = rpc:call(Node2, antidote, start_transaction, [ignore, [{shared_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    {TxId1,{using,Keys,[]}} = lists:keyfind(TxId1,1,Lock_Info1),
    {error,{error,{[],[{TxId1,Missing_Keys0}]}}} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Missing_Keys0=Keys,
    {ok, _Clock} = rpc:call(Node2, antidote, commit_transaction, [TxId1]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2).

% One dc (not leader) gets the shared lock and then another dc (not leader) tries to get the exclusive lock
lock_already_shared_test3(Config) ->
    [_Node1,Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Keys =[lock_already_shared_test3_key_1],
    {ok, TxId1} = rpc:call(Node2, antidote, start_transaction, [ignore, [{shared_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    {TxId1,{using,Keys,[]}} = lists:keyfind(TxId1,1,Lock_Info1),
    {error,{error,{[],Keys}}} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    {ok, _Clock} = rpc:call(Node2, antidote, commit_transaction, [TxId1]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2).

%One dc (leader) gets the shared and exclusive lock at the same time for one transaction
lock_shared_and_exclusive_test1(Config)->
    [Node1 | _Nodes] = proplists:get_value(nodes, Config),
    Keys =[lock_shared_and_exclusive_test1_key_1],
    {ok, TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{shared_locks,Keys},{exclusive_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    {TxId1,{using,Keys,Keys}} = lists:keyfind(TxId1,1,Lock_Info1),
    {ok, _Clock} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2).

%One dc (not leader) gets the shared and exclusive lock at the same time for one transaction
lock_shared_and_exclusive_test2(Config)->
    [_Node1,Node2 | _Nodes] = proplists:get_value(nodes, Config),
    Keys =[lock_shared_and_exclusive_test2_key_1],
    {ok, TxId1} = rpc:call(Node2, antidote, start_transaction, [ignore, [{shared_locks,Keys},{exclusive_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    {TxId1,{using,Keys,Keys}} = lists:keyfind(TxId1,1,Lock_Info1),
    {ok, _Clock} = rpc:call(Node2, antidote, commit_transaction, [TxId1]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2).

%One dc (leader) gets the shared and exclusive locks with a non empty intersection at the same time for one transaction
lock_shared_and_exclusive_test3(Config)->
    [Node1 | _Nodes] = proplists:get_value(nodes, Config),
    Keys1 =[lock_shared_and_exclusive_test3_key_1,lock_shared_and_exclusive_test3_key_2,lock_shared_and_exclusive_test3_key_3,lock_shared_and_exclusive_test3_key_4],
    Keys2 =[lock_shared_and_exclusive_test3_key_1,lock_shared_and_exclusive_test3_key_2,lock_shared_and_exclusive_test3_key_5,lock_shared_and_exclusive_test3_key_6],
    {ok, TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{shared_locks,Keys1},{exclusive_locks,Keys2}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    {TxId1,{using,Keys1,Keys2}} = lists:keyfind(TxId1,1,Lock_Info1),
    {ok, _Clock} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2).

%One dc (not leader) gets the shared and exclusive locks with a non empty intersection at the same time for one transaction
lock_shared_and_exclusive_test4(Config)->
    [_Node1, Node2 | _Nodes] = proplists:get_value(nodes, Config),
    Keys1 =[lock_shared_and_exclusive_test3_key_1,lock_shared_and_exclusive_test3_key_2,lock_shared_and_exclusive_test3_key_3,lock_shared_and_exclusive_test3_key_4],
    Keys2 =[lock_shared_and_exclusive_test3_key_1,lock_shared_and_exclusive_test3_key_2,lock_shared_and_exclusive_test3_key_5,lock_shared_and_exclusive_test3_key_6],
    {ok, TxId1} = rpc:call(Node2, antidote, start_transaction, [ignore, [{shared_locks,Keys1},{exclusive_locks,Keys2}]]),
    Lock_Info1 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    {TxId1,{using,Keys1,Keys2}} = lists:keyfind(TxId1,1,Lock_Info1),
    {ok, _Clock} = rpc:call(Node2, antidote, commit_transaction, [TxId1]),
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info2).

%Multiple dcs acquire and release a set of locks in a predefined order
lock_acquisition_in_sequence_test1(Config) ->
    [Node1, Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Exclusive_Locks = [lock_acquisition_in_sequence_test1_key_1,lock_acquisition_in_sequence_test1_key_2,lock_acquisition_in_sequence_test1_key_3,lock_acquisition_in_sequence_test1_key_4,lock_acquisition_in_sequence_test1_key_5],
    Shared_Locks = [lock_acquisition_in_sequence_test1_key_5,lock_acquisition_in_sequence_test1_key_6,lock_acquisition_in_sequence_test1_key_7,lock_acquisition_in_sequence_test1_key_8],
    DC_Sequence = [Node2,Node3,Node3,Node2,Node1,Node3,Node1,Node2,Node2,Node3,Node1,Node1,Node2],
    lists:foreach(fun(Elem)->lock_get_and_release_helper(Elem,Shared_Locks,Exclusive_Locks,0) end,DC_Sequence).

%Multiple dcs acquire and release a changing set of locks in a predefined order
lock_acquisition_in_sequence_test2(Config) ->
    [Node1, Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Exclusive_Locks1 = [lock_acquisition_in_sequence_test2_key_1,lock_acquisition_in_sequence_test2_key_2,lock_acquisition_in_sequence_test2_key_3,lock_acquisition_in_sequence_test2_key_4,lock_acquisition_in_sequence_test2_key_5],
    Exclusive_Locks2 = [lock_acquisition_in_sequence_test2_key_2,lock_acquisition_in_sequence_test2_key_3],
    Exclusive_Locks3 = [lock_acquisition_in_sequence_test2_key_1,lock_acquisition_in_sequence_test2_key_4,lock_acquisition_in_sequence_test2_key_5],
    Shared_Locks1 = [lock_acquisition_in_sequence_test2_key_5,lock_acquisition_in_sequence_test2_key_6,lock_acquisition_in_sequence_test2_key_7,lock_acquisition_in_sequence_test2_key_8],
    Shared_Locks2 = [lock_acquisition_in_sequence_test2_key_5,lock_acquisition_in_sequence_test2_key_6,lock_acquisition_in_sequence_test2_key_8],
    Shared_Locks3 = [lock_acquisition_in_sequence_test2_key_6,lock_acquisition_in_sequence_test2_key_7],
    
    DC_Sequence = [{Node2,1},{Node3,1},{Node3,2},{Node2,1},{Node1,3},{Node3,3},{Node1,3},{Node2,1},{Node2,2},{Node3,1},{Node1,2},{Node1,3},{Node2,2}],
    lists:foreach(fun({Node,Locks})-> case Locks of
                                         1 ->
                                            lock_get_and_release_helper(Node,Shared_Locks1,Exclusive_Locks1,0);
                                         2 ->
                                            lock_get_and_release_helper(Node,Shared_Locks2,Exclusive_Locks2,0);
                                         3 ->
                                            lock_get_and_release_helper(Node,Shared_Locks3,Exclusive_Locks3,0)
                                         end
                                     end,DC_Sequence).

%Multiple dcs acquire and release a set of shared locks in a predefined order
shared_locks_acquisition_in_sequence_test1(Config)->
    [Node1, Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Shared_Locks = [shared_locks_acquisition_in_sequence_test1_key_1,shared_locks_acquisition_in_sequence_test1_key_2,shared_locks_acquisition_in_sequence_test1_key_3,shared_locks_acquisition_in_sequence_test1_key_4],
    DC_Sequence = [Node2,Node3,Node3,Node2,Node1,Node3,Node1,Node2,Node2,Node3,Node1,Node1,Node2],
    lists:foreach(fun(Elem)->lock_get_and_release_helper(Elem,Shared_Locks,[],0) end,DC_Sequence).

%Multiple dcs acquire and release a changing set of shared locks in a predefined order
shared_locks_acquisition_in_sequence_test2(Config)->
    [Node1, Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Shared_Locks1 = [shared_locks_acquisition_in_sequence_test2_key_1,shared_locks_acquisition_in_sequence_test2_key_2,shared_locks_acquisition_in_sequence_test2_key_3,shared_locks_acquisition_in_sequence_test2_key_4,shared_locks_acquisition_in_sequence_test2_key_5],
    Shared_Locks2 = [shared_locks_acquisition_in_sequence_test2_key_2,shared_locks_acquisition_in_sequence_test2_key_3,shared_locks_acquisition_in_sequence_test2_key_5],
    Shared_Locks3 = [shared_locks_acquisition_in_sequence_test2_key_1,shared_locks_acquisition_in_sequence_test2_key_3,shared_locks_acquisition_in_sequence_test2_key_4,shared_locks_acquisition_in_sequence_test2_key_5],
    DC_Sequence = [{Node2,1},{Node3,1},{Node3,2},{Node2,1},{Node1,3},{Node3,3},{Node1,3},{Node2,1},{Node2,2},{Node3,1},{Node1,2},{Node1,3},{Node2,2}],
    lists:foreach(fun({Node,Locks})-> case Locks of
                                         1 ->
                                            lock_get_and_release_helper(Node,Shared_Locks1,[],0);
                                         2 ->
                                            lock_get_and_release_helper(Node,Shared_Locks2,[],0);
                                         3 ->
                                            lock_get_and_release_helper(Node,Shared_Locks3,[],0)
                                         end
                                     end,DC_Sequence).

%Multiple dcs try to get the same shared locks asynchroneously once
shared_locks_asynchroneous_test1(Config) ->
    [Node1, Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Shared_Locks = [shared_locks_asynchroneous_test1_key_1,shared_locks_asynchroneous_test1_key_2,shared_locks_asynchroneous_test1_key_3,shared_locks_asynchroneous_test1_key_4],
    transaction_asynchroneous_helper1(Node1, Shared_Locks, [], 0, self(), 1),
    transaction_asynchroneous_helper1(Node2, Shared_Locks, [], 0, self(), 2),
    transaction_asynchroneous_helper1(Node3, Shared_Locks, [], 0, self(), 3),
    ok = transaction_asynchroneous_helper3(1),
    ok = transaction_asynchroneous_helper3(2),
    ok = transaction_asynchroneous_helper3(3).



%Multiple dcs try to get the same shared locks asynchroneously (many times) and increment a counter by 1.
%Probably fails if "-ifdef(WAIT_FOR_SHARED_LOCKS)." is not true.
shared_locks_asynchroneous_test2(Config) ->
    [Node1, Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Shared_Locks = [shared_locks_asynchroneous_test2_key_1,shared_locks_asynchroneous_test2_key_2,shared_locks_asynchroneous_test2_key_3,shared_locks_asynchroneous_test2_key_4],
    Key = shared_locks_asynchroneous_test2_counter_key,
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Object = {Key, Type, Bucket},
    Update = {Object, increment, 1},
    {ok, TxId} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Shared_Locks}]]),
    {ok, [0]} = rpc:call(Node1, antidote, read_objects, [[Object],TxId]),
    {ok, _Clock3} = rpc:call(Node1, antidote, commit_transaction, [TxId]),
    transaction_asynchroneous_in_sequence_helper1(Node1,Shared_Locks,[],10,100,Update,self(),11),
    transaction_asynchroneous_in_sequence_helper1(Node2,Shared_Locks,[],10,100,Update,self(),22),
    transaction_asynchroneous_in_sequence_helper1(Node3,Shared_Locks,[],10,100,Update,self(),33),
    ok = transaction_asynchroneous_helper3(11),
    ok = transaction_asynchroneous_helper3(22),
    ok = transaction_asynchroneous_helper3(33),
    % Using "Shared_Locks" as exclusive locks to make sure all updates are probagated before reading the value.
    ok = check_value_helper(Node1,[],Shared_Locks,5,Object,30),
    ok = check_value_helper(Node2,[],Shared_Locks,5,Object,30),
    ok = check_value_helper(Node3,[],Shared_Locks,5,Object,30).


%Multiple "clients" on Multiple dcs try to get the same shared locks asynchroneously (many times) and increment a counter by 1.
%Probably fails if "-ifdef(WAIT_FOR_SHARED_LOCKS)." is not true.
shared_locks_asynchroneous_test3(Config) ->
    [Node1, Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Shared_Locks = [shared_locks_asynchroneous_test3_key_1,shared_locks_asynchroneous_test3_key_2,shared_locks_asynchroneous_test3_key_3,shared_locks_asynchroneous_test3_key_4],
    Key = shared_locks_asynchroneous_test3_counter_key,
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Object = {Key, Type, Bucket},
    Update = {Object, increment, 1},
    {ok, TxId} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Shared_Locks}]]),
    {ok, [0]} = rpc:call(Node1, antidote, read_objects, [[Object],TxId]),
    {ok, _Clock3} = rpc:call(Node1, antidote, commit_transaction, [TxId]),
    transaction_asynchroneous_in_sequence_helper1(Node1,Shared_Locks,[],10,100,Update,self(),110),
    transaction_asynchroneous_in_sequence_helper1(Node2,Shared_Locks,[],10,100,Update,self(),220),
    transaction_asynchroneous_in_sequence_helper1(Node3,Shared_Locks,[],10,100,Update,self(),330),
    transaction_asynchroneous_in_sequence_helper1(Node1,Shared_Locks,[],10,100,Update,self(),111),
    transaction_asynchroneous_in_sequence_helper1(Node2,Shared_Locks,[],10,100,Update,self(),221),
    transaction_asynchroneous_in_sequence_helper1(Node3,Shared_Locks,[],10,100,Update,self(),331),
    transaction_asynchroneous_in_sequence_helper1(Node1,Shared_Locks,[],10,100,Update,self(),112),
    transaction_asynchroneous_in_sequence_helper1(Node2,Shared_Locks,[],10,100,Update,self(),222),
    transaction_asynchroneous_in_sequence_helper1(Node3,Shared_Locks,[],10,100,Update,self(),332),
    ok = transaction_asynchroneous_helper3(110),
    ok = transaction_asynchroneous_helper3(220),
    ok = transaction_asynchroneous_helper3(330),
    ok = transaction_asynchroneous_helper3(111),
    ok = transaction_asynchroneous_helper3(221),
    ok = transaction_asynchroneous_helper3(331),
    ok = transaction_asynchroneous_helper3(112),
    ok = transaction_asynchroneous_helper3(222),
    ok = transaction_asynchroneous_helper3(332),
    % Using "Shared_Locks" as exclusive locks to make sure all updates are probagated before reading the value.
    ok = check_value_helper(Node1,[],Shared_Locks,5,Object,90),
    ok = check_value_helper(Node2,[],Shared_Locks,5,Object,90),
    ok = check_value_helper(Node3,[],Shared_Locks,5,Object,90).


%Multiple "clients" on Multiple dcs try to get the same shared locks asynchroneously (many times) and increment a counter by 1.
%In parallell 3 "client" tries to get the exclusive lock.
%Probably fails if "-ifdef(WAIT_FOR_SHARED_LOCKS)." is not true.
shared_locks_asynchroneous_test4(Config) ->
    [Node1, Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Shared_Locks = [shared_locks_asynchroneous_test4_key_1,shared_locks_asynchroneous_test4_key_2,shared_locks_asynchroneous_test4_key_3,shared_locks_asynchroneous_test4_key_4],
    Key = shared_locks_asynchroneous_test4_counter_key,
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Object = {Key, Type, Bucket},
    Update = {Object, increment, 1},
    {ok, TxId} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Shared_Locks}]]),
    {ok, [0]} = rpc:call(Node2, antidote, read_objects, [[Object],TxId]),
    {ok, _Clock3} = rpc:call(Node2, antidote, commit_transaction, [TxId]),
    transaction_asynchroneous_in_sequence_helper1(Node1,Shared_Locks,[],10,100,Update,self(),110),
    transaction_asynchroneous_in_sequence_helper1(Node2,Shared_Locks,[],10,100,Update,self(),220),
    transaction_asynchroneous_in_sequence_helper1(Node3,Shared_Locks,[],10,100,Update,self(),330),
    transaction_asynchroneous_in_sequence_helper1(Node3,Shared_Locks,Shared_Locks,1,100,Update,self(),440),
    transaction_asynchroneous_in_sequence_helper1(Node1,Shared_Locks,[],10,100,Update,self(),111),
    transaction_asynchroneous_in_sequence_helper1(Node2,Shared_Locks,[],10,100,Update,self(),221),
    transaction_asynchroneous_in_sequence_helper1(Node3,Shared_Locks,[],10,100,Update,self(),331),
    transaction_asynchroneous_in_sequence_helper1(Node2,Shared_Locks,Shared_Locks,1,100,Update,self(),441),
    transaction_asynchroneous_in_sequence_helper1(Node1,Shared_Locks,[],10,100,Update,self(),112),
    transaction_asynchroneous_in_sequence_helper1(Node2,Shared_Locks,[],10,100,Update,self(),222),
    transaction_asynchroneous_in_sequence_helper1(Node3,Shared_Locks,[],10,100,Update,self(),332),
    transaction_asynchroneous_in_sequence_helper1(Node1,Shared_Locks,Shared_Locks,1,100,Update,self(),442),
    ok = transaction_asynchroneous_helper3(110),
    ok = transaction_asynchroneous_helper3(220),
    ok = transaction_asynchroneous_helper3(330),
    ok = transaction_asynchroneous_helper3(111),
    ok = transaction_asynchroneous_helper3(221),
    ok = transaction_asynchroneous_helper3(331),
    ok = transaction_asynchroneous_helper3(112),
    ok = transaction_asynchroneous_helper3(222),
    ok = transaction_asynchroneous_helper3(332),
    ok = transaction_asynchroneous_helper3(440),
    ok = transaction_asynchroneous_helper3(441),
    ok = transaction_asynchroneous_helper3(442),
    % Using "Shared_Locks" as exclusive locks to make sure all updates are probagated before reading the value.
    ok = check_value_helper(Node1,[],Shared_Locks,5,Object,93),
    ok = check_value_helper(Node2,[],Shared_Locks,5,Object,93),
    ok = check_value_helper(Node3,[],Shared_Locks,5,Object,93).

% Tests if lock_mgr_es works with keys of different data types/structures.
different_key_types_test1(Config) ->
    [Node1, Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Atom_Key = different_key_types_test_key_1,
    String_Key = "different_key_types_test_key_2",
    Binary_Key = list_to_binary("different_key_types_test_key_3"),
    Bitstring_Key = <<"different_key_types_test_key_4">>,
    Tuple_Key = {different_key_types_test_key_5,different_key_types_test_key_5},
    List_Key = [different_key_types_test_key_6,different_key_types_test_key_6],
    Empyt_List_Key = [],
    Complex_Key = {[<<"different_key_types_test_key_7">>],different_key_types_test_key_7,list_to_binary("different_key_types_test_key_7")},
    %
    {ok, TxId1} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,[Atom_Key]}]]),
    {ok, TxId2} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,[String_Key]}]]),
    {ok, TxId3} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,[Binary_Key]}]]),
    {ok, TxId4} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,[Bitstring_Key]}]]),
    {ok, TxId5} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,[Tuple_Key]}]]),
    {ok, TxId6} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,[List_Key]}]]),
    {ok, TxId7} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,[Empyt_List_Key]}]]),
    {ok, TxId8} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,[Complex_Key]}]]),
    %
    {ok, _Clock1} = rpc:call(Node2, antidote, commit_transaction, [TxId1]),
    {ok, _Clock2} = rpc:call(Node1, antidote, commit_transaction, [TxId2]),
    {ok, _Clock3} = rpc:call(Node3, antidote, commit_transaction, [TxId3]),
    {ok, _Clock4} = rpc:call(Node1, antidote, commit_transaction, [TxId4]),
    {ok, _Clock5} = rpc:call(Node3, antidote, commit_transaction, [TxId5]),
    {ok, _Clock6} = rpc:call(Node2, antidote, commit_transaction, [TxId6]),
    {ok, _Clock7} = rpc:call(Node2, antidote, commit_transaction, [TxId7]),
    {ok, _Clock8} = rpc:call(Node1, antidote, commit_transaction, [TxId8]).
    
    
% Tests if exclusive locks and regular locks can be acquired at the same time.
locks_and_exclusive_locks_together_test1(Config) ->
    [Node1, Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    Locks = [locks_and_exclusive_locks_together_test1_key_1,locks_and_exclusive_locks_together_test1_test_key_2,locks_and_exclusive_locks_together_test1_test_key_3,locks_and_exclusive_locks_together_test1_test_key_4],
    {ok, TxId} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Locks},{locks,Locks}]]),
    {ok, _Clock} = rpc:call(Node2, antidote, commit_transaction, [TxId]).



% Tries to start a transaction requesting the specified locks and stops the transaction then.
% If the transaction start failed it will retry it "Retries" times.
lock_get_and_release_helper(Node,Shared_Locks,Exclusive_Locks,Retries) when (Retries =< 0) ->
    case rpc:call(Node, antidote, start_transaction, [ignore, [{shared_locks,Shared_Locks},{exclusive_locks,Exclusive_Locks}]]) of
        {ok,TxId1}->
            {ok, _Clock} = rpc:call(Node, antidote, commit_transaction, [TxId1]);
        {error,_} ->
            {error,lock_get_and_release_helper}
    end;
lock_get_and_release_helper(Node,Shared_Locks,Exclusive_Locks,Retries) ->
    case rpc:call(Node, antidote, start_transaction, [ignore, [{shared_locks,Shared_Locks},{exclusive_locks,Exclusive_Locks}]]) of
        {ok,TxId1}->
            {ok, _Clock} = rpc:call(Node, antidote, commit_transaction, [TxId1]);
        {error,_} ->
            lock_get_and_release_helper(Node,Shared_Locks,Exclusive_Locks,Retries-1)
    end.

% Spawns an asynchroneous process that starts a transaction and stops the transaction and sends a message to the caller containing the id.
% If the transaction start failed  it will retry it "Retries" times.
transaction_asynchroneous_helper1(Node,Shared_Locks,Exclusive_Locks,Retries,Caller,Id)->
    spawn(lock_mgr_es_2_SUITE,transaction_asynchroneous_helper2,[Node,Shared_Locks,Exclusive_Locks,Retries,Caller,Id]).

transaction_asynchroneous_helper2(Node,Shared_Locks,Exclusive_Locks,Retries,Caller,Id) when (Retries =< 0)->
    case rpc:call(Node, antidote, start_transaction, [ignore, [{shared_locks,Shared_Locks},{exclusive_locks,Exclusive_Locks}]]) of
        {ok,TxId1}->
            case rpc:call(Node, antidote, commit_transaction, [TxId1]) of
                {ok, _Clock}->
                    Caller ! {ok,Id};
                _ ->
                    Caller ! {error,transaction_asynchroneous_helper2,Id}
            end;
        {error,_} ->
            Caller ! {error,transaction_asynchroneous_helper2,Id}
    end;
transaction_asynchroneous_helper2(Node,Shared_Locks,Exclusive_Locks,Retries,Caller,Id)->
    case rpc:call(Node, antidote, start_transaction, [ignore, [{shared_locks,Shared_Locks},{exclusive_locks,Exclusive_Locks}]]) of
        {ok,TxId1}->
            case rpc:call(Node, antidote, commit_transaction, [TxId1]) of
                {ok, _Clock}->
                    Caller ! {ok,Id};
                _ ->
                    transaction_asynchroneous_helper2(Node,Shared_Locks,Exclusive_Locks,Retries-1,Caller,Id)
            end;
        {error,_} ->
            transaction_asynchroneous_helper2(Node,Shared_Locks,Exclusive_Locks,Retries-1,Caller,Id)
    end.
transaction_asynchroneous_helper3(Id)->
    receive 
        {ok,Id}->ok;
        {error,Error_Msg,Id} -> Msg = atom_to_list(Error_Msg),?assertError(Msg,false)
    after 300000 -> ?assertError("The test case took too long and was timed out",false)
    end.
% Asynchroneously starts a transaction on Node requesting the specified locks. Then it directly commits that transactions.
% This is done "Repetitions" times.
% If a transaction start failed it is retried "Retries_Per_Transaction" times.
transaction_asynchroneous_in_sequence_helper1(Node,Shared_Locks,Exclusive_Locks,Repetitions,Retries_Per_Transaction,Update,Caller,Id)->
    spawn(lock_mgr_es_2_SUITE,transaction_asynchroneous_in_sequence_helper2,[Node,Shared_Locks,Exclusive_Locks,Repetitions,Retries_Per_Transaction,Update,Caller,Id]).
transaction_asynchroneous_in_sequence_helper2(_,_,_,0,_,_,Caller,Id)->
    Caller ! {ok,Id};
transaction_asynchroneous_in_sequence_helper2(Node,Shared_Locks,Exclusive_Locks,Repetitions,Retries_Per_Transaction,Update,Caller,Id)->
    case transaction_asynchroneous_in_sequence_helper3(Node,Shared_Locks,Exclusive_Locks,Retries_Per_Transaction,Update) of
        ok ->
            transaction_asynchroneous_in_sequence_helper2(Node,Shared_Locks,Exclusive_Locks,Repetitions-1,Retries_Per_Transaction,Update,Caller,Id);
        {error,transaction_asynchroneous_in_sequence_helper3}->
            Caller ! {error,transaction_asynchroneous_in_sequence_helper2,Id}
    end.
transaction_asynchroneous_in_sequence_helper3(_,_,_,0,_)->
    {error,transaction_asynchroneous_in_sequence_helper3};
transaction_asynchroneous_in_sequence_helper3(Node,Shared_Locks,Exclusive_Locks,Retries_Per_Transaction,Update)->
    case rpc:call(Node, antidote, start_transaction, [ignore, [{shared_locks,Shared_Locks},{exclusive_locks,Exclusive_Locks}]]) of
        {ok,TxId}->
            ok = rpc:call(Node, antidote, update_objects, [[Update],TxId]),
            case rpc:call(Node, antidote, commit_transaction, [TxId]) of
                {ok, _Clock3} ->
                    ok;
                _ ->
                    transaction_asynchroneous_in_sequence_helper3(Node,Shared_Locks,Exclusive_Locks,Retries_Per_Transaction-1,Update)
            end;
        {error,_}->
            transaction_asynchroneous_in_sequence_helper3(Node,Shared_Locks,Exclusive_Locks,Retries_Per_Transaction-1,Update)
    end.
check_value_helper(_,_,_,0,_,_)->
    {error,check_value_helper};
check_value_helper(Node,Shared_Locks,Exclusive_Locks,Retries,Object,Value)->
    case rpc:call(Node, antidote, start_transaction, [ignore, [{shared_locks,Shared_Locks},{exclusive_locks,Exclusive_Locks}]]) of
        {ok,TxId}->
            {ok, [Val1]} = rpc:call(Node, antidote, read_objects, [[Object],TxId]),
            ?assertEqual(Value, Val1),
            case rpc:call(Node, antidote, commit_transaction, [TxId]) of
                {ok, _Clock3}->
                    ok;
                _ ->
                    check_value_helper(Node,Shared_Locks,Exclusive_Locks,Retries-1,Object,Value)
            end;
        {error,_}->
            check_value_helper(Node,Shared_Locks,Exclusive_Locks,Retries-1,Object,Value)
    end.




