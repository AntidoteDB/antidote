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
         lock_already_shared_test3/1
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

end_per_testcase(_, _) ->
    ok.

all() -> [
         single_dc_sl_test1,
         single_dc_sl_test2,
         lock_already_exclusive_test1,
         lock_already_exclusive_test2,
         lock_already_exclusive_test3,
         lock_already_shared_test1,
         lock_already_shared_test2,
         lock_already_shared_test3
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
