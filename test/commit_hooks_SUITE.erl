%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
% This file is provided to you under the Apache License,
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

-module(commit_hooks_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([register_hook_test/1,
         execute_hook_test/1,
         execute_post_hook_test/1,
         execute_prehooks_static_txn_test/1,
         execute_posthooks_static_txn_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(ADDRESS, "localhost").

-define(PORT, 10017).

init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Nodes = test_utils:pmap(fun(N) ->
                    test_utils:start_suite(N, Config)
            end, [dev1, dev2, dev3]),

    test_utils:connect_dcs(Nodes),
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.
    
end_per_testcase(_, _) ->
    ok.

all() -> [register_hook_test,
          execute_hook_test,
          execute_post_hook_test,
          execute_prehooks_static_txn_test,
          execute_posthooks_static_txn_test].

register_hook_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),

    Bucket = hook_bucket,
    Response=rpc:call(Node, antidote, register_pre_hook,
                    [Bucket, hooks_module, hooks_function]),
    ?assertMatch({error, _}, Response),
    
    ok=rpc:call(Node, antidote_hooks, register_post_hook,
                [Bucket, antidote_hooks, test_commit_hook]),
    Result1 = rpc:call(Node, antidote_hooks, get_hooks, 
                       [post_commit, Bucket]),
    ?assertMatch({antidote_hooks, test_commit_hook}, Result1),
    ok=rpc:call(Node, antidote, unregister_hook,
                [post_commit, Bucket]),
    Result2 = rpc:call(Node, antidote_hooks, get_hooks, 
                       [post_commit, Bucket]),
    ?assertMatch(undefined, Result2).

%% Test pre-commit hook
execute_hook_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bucket = test_bucket,
    ok = rpc:call(Node, antidote, register_pre_hook,
                  [Bucket, antidote_hooks, test_increment_hook]),
    
    Bound_object = {key, riak_dt_pncounter, Bucket},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    ok = rpc:call(Node, antidote, update_objects, [[{Bound_object, increment, 1}], TxId]),
    {ok, CT} = rpc:call(Node, antidote, commit_transaction, [TxId]),
    
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [CT, []]),
    Res = rpc:call(Node, antidote, read_objects, [[Bound_object], TxId2]),
    rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertMatch({ok, [2]}, Res).

%% Test post-commit hook
execute_post_hook_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bucket = test_bucket2,
    ok = rpc:call(Node, antidote, register_post_hook,
                  [Bucket, antidote_hooks, test_post_hook]),
    
    Bound_object = {key1, riak_dt_pncounter, Bucket},
    {ok, TxId} =  rpc:call(Node, antidote, start_transaction, [ignore, []]),
    ok = rpc:call(Node, antidote, update_objects, [[{Bound_object, increment, 1}], TxId]),
    {ok, CT} = rpc:call(Node, antidote, commit_transaction, [TxId]),

    CommitCount = {key1, riak_dt_pncounter, commitcount},
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [CT, []]),
    Res = rpc:call(Node, antidote, read_objects, [[CommitCount], TxId2]),
    rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertMatch({ok, [1]}, Res).

execute_prehooks_static_txn_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bucket = test_bucket3,
    ok = rpc:call(Node, antidote, register_pre_hook,
                  [Bucket, antidote_hooks, test_increment_hook]),
    
    Bound_object = {key3, riak_dt_pncounter, Bucket},
    {ok, CT} = rpc:call(Node, antidote, update_objects, [ignore, [], [{Bound_object, increment, 1}]]),
    
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [CT, []]),
    Res = rpc:call(Node, antidote, read_objects, [[Bound_object], TxId2]),
    rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertMatch({ok, [2]}, Res).

execute_posthooks_static_txn_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bucket = test_bucket4,
    ok = rpc:call(Node, antidote, register_post_hook,
                  [Bucket, antidote_hooks, test_post_hook]),
    
    Bound_object = {key4, riak_dt_pncounter, Bucket},
    {ok, CT} = rpc:call(Node, antidote, update_objects, [ignore, [], [{Bound_object, increment, 1}]]),
   
    CommitCount = {key4, riak_dt_pncounter, commitcount},
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [CT, []]),
    Res = rpc:call(Node, antidote, read_objects, [[CommitCount], TxId2]),
    rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertMatch({ok, [1]}, Res).
