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

-module(commit_hooks_SUITE).

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

-define(BUCKET, test_utils:bucket(commit_hooks_bucket)).


init_per_suite(Config) ->
    test_utils:init_single_dc(?MODULE, Config).

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    register_hook_test,
    execute_hook_test,
    execute_post_hook_test,
    execute_prehooks_static_txn_test,
    execute_posthooks_static_txn_test
].


register_hook_test(Config) ->
    Node = proplists:get_value(node, Config),
    Bucket = ?BUCKET,

    Response = rpc:call(Node, antidote, register_pre_hook, [Bucket, hooks_module, hooks_function]),
    ?assertMatch({error, _}, Response),

    ok = rpc:call(Node, antidote_hooks, register_post_hook, [Bucket, antidote_hooks, test_commit_hook]),
    Result1 = rpc:call(Node, antidote_hooks, get_hooks, [post_commit, Bucket]),
    ?assertMatch({antidote_hooks, test_commit_hook}, Result1),

    ok = rpc:call(Node, antidote, unregister_hook, [post_commit, Bucket]),
    Result2 = rpc:call(Node, antidote_hooks, get_hooks, [post_commit, Bucket]),
    ?assertMatch(undefined, Result2).


%% Test pre-commit hook
execute_hook_test(Config) ->
    Node = proplists:get_value(node, Config),
    Bucket = ?BUCKET,
    BoundObject = {hook_key, antidote_crdt_counter_pn, Bucket},

    ok = rpc:call(Node, antidote, register_pre_hook, [Bucket, antidote_hooks, test_increment_hook]),

    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    ct:log("Txid ~p", [TxId]),
    ok = rpc:call(Node, antidote, update_objects, [[{BoundObject, increment, 1}], TxId]),
    {ok, CT} = rpc:call(Node, antidote, commit_transaction, [TxId]),

    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [CT, []]),
    Res = rpc:call(Node, antidote, read_objects, [[BoundObject], TxId2]),
    rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertMatch({ok, [2]}, Res).


%% Test post-commit hook
execute_post_hook_test(Config) ->
    Node = proplists:get_value(node, Config),
    Bucket = ?BUCKET,
    BoundObject = {post_hook_key, antidote_crdt_counter_pn, Bucket},

    ok = rpc:call(Node, antidote, register_post_hook, [Bucket, antidote_hooks, test_post_hook]),
    {ok, TxId} =  rpc:call(Node, antidote, start_transaction, [ignore, []]),
    ok = rpc:call(Node, antidote, update_objects, [[{BoundObject, increment, 1}], TxId]),
    {ok, CT} = rpc:call(Node, antidote, commit_transaction, [TxId]),

    CommitCount = {post_hook_key, antidote_crdt_counter_pn, commitcount},
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [CT, []]),
    Res = rpc:call(Node, antidote, read_objects, [[CommitCount], TxId2]),
    rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertMatch({ok, [1]}, Res).

execute_prehooks_static_txn_test(Config) ->
    Node = proplists:get_value(node, Config),
    Bucket = ?BUCKET,
    BoundObject = {prehook_key, antidote_crdt_counter_pn, Bucket},

    ok = rpc:call(Node, antidote, register_pre_hook, [Bucket, antidote_hooks, test_increment_hook]),

    {ok, CT} = rpc:call(Node, antidote, update_objects, [ignore, [], [{BoundObject, increment, 1}]]),

    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [CT, []]),
    Res = rpc:call(Node, antidote, read_objects, [[BoundObject], TxId2]),
    rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertMatch({ok, [2]}, Res).

execute_posthooks_static_txn_test(Config) ->
    Node = proplists:get_value(node, Config),
    Bucket = ?BUCKET,
    BoundObject = {posthook_static_key, antidote_crdt_counter_pn, Bucket},

    ok = rpc:call(Node, antidote, register_post_hook, [Bucket, antidote_hooks, test_post_hook]),

    {ok, CT} = rpc:call(Node, antidote, update_objects, [ignore, [], [{BoundObject, increment, 1}]]),

    CommitCount = {posthook_static_key, antidote_crdt_counter_pn, commitcount},
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [CT, []]),
    Res = rpc:call(Node, antidote, read_objects, [[CommitCount], TxId2]),
    rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertMatch({ok, [1]}, Res).
