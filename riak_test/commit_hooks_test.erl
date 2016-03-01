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
-module(commit_hooks_test).

-include_lib("eunit/include/eunit.hrl").
-include("antidote.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

-export([confirm/0]).

-define(ADDRESS, "localhost").

-define(PORT, 10017).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
                              {riak_core, [{ring_creation_size, NumVNodes}]}
                             ]),
    [Nodes] = rt:build_clusters([3]),
    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Nodes),fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),
    lager:info("Nodes: ~p", [Nodes]),

    register_hook_test(Nodes),
    execute_hook_test(Nodes),
    execute_post_hook_test(Nodes),
    pass.

register_hook_test(Nodes) ->
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
execute_hook_test(Nodes) ->
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
execute_post_hook_test(Nodes) ->
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
    
