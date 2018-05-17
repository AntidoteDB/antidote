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

%% This module tests gentlerain read, write and snapshot read operations

-module(gr_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([read_write_test/1,
         read_multiple_test/1,
         replication_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = lists:flatten(Clusters),
    %Ensure that the gentlerain protocol is used
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
        [antidote, txn_prot, gr]) end, Nodes),

    %Check that indeed gentlerain is running
    {ok, gr} = rpc:call(hd(Nodes), application, get_env, [antidote, txn_prot]),

    %% Check whether heartbeats from all replicas has received
    %% After this stable snapshot vectorclock contain entry for all DCs
    %% This is required for correct functioning of the protocol
    %rt:wait_until(hd(Nodes1), fun wait_init:check_replication_complete/1),
    %rt:wait_until(hd(Nodes2), fun wait_init:check_replication_complete/1),

    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [read_write_test,
          read_multiple_test,
          replication_test].

read_write_test(Config) ->
    lager:info("Single read write test"),
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bound_object = {gr_rw_key, antidote_crdt_counter_pn, bucket},
    {ok, [0], _} = rpc:call(Node, antidote, read_objects, [ignore, [], [Bound_object]]),
    {ok, _} = rpc:call(Node, antidote, update_objects, [ignore, [], [{Bound_object, increment, 1}]]),
    {ok, Res, _} = rpc:call(Node, antidote, read_objects, [ignore, [], [Bound_object]]),
    ?assertMatch([1], Res).

read_multiple_test(Config) ->
    lager:info("Snapshot read"),
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    O1 = {gr_read_mult_key1, antidote_crdt_counter_pn, bucket},
    {ok, _} = rpc:call(Node, antidote, update_objects, [ignore, [], [{O1, increment, 1}]]),
    O2 = {o2, antidote_crdt_counter_pn, bucket},
    {ok, CT} = rpc:call(Node, antidote, update_objects, [ignore, [], [{O2, increment, 1}]]),
    {ok, Res, _} = rpc:call(Node, antidote, read_objects, [CT, {}, [O1, O2]]),
    ?assertMatch([1, 1], Res).

replication_test(Config) ->
    lager:info("Replication Test"),
    [Node1, Node2 | _] = proplists:get_value(nodes, Config),

    O1 = {gr_repl_key1, antidote_crdt_counter_pn, bucket},
    O2 = {gr_repl_key2, antidote_crdt_counter_pn, bucket},
    %% Write to DC1
    {ok, _CT1} = rpc:call(Node1, antidote, update_objects, [ignore, [], [{O1, increment, 1}]]),
    %% Write to DC2
    {ok, CT2} = rpc:call(Node2, antidote, update_objects, [ignore, [], [{O2, increment, 1}]]),
    %% Read r1 from DC2, with dependency to first write
    {ok, [Res1], _} = rpc:call(Node2, antidote, read_objects, [ignore, [], [O1]]),
    lager:info("Read r1 from DC2: ~p", [Res1]), %% Result could be 0 or 1, there is no guarantee
    {ok, Res2, _} = rpc:call(Node2, antidote, read_objects, [CT2, {}, [O1, O2]]),
    %% Since CT1 < CT2, any snapshot that includes second write must include first write
    ?assertMatch([1, 1], Res2).
