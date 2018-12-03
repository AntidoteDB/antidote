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

%% This module tests gentlerain read, write and snapshot read operations

-module(gr_SUITE).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([
    read_write_test/1,
    read_multiple_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(gr_bucket)).

init_per_suite(InitialConfig) ->
    Config = test_utils:init_single_dc(?MODULE, InitialConfig),

    Nodes = proplists:get_value(nodes, Config),
    %Ensure that the gentlerain protocol is used
    test_utils:pmap(fun(Node) -> rpc:call(Node, application, set_env, [antidote, txn_prot, gr]) end, Nodes),

    %Check that indeed gentlerain is running
    {ok, gr} = rpc:call(hd(Nodes), application, get_env, [antidote, txn_prot]),

    %% Check whether heartbeats from all replicas has received
    %% After this stable snapshot vectorclock contain entry for all DCs
    %% This is required for correct functioning of the protocol
    %rt:wait_until(hd(Nodes1), fun wait_init:check_replication_complete/1),
    %rt:wait_until(hd(Nodes2), fun wait_init:check_replication_complete/1),

    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [ read_write_test, read_multiple_test ].

read_write_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    BoundObject = {gr_rw_key, antidote_crdt_counter_pn, Bucket},

    {ok, [0], _} = rpc:call(Node, antidote, read_objects, [ignore, [], [BoundObject]]),
    {ok, _} = rpc:call(Node, antidote, update_objects, [ignore, [], [{BoundObject, increment, 1}]]),
    {ok, Res, _} = rpc:call(Node, antidote, read_objects, [ignore, [], [BoundObject]]),
    ?assertMatch([1], Res).

read_multiple_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    BoundObject1 = {gr_read_mult_key1, antidote_crdt_counter_pn, Bucket},
    BoundObject2 = {o2, antidote_crdt_counter_pn, Bucket},

    {ok, _} = rpc:call(Node, antidote, update_objects, [ignore, [], [{BoundObject1, increment, 1}]]),
    {ok, CT} = rpc:call(Node, antidote, update_objects, [ignore, [], [{BoundObject2, increment, 1}]]),
    {ok, Res, _} = rpc:call(Node, antidote, read_objects, [CT, {}, [BoundObject1, BoundObject2]]),
    ?assertMatch([1, 1], Res).
