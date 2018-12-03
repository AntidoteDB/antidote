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
-export([ replication_test/1 ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(gr_bucket)).

init_per_suite(InitialConfig) ->
    Config = test_utils:init_multi_dc(?MODULE, InitialConfig),

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

all() -> [ replication_test ].


replication_test(Config) ->
    Bucket = ?BUCKET,
    [Node1, Node2 | _] = proplists:get_value(nodes, Config),

    O1 = {gr_repl_key1, antidote_crdt_counter_pn, Bucket},
    O2 = {gr_repl_key2, antidote_crdt_counter_pn, Bucket},

    %% Write to DC1
    {ok, _CT1} = rpc:call(Node1, antidote, update_objects, [ignore, [], [{O1, increment, 1}]]),
    %% Write to DC2
    {ok, CT2} = rpc:call(Node2, antidote, update_objects, [ignore, [], [{O2, increment, 1}]]),
    %% Read r1 from DC2, with dependency to first write
    {ok, [Res1], _} = rpc:call(Node2, antidote, read_objects, [ignore, [], [O1]]),
    ct:log("Read r1 from DC2: ~p", [Res1]), %% Result could be 0 or 1, there is no guarantee
    {ok, Res2, _} = rpc:call(Node2, antidote, read_objects, [CT2, {}, [O1, O2]]),
    %% Since CT1 < CT2, any snapshot that includes second write must include first write
    ?assertMatch([1, 1], Res2).
