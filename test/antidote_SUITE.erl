%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
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
%% @doc log_test: Test that perform NumWrites increments to the key:key1.
%%      Each increment is sent to a random node of the cluster.
%%      Test normal behavior of the logging layer
%%      Performs a read to the first node of the cluster to check whether all the
%%      increment operations where successfully applied.
%%  Variables:  N:  Number of nodes
%%              Nodes: List of the nodes that belong to the built cluster
%%

-module(antidote_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([dummy_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = hd(Clusters),
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    %application:stop(lager),
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() ->
    [
     dummy_test
    ].

dummy_test(Config) ->
  [Node1, Node2 | _Nodes] = proplists:get_value(nodes, Config),
  ct:print("Test on ~p!",[Node1]),
  %timer:sleep(10000),
  %application:set_env(antidote, txn_cert, true),
  %application:set_env(antidote, txn_prot, clocksi),
  Key = antidote_key,
  Type = antidote_crdt_counter,

  {ok,_} = rpc:call(Node1, antidote, append, [Key, Type, {increment, 1}]),
  {ok,_} = rpc:call(Node1, antidote, append, [Key, Type, {increment, 1}]),
  {ok,_} = rpc:call(Node2, antidote, append, [Key, Type, {increment, 1}]),

  % Propagation of updates
  F = fun() ->
    rpc:call(Node2, antidote, read, [Key, Type])
    end,
  Delay = 100,
  Retry = 360000 div Delay, %wait for max 1 min
  ok = test_utils:wait_until_result(F, {ok, 3}, Retry, Delay),

  ok.
