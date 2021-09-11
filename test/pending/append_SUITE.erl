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

-module(append_SUITE).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

%% tests
-export([
         append_test/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(append_bucket)).

init_per_suite(Config) ->
    test_utils:init_single_dc(?MODULE, Config).

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() ->
    [
     append_test
    ].

append_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    ct:log("Starting write operation 1"),
    antidote_utils:increment_pn_counter(Node, append_key1, Bucket),

    ct:log("Starting write operation 2"),
    antidote_utils:increment_pn_counter(Node, append_key2, Bucket),

    ct:log("Starting read operation 1"),
    {Val1, _} = antidote_utils:read_pn_counter(Node, append_key1, Bucket),
    ?assertEqual(1, Val1),

    ct:log("Starting read operation 2"),
    {Val2, _} = antidote_utils:read_pn_counter(Node, append_key2, Bucket),
    ?assertEqual(1, Val2).
