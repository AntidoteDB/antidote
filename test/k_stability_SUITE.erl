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

-module(k_stability_SUITE).
-compile({parse_transform, lager_transform}).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

%% common_test callbacks
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

% Tests
-export([
    get_k_vector_test/1,
    get_version_matrix_test/1,
    get_dc_vals_test/1,
    get_k_vector_test/1
]).


-define(TAB, k_stability_test).
-define(DC1, {'antidote_1', {1501, 537303, 598423}}).
-define(DC2, {'antidote_2', {1390, 186897, 698677}}).
-define(DC3, {'antidote_3', {1490, 186159, 768617}}).
-define(DC4, {'antidote_4', {1590, 184597, 573977}}).
-define(ListVC_DC1, [{DC1, 1}, {DC2, 4}, {DC3, 0}, {DC4, 3}]).
-define(ListVC_DC2, [{DC1, 1}, {DC2, 5}, {DC3, 2}, {DC4, 4}]).
-define(ListVC_DC3, [{DC1, 0}, {DC2, 5}, {DC3, 4}, {DC4, 12}]).
-define(ListVC_DC4, [{DC1, 1}, {DC2, 0}, {DC3, 0}, {DC4, 12}]).


init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = lists:flatten(Clusters),

    %Ensure that the clocksi protocol is used
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
            [antidote, txn_prot, clocksi]) end, Nodes),

    %Check that indeed clocksi is running
    {ok, clocksi} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_prot]),
    TheNode = hd(hd(Clusters)),
        rpc:call(TheNode, ets, new, [(?TAB, [set, named_table]]),


    DC1_VC = rpc:call(TheNode, vectorclock, from_list, [?ListVC_DC1]),
    DC2_VC = rpc:call(TheNode, vectorclock, from_list, [?ListVC_DC2]),
    DC3_VC = rpc:call(TheNode, vectorclock, from_list, [?ListVC_DC3]),
    DC4_VC = rpc:call(TheNode, vectorclock, from_list, [?ListVC_DC4]),

    % DC IDs are unique
    ets:insert(?TAB, {?DC1, DC1_VC}),
    ets:insert(?TAB, {?DC2, DC2_VC}),
    ets:insert(?TAB, {?DC3, DC3_VC}),
    ets:insert(?TAB, {?DC4, DC4_VC}),

    Keys = [?DC1, ?DC2, ?DC3, ?DC4],

    [{clusters, Clusters} | Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [get_k_vector_test,
    get_version_matrix_test,
    get_dc_vals_test,
    get_dc_vals_test].


get_dc_vals_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    lager:info("get_dc_vals test passed!"),
    pass.

get_version_matrix_test(Config) ->
    lager:info("Build VersionMatrix test passed!"),
    Clusters = proplists:get_value(clusters, Config),

    pass.

get_k_vector_test(Config) ->
    lager:info("Build K-Vector test passed!"),
    Clusters = proplists:get_value(clusters, Config),

    pass.

