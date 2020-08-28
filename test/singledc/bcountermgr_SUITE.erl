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

-module(bcountermgr_SUITE).

%% common_test callbacks
-export([init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([new_bcounter_test/1,
    update_bcounter_test/1,
    invalid_bcounter_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(bcountermgr_bucket)). %%TODO Why?


init_per_suite(InitialConfig) ->
    Config = test_utils:init_single_dc(?MODULE, InitialConfig),
    Clusters = proplists:get_value(clusters, Config),
    Nodes = lists:flatten(Clusters),

    % Ensure that write operations are certified
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env, [antidote, txn_cert, true])
                    end, Nodes),

    % Check that indeed transactions certification is turned on
    {ok, true} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_cert]),

    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    new_bcounter_test,
    update_bcounter_test,
    invalid_bcounter_test
].

new_bcounter_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter1_mgr,
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0).

update_bcounter_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter2_mgr,
    {_, CommitTime1} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    {ok, CommitTime2} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime1, antidote_utils:bcounter_get_increment_op(Node, 5)),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, CommitTime2, 5).

invalid_bcounter_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter2_mgr,
    {_, CommitTime1} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    {error, no_permissions} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime1, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, CommitTime1, 0).
