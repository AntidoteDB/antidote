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

-module(multiple_dcs_repl_SUITE).

%% common_test callbacks
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([
    join_to_replicate/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ADDRESS, "localhost").
-define(PORT, 10017).
-define(BUCKET_BIN, term_to_binary(test_utils:bucket(multiple_dcs_repl_SUITE))).


init_per_suite(_Config) ->
    [].


end_per_suite(Config) ->
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.


all() -> [
    join_to_replicate
].

%% DC1 and DC3 are main DCs with all updates
%% DC2 is joining later empty
join_to_replicate(Config) ->
    NodeNames = [dcdev1, dcdev2, dcdev3],
    Nodes = test_utils:pmap(fun(Node) -> test_utils:start_node(Node, Config) end, NodeNames),
    [Node, ReplNode, Node2] = test_utils:unpack(Nodes),

    {ok, DescriptorRepl} = rpc:call(Node, antidote_dc_manager, get_connection_descriptor, []),
    {ok, DescriptorMain} = rpc:call(Node, antidote_dc_manager, get_connection_descriptor, []),
    {ok, DescriptorMain2} = rpc:call(Node2, antidote_dc_manager, get_connection_descriptor, []),

    %% dcdev1 == dcdev3
    ok = rpc:call(Node, antidote_dc_manager, subscribe_updates_from, [[DescriptorMain2]]),
    ok = rpc:call(Node2, antidote_dc_manager, subscribe_updates_from, [[DescriptorMain]]),
    Bucket = ?BUCKET_BIN,

    % write counter on dcdv1:
    Times = 300,
    ct:log("Starting ~p write operations and 1 read", [Times]),
    %% concurrent requests will throw {error, aborted}
%%    test_utils:pmap(fun(_) -> antidote_utils:increment_pn_counter(Node, append_key1, Bucket) end, lists:seq(0, Times)),
    [ antidote_utils:increment_pn_counter(Node, append_key1, Bucket) || _ <- lists:seq(1, Times - 1)],
    Clock = antidote_utils:increment_pn_counter(Node, append_key1, Bucket),
    {Val1, _} = antidote_utils:read_pn_counter(Node, append_key1, Bucket, Clock),
    Val1 = Times,


    %% subscribe to descriptors of other dcs
    ok = rpc:call(ReplNode, antidote_dc_manager, subscribe_updates_from, [[DescriptorMain]]),
    ok = rpc:call(ReplNode, antidote_dc_manager, subscribe_updates_from, [[DescriptorMain2]]),
    ok = rpc:call(Node, antidote_dc_manager, subscribe_updates_from, [[DescriptorRepl]]),
    ok = rpc:call(Node2, antidote_dc_manager, subscribe_updates_from, [[DescriptorRepl]]),

    %% read on repl node
    {Val1, _} = antidote_utils:read_pn_counter(ReplNode, append_key1, Bucket, Clock).
