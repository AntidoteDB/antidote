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

-module(oprga_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([empty_test/1,
         add_test/1,
         remove_test/1,
         insert_after_remove_test/1,
         concurrency_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = hd(Clusters),
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [empty_test,
          add_test,
          remove_test,
          insert_after_remove_test,
          concurrency_test].

empty_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Empty test started"),
    Type = crdt_rga,
    Key = key_empty,
    Result0 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, []}, Result0),
    lager:info("Empty test OK").

add_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Add test started"),
    Type = crdt_rga,
    Key = key_add,
    Result0 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{addRight, a, 0}, ucl}}}]]),
    ?assertMatch({ok, _}, Result0),
    Result1 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}]}, Result1),
    Result2 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{addRight, b, 1}, ucl}}}]]),
    ?assertMatch({ok, _}, Result2),
    Result3 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}, {ok, b, _}]}, Result3),
    Result4 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{addRight, c, 2}, ucl}}}]]),
    ?assertMatch({ok, _}, Result4),
    Result5 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}, {ok, b, _}, {ok, c, _}]}, Result5),
    lager:info("Add test OK").

remove_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Remove test started"),
    Type = crdt_rga,
    Key = key_remove,
    Result0 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{addRight, a, 0}, ucl}}},
            {update, {Key, Type, {{addRight, b, 1}, ucl}}},
            {update, {Key, Type, {{addRight, c, 2}, ucl}}}]]),
    ?assertMatch({ok, _}, Result0),
    Result1 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}, {ok, b, _}, {ok, c, _}]}, Result1),
    Result2 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{remove, 2}, ucl}}}]]),
    ?assertMatch({ok, _}, Result2),
    Result3 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}, {deleted, b, _}, {ok, c, _}]}, Result3),
    Result4 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{remove, 1}, ucl}}}, {update, {Key, Type, {{remove, 3}, ucl}}}]]),
    ?assertMatch({ok, _}, Result4),
    Result5 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{deleted, a, _}, {deleted, b, _}, {deleted, c, _}]}, Result5),
    lager:info("Remove test OK").

insert_after_remove_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    FirstNode = hd(Nodes),
    lager:info("Remove-Insert test started"),
    Type = crdt_rga,
    Key = key_remove_insert,
    Result0 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{addRight, a, 0}, ucl}}},
            {update, {Key, Type, {{addRight, b, 1}, ucl}}},
            {update, {Key, Type, {{addRight, c, 2}, ucl}}}]]),
    ?assertMatch({ok, _}, Result0),
    Result1 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{remove, 3}, ucl}}}]]),
    ?assertMatch({ok, _}, Result1),
    Result2 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{addRight, d, 3}, ucl}}}]]),
    ?assertMatch({ok, _}, Result2),
    Result3 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}, {ok, b, _}, {deleted, c, _}, {ok, d, _}]}, Result3),
    lager:info("Remove-Insert test OK").

concurrency_test(Config) ->
    [FirstNode, SecondNode | _Nodes] = proplists:get_value(nodes, Config),
    lager:info("Concurrency test started"),
    Type = crdt_rga,
    Key = key_concurrency,

    %% insert in both nodes, an element in the beginning
    Result1 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{addRight, a, 0}, ucl}}}]]),
    ?assertMatch({ok, _}, Result1),

    Result2 = rpc:call(SecondNode, antidote, clocksi_execute_tx,
        [[{update, {Key, Type, {{addRight, b, 0}, ucl}}}]]),
    ?assertMatch({ok, _}, Result2),

    F = fun() -> rpc:call(SecondNode, antidote, read, [Key, Type]) end,
    Delay = 100,
    Retry = 36000 div Delay, %wait for max 1 min
    ok = wait_for_result_concurrency_test(F, Retry, Delay),

    %% the result should have first, element b, and then element a,
    %% because node2 > node1
    lager:info("Concurrency test OK").

wait_for_result_concurrency_test(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res  of
        {ok, [{ok, b, _}, {ok, a, _}]} ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_for_result_concurrency_test(Fun, Retry-1, Delay)
    end.
