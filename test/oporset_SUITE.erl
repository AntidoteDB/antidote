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
-module(oporset_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([empty_set_test/1,
         add_test/1,
         remove_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Nodes = test_utils:pmap(fun(N) ->
                    test_utils:start_suite(N, Config)
            end, [dev1]),

    test_utils:connect_dcs(Nodes),
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.
    
end_per_testcase(_, _) ->
    ok.

all() -> [empty_set_test,
          add_test,
          remove_test].

empty_set_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),    
    FirstNode = hd(Nodes),
    lager:info("Test1 started"),
    Type = crdt_orset,
    Key = key_empty,
    Result0=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, []}, Result0),
    Result3=rpc:call(FirstNode, antidote, append,
                    [Key, Type, {{add, a}, ucl}]),
    ?assertMatch({ok, _}, Result3),
    Result4=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a]}, Result4).


add_test(Config) ->
    Nodes = proplists:get_value(nodes, Config), 
    FirstNode = hd(Nodes),
    lager:info("Add test started"),
    Type = crdt_orset,
    Key = key_add,
    %%Add multiple key works
    Result0=rpc:call(FirstNode, antidote, clocksi_execute_int_tx,
                    [[{update, {Key, Type, {{add, a}, ucl}}}, {update, {Key, Type, {{add, b}, ucl}}}]]),
    ?assertMatch({ok, _}, Result0),
    Result1=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a,b]}, Result1),
    %%Add a key twice in a transaction only adds one
    Result2=rpc:call(FirstNode, antidote, clocksi_execute_int_tx,
                    [[{update, {Key, Type, {{add, c}, ucl}}}, {update, {Key, Type, {{add, c}, ucl}}}]]),
    ?assertMatch({ok, _}, Result2),
    Result3=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a,b,c]}, Result3),
    %%Add a key multiple time will not duplicate
    Result4=rpc:call(FirstNode, antidote, clocksi_execute_int_tx,
                    [[{update, {Key, Type, {{add, a}, ucl}}}]]),
    ?assertMatch({ok, _}, Result4),
    Result5=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a,b,c]}, Result5).


remove_test(Config) ->
    Nodes = proplists:get_value(nodes, Config), 
    FirstNode = hd(Nodes),
    lager:info("Remove started"),
    Type = crdt_orset,
    Key = key_remove,
    
    Result1=rpc:call(FirstNode, antidote, clocksi_execute_int_tx,
                    [[{update, {Key, Type, {{add, a}, ucl}}}, {update, {Key, Type, {{add, b}, ucl}}}]]),
    ?assertMatch({ok, _}, Result1),
    Result2=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a,b]}, Result2),
    %% Remove an element works
    Result3=rpc:call(FirstNode, antidote, clocksi_execute_int_tx,
                    [[{update, {Key, Type, {{remove, a}, ucl}}}]]),
    ?assertMatch({ok, _}, Result3),
    Result4=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [b]}, Result4),
    %%Add back and remove all works
    Result5=rpc:call(FirstNode, antidote, clocksi_execute_int_tx,
                    [[{update, {Key, Type, {{add, a}, ucl}}}, {update, {Key, Type, {{add, b}, ucl}}}]]),
    ?assertMatch({ok, _}, Result5),
    %%Remove all
    Result6=rpc:call(FirstNode, antidote, clocksi_execute_int_tx,
                    [[{update, {Key, Type, {{remove, a}, ucl}}}, {update, {Key, Type, {{remove, b}, ucl}}}]]),
    ?assertMatch({ok, _}, Result6),
    Result7=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, []}, Result7).
