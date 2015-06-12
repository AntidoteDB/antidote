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
-module(oporset_test).

-export([confirm/0, concurrency_test/1]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, 8}]}
    ]),
    [Nodes] = rt:build_clusters([3]),
    rt:wait_until_ring_converged(Nodes),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Nodes),fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    lager:info("Nodes: ~p", [Nodes]),
    empty_set_test(Nodes),
    add_test(Nodes),
    remove_test(Nodes),
    %%concurrency_test(Nodes),
    rt:clean_cluster(Nodes),
    pass.


empty_set_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test1 started"),
    Type = crdt_orset,
    Key = key_empty,
    Result0=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, []}, Result0),
    %%Result1=rpc:call(FirstNode, antidote, append,
    %%                [Key, Type, {{remove, a}, ucl}]),
    %%?assertMatch({ok, _}, Result1),
    %%Result2=rpc:call(FirstNode, antidote, read,
    %%                [Key, Type]),
    %%?assertMatch({error,{precondition,{not_present,a}}}, Result2),
    Result3=rpc:call(FirstNode, antidote, append,
                    [Key, Type, {{add, a}, ucl}]),
    ?assertMatch({ok, _}, Result3),
    Result4=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a]}, Result4).


add_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Add test started"),
    Type = crdt_orset,
    Key = key_add,
    %%Add multiple key works
    Result0=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[{update, Key, Type, {{add, a}, ucl}}, {update, Key, Type, {{add, b}, ucl}}]]),
    ?assertMatch({ok, _}, Result0),
    Result1=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a,b]}, Result1),
    %%Add a key twice in a transaction only adds one
    Result2=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[{update, Key, Type, {{add, c}, ucl}}, {update, Key, Type, {{add, c}, ucl}}]]),
    ?assertMatch({ok, _}, Result2),
    Result3=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a,b,c]}, Result3),
    %%Add a key multiple time will not duplicate
    Result4=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[{update, Key, Type, {{add, a}, ucl}}]]),
    ?assertMatch({ok, _}, Result4),
    Result5=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a,b,c]}, Result5).


remove_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Remove started"),
    Type = crdt_orset,
    Key = key_remove,
    %%Remove a non-existent key will trigger error
    %Result0=rpc:call(FirstNode, antidote, clocksi_execute_tx,
    %                [{update, Key, Type, {{remove, a}, ucl}}]),
    %lager:info("Result0: ~w", [Result0]),
    %?assertMatch({error, _}, Result0),
    
    Result1=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[{update, Key, Type, {{add, a}, ucl}}, {update, Key, Type, {{add, b}, ucl}}]]),
    ?assertMatch({ok, _}, Result1),
    Result2=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a,b]}, Result2),
    %% Remove an element works
    Result3=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[{update, Key, Type, {{remove, a}, ucl}}]]),
    ?assertMatch({ok, _}, Result3),
    Result4=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [b]}, Result4),
    %%Add back and remove all works
    Result5=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[{update, Key, Type, {{add, a}, ucl}}, {update, Key, Type, {{add, b}, ucl}}]]),
    ?assertMatch({ok, _}, Result5),
    %%Remove all
    Result6=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[{update, Key, Type, {{remove, a}, ucl}}, {update, Key, Type, {{remove, b}, ucl}}]]),
    ?assertMatch({ok, _}, Result6),
    Result7=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, []}, Result7).
    

concurrency_test(Nodes) ->
    FirstNode = hd(Nodes),
    SecondNode = lists:nth(2, Nodes),
    lager:info("Concurrency test started"),
    Type = crdt_orset,
    Key = key_concurrency,
    
    Result1=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[{update, Key, Type, {{add, a}, ucl}}]]),
    ?assertMatch({ok, _}, Result1),

    Result2=rpc:call(SecondNode, antidote, clocksi_execute_tx,
                    [[{update, Key, Type, {{add, a}, no_user}}]]),
    ?assertMatch({ok, _}, Result2),

    Result3=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[{update, Key, Type, {{remove, a}, ucl}}]]),
    ?assertMatch({ok, _}, Result3),

    Result4=rpc:call(SecondNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [a]}, Result4).
        



