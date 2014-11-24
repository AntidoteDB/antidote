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

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    empty_set_test(Nodes),
    add_test(Nodes),
    remove_test(Nodes),
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
    Result1=rpc:call(FirstNode, antidote, append,
                    [Key, Type, {{remove, a}, ucl}]),
    ?assertMatch({error,{precondition,{not_present,a}}}, Result1),
    Result2=rpc:call(FirstNode, antidote, append,
                    [Key, Type, {{add, a}, ucl}]),
    ?assertMatch({ok, [a]}, Result2).


add_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Add test started"),
    Type = crdt_orset,
    Key = key_add,
    %%Add multiple key works
    Result0=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [{Key, Type, {{add, a}, ucl}}, {Key, Type, {{add, b}, ucl}}]),
    ?assertMatch({ok, [a,b]}, Result0),
    %%Add a key twice in a transaction only adds one
    Result1=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [{Key, Type, {{add, c}, ucl}}, {Key, Type, {{add, c}, ucl}}]),
    ?assertMatch({ok, [a,b,c]}, Result1),
    %%Add a key multiple time will not duplicate
    Result2=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [{Key, Type, {{add, a}, ucl}}]),
    ?assertMatch({ok, [a,b,c]}, Result2).


remove_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Remove started"),
    Type = crdt_orset,
    Key = key_remove,
    %%Remove a non-existent key will trigger error
    Result0=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [{Key, Type, {{remove, a}, ucl}}]),
    lager:info("Result0: ~w", [Result0]),
    ?assertMatch({error, _}, Result0),
    
    Result1=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [{Key, Type, {{add, a}, ucl}}, {Key, Type, {{add, b}, ucl}}]),
    ?assertMatch({ok, _}, Result1),
    Result2=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch([a,b], Result2),
    %% Remove an element works
    Result3=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [{Key, Type, {{remove, a}, ucl}}]),
    ?assertMatch({ok, _}, Result3),
    Result4=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch([b], Result4),
    %%Add back and remove all works
    Result5=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [{Key, Type, {{add, a}, ucl}}, {Key, Type, {{add, b}, ucl}}]),
    ?assertMatch({ok, _}, Result5),
    %%Remove all
    Result6=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [{Key, Type, {{remove, a}, ucl}}, {Key, Type, {{remove, b}, ucl}}]),
    ?assertMatch({ok, _}, Result6),
    Result7=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch([], Result7).
    


