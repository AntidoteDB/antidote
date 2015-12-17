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

%% This module tests gentlerain read,write and snapshot read operations

-module(gr_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include("antidote.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
                              {riak_core, [{ring_creation_size, NumVNodes}]},
                              {antidote, [{txn_prot, gr}]}
                             ]),
    [Nodes1, Nodes2] = rt:build_clusters([1,1]),
    
    {ok, Prot} = rpc:call(hd(Nodes1), application, get_env, [antidote, txn_prot]),
    ?assertMatch(gr, Prot),
    
    ok = common:setup_dc_manager([Nodes1, Nodes2], first_run),
    
    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes1),
    rt:wait_until_ring_converged(Nodes2),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Nodes1),fun wait_init:check_ready/1),
    rt:wait_until(hd(Nodes2),fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    lager:info("Nodes: ~p, ~p", [Nodes1, Nodes2]),

    %% Check whether heartbeats from all replicas has received
    %% After this stable snapshot vectorclock contain entry for all DCs
    %% This is required for correct functioning of the protocol
    %rt:wait_until(hd(Nodes1), fun wait_init:check_replication_complete/1),
    %rt:wait_until(hd(Nodes2), fun wait_init:check_replication_complete/1),
    %% Test read and write on single nodes
    read_write_test(Nodes1),
    read_multiple_test(Nodes1),

    %% Test Replication

    replication_test(hd(Nodes1), hd(Nodes2)),
    pass.

read_write_test(Nodes) ->
    lager:info("Single read write test"),
    Node = hd(Nodes),
    Bound_object = {key, riak_dt_pncounter, bucket},
    {ok, [0], _} = rpc:call(Node, antidote, read_objects, [ignore, {}, [Bound_object]]),
    {ok, _} = rpc:call(Node, antidote, update_objects, [ignore, {}, [{Bound_object, increment, 1}]]),    
    {ok, Res, _} = rpc:call(Node, antidote, read_objects, [ignore, {}, [Bound_object]]),
    ?assertMatch([1], Res).

read_multiple_test(Nodes) ->
    lager:info("Snapshot read"),
    Node = hd(Nodes),
    O1 = {o1, riak_dt_pncounter, bucket},
    {ok, _} = rpc:call(Node, antidote, update_objects, [ignore, {}, [{O1, increment, 1}]]),
    O2 = {o2, riak_dt_pncounter, bucket},
    {ok, CT} = rpc:call(Node, antidote, update_objects, [ignore, {}, [{O2, increment, 1}]]),
    {ok, Res, _} = rpc:call(Node, antidote, read_objects, [CT, {}, [O1,O2]]),
    ?assertMatch([1,1], Res).

replication_test(Node1, Node2) ->
    O1 = {r1, riak_dt_pncounter, bucket},
    O2 = {r2, riak_dt_pncounter, bucket},
    %% Write to DC1
    {ok, _CT1} = rpc:call(Node1, antidote, update_objects, [ignore, {}, [{O1, increment, 1}]]),
    %% Write to DC2
    {ok, CT2} = rpc:call(Node2, antidote, update_objects, [ignore, {}, [{O2, increment, 1}]]),
    %% Read r1 from DC2, with dependency to first write
    {ok, [Res1], _} = rpc:call(Node2, antidote, read_objects, [ignore, {}, [O1]]),
    lager:info("Read r1 from DC2: ~p", [Res1]), %% Result could be 0 or 1, there is no guarantee
    {ok, Res2, _} = rpc:call(Node2, antidote, read_objects, [CT2, {}, [O1, O2]]),
    %% Since CT1 < CT2, any snapshot that includes second write must include first write
    ?assertMatch([1,1], Res2).
