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
-module(append_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    [Nodes] = rt:build_clusters([3]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    append_test(Nodes),
    append_failure_test(Nodes),
    pass.

append_test(Nodes) ->
    Node = hd(Nodes),
    rt:wait_for_service(Node, antidote),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(Node,fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    rt:log_to_nodes(Nodes, "Starting write operation 1"),

    WriteResult = rpc:call(Node,
                           antidote, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),

    rt:log_to_nodes(Nodes, "Starting write operation 2"),

    WriteResult2 = rpc:call(Node,
                           antidote, append,
                           [key2, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),

    rt:log_to_nodes(Nodes, "Starting read operation 1"),

    ReadResult1 = rpc:call(Node,
                           antidote, read,
                           [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 1}, ReadResult1),

    rt:log_to_nodes(Nodes, "Starting read operation 2"),

    ReadResult2 = rpc:call(Node,
                           antidote, read,
                           [key2, riak_dt_gcounter]),
    ?assertEqual({ok, 1}, ReadResult2).

append_failure_test(Nodes) ->
    N = hd(Nodes),
    Key = append_failure,

    %% Identify preference list for a given key.
    Preflist = rpc:call(N, log_utilities, get_preflist_from_key, [Key]),
    lager:info("Preference list: ~p", [Preflist]),

    NodeList = [Node || {_Index, Node} <- Preflist],
    lager:info("Responsible nodes for key: ~p", [NodeList]),

    {A, _} = lists:split(1, NodeList),
    First = hd(A),

    %% Perform successful write and read.
    WriteResult = rpc:call(First,
                           antidote, append, [Key, riak_dt_gcounter, {increment, ucl}]),
    lager:info("WriteResult: ~p", [WriteResult]),
    ?assertMatch({ok, _}, WriteResult),

    ReadResult = rpc:call(First, antidote, read, [Key, riak_dt_gcounter]),
    lager:info("ReadResult: ~p", [ReadResult]),
    ?assertMatch({ok, 1}, ReadResult),

    %% Partition the network.
    lager:info("About to partition: ~p from: ~p", [A, Nodes -- A]),
    PartInfo = rt:partition(A, Nodes -- A),

    %% Heal the partition.
    rt:heal(PartInfo),

    %% Read after the partition has been healed.
    ReadResult3 = rpc:call(First, antidote, read, [Key, riak_dt_gcounter]),
    lager:info("ReadResult3: ~p", [ReadResult3]),
    ?assertMatch({ok, 1}, ReadResult3).
