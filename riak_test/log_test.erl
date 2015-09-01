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
%% @doc log_test: Test that perform NumWrites increments to the key:abc.
%%      Each increment is sent to a random node of the cluster.
%%      Test norml behaviour of the logging layer
%%      Perflorms a read to the first node of the cluster to check whether all the
%%      increment operations where successfully applied.
%%  Input:  N:  Number of nodes
%%          Nodes: List of the nodes that belong to the built cluster.
%%

-module(log_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, 8}]}
    ]),
    N = 6,
    [Nodes] = rt:build_clusters([N]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Nodes),fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),


    NumWrites = 120,
    ListIds = [random:uniform(N) || _ <- lists:seq(1, NumWrites)],

    F = fun(Elem, Acc) ->
            rt:log_to_nodes(Nodes, "Issuing write operation: ~p", [Acc]),
            Node = lists:nth(Elem, Nodes),
            lager:info("Sending append to Node ~w~n",[Node]),
            WriteResult = rpc:call(Node,
                                   antidote, append, [abc, riak_dt_gcounter, {increment, a}]),
            ?assertMatch({ok, _}, WriteResult),
            Acc + 1
    end,

    Total = lists:foldl(F, 0, ListIds),

    rt:log_to_nodes(Nodes, "Issuing read operation."),
    FirstNode = hd(Nodes),
    ReadResult = rpc:call(FirstNode,
                          antidote, read, [abc, riak_dt_gcounter]),
    lager:info("Read value: ~p", [ReadResult]),
    ?assertEqual({ok, Total}, ReadResult),

    pass.
