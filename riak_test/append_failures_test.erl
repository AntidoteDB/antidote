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
-module(append_failures_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([6]),
    N = hd(Nodes),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    %% Identify preference list for a given key.
    Preflist = rpc:call(N, log_utilities, get_preflist_from_key, [key1]),
    lager:info("Preference list: ~p", [Preflist]),

    NodeList = [Node || {_Index, Node} <- Preflist],
    lager:info("Responsible nodes for key: ~p", [NodeList]),

    {A, _} = lists:split(1, NodeList),
    First = hd(A),

    %% Perform successful write and read.
    WriteResult = rpc:call(First,
                           floppy, append, [key1, riak_dt_gcounter, {increment, ucl}]),
    lager:info("WriteResult: ~p", [WriteResult]),
    ?assertMatch({ok, _}, WriteResult),

    ReadResult = rpc:call(First, floppy, read, [key1, riak_dt_gcounter]),
    lager:info("ReadResult: ~p", [ReadResult]),
    ?assertMatch({ok, 1}, ReadResult),

    %% Partition the network.
    lager:info("About to partition: ~p from: ~p", [A, Nodes -- A]),
    PartInfo = rt:partition(A, Nodes -- A),

    %% Write to the minority partition.
    WriteResult2 = rpc:call(First,
                            floppy, append, [key1, riak_dt_gcounter, {increment, ucl}]),
    lager:info("WriteResult2: ~p", [WriteResult2]),
    ?assertMatch({error, timeout}, WriteResult2),

    %% Read from the minority partition.
    ReadResult2 = rpc:call(First, floppy, read, [key1, riak_dt_gcounter]),
    lager:info("ReadResult2: ~p", [ReadResult2]),
    ?assertMatch({error, timeout}, ReadResult2),

    %% Heal the partition.
    rt:heal(PartInfo),

    %% Read after the partition has been healed.
    ReadResult3 = rpc:call(First, floppy, read, [key1, riak_dt_gcounter]),
    lager:info("ReadResult3: ~p", [ReadResult3]),
    ?assertMatch({ok, 1}, ReadResult3),

    pass.
