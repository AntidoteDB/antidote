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
-module(read_from_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    Node = hd(Nodes),

    WriteResult = rpc:call(Node,
                           floppy_rep_vnode, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),
   
    WriteResult2 = rpc:call(Node,
                            floppy_rep_vnode, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),
    {ok, OpId2} = WriteResult2,

    WriteResult3 = rpc:call(Node,
                            floppy_rep_vnode, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),
    {ok, OpId3} = WriteResult3,


    WriteResult4 = rpc:call(Node,
                            floppy_rep_vnode, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),
    {ok, OpId4} = WriteResult4,

    ReadResult1 = rpc:call(Node,
                           floppy_rep_vnode, read_from,
                           [key1, riak_dt_gcounter, OpId2]),
    ?assertMatch({ok, _}, ReadResult1),
    {ok, Ops1} = ReadResult1,
    ?assertEqual(length(Ops1), 2),

    ReadResult2 = rpc:call(Node,
                           floppy_rep_vnode, read_from,
                           [key1, riak_dt_gcounter, OpId4]),
    ?assertMatch({ok, _}, ReadResult2),
    {ok, Ops2} = ReadResult2,
    ?assertEqual(length(Ops2), 0),

    ReadResult3 = rpc:call(Node,
                           floppy_rep_vnode, read_from,
                           [key1, riak_dt_gcounter, OpId3]),
    ?assertMatch({ok, _}, ReadResult3),
    {ok, Ops3} = ReadResult3,
    ?assertEqual(length(Ops3), 1),

    pass.
