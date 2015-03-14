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
-module(pb_client_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

-define(ADDRESS, "localhost").

-define(PORT, 10017).

confirm() ->
    [Nodes] = rt:build_clusters([1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),
    Node = hd(Nodes),
    rt:wait_for_service(Node, antidote),

    pass = start_stop_test(),
    pass = get_empty_crdt_test(<<"key0">>),
    pass = update_counter_crdt_test(<<"key1">>, 10),
    pass = update_counter_crdt_and_read_test(<<"key2">>, 15),

    pass.

start_stop_test() ->
    lager:info("Verifying pb connection..."),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(ok, Disconnected),
    pass.

get_empty_crdt_test(Key) ->
    lager:info("Verifying retrieval of empty CRDT..."),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Obj} = antidotec_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Obj)),
    pass.

update_counter_crdt_test(Key, Amount) ->
    lager:info("Verifying retrieval of updated counter CRDT..."),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Obj} = antidotec_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid),
    Obj2 = antidotec_counter:increment(Amount, Obj),
    Result = antidotec_pb_socket:store_crdt(Obj2, Pid),
     _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(ok, Result),
    pass.

update_counter_crdt_and_read_test(Key, Amount) ->
    pass = update_counter_crdt_test(Key, Amount),
    pass = get_crdt_check_value(Key, riak_dt_pncounter, Amount).

get_crdt_check_value(Key, Type, Expected) ->
    lager:info("Verifying value of updated CRDT..."),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Obj} = antidotec_pb_socket:get_crdt(Key, Type, Pid),
    Mod = antidotec_datatype:module_for_type(Type),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(Expected, Mod:value(Obj)),
    pass.
