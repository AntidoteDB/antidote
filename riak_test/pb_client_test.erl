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
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, 8}]}
    ]),
    [Nodes] = rt:build_clusters([1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),
    Node = hd(Nodes),
    rt:wait_for_service(Node, antidote),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(Node,fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    pass = start_stop_test(),
    pass = get_empty_crdt_test(<<"key0">>),
    pass = update_counter_crdt_test(<<"key1">>, 10),
    pass = update_counter_crdt_and_read_test(<<"key2">>, 15),
    pass = atomic_update_txn_test(),
    pass = snapshot_read_test(),
    pass = transaction_orset_test(),
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

atomic_update_txn_test() ->
    lager:info("Testing Atomic Updates"),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, C1} = antidotec_pb_socket:get_crdt(<<"counter1">>, riak_dt_pncounter, Pid),
    {ok, C2} = antidotec_pb_socket:get_crdt(<<"counter2">>, riak_dt_pncounter, Pid),
    C11 = antidotec_counter:increment(1, C1),
    C21 = antidotec_counter:increment(2, C2),
    lager:info("Testing.. "),
    Msg = riak_pb_messages:msg_code(fpbatomicupdatetxnreq),
    lager:info("Msg Code is ~p",[Msg]),
    Response = antidotec_pb_socket:atomic_store_crdts([C11,C21],Pid),
    antidotec_pb_socket:stop(Pid),
    ?assertMatch({ok,_},Response),

    %Read your writes
    pass = get_crdt_check_value(<<"counter1">>, riak_dt_pncounter, 1),
    pass = get_crdt_check_value(<<"counter2">>, riak_dt_pncounter, 2),
    pass.

snapshot_read_test() ->
    lager:info("Testing Snapshot Reads"),
    Key1 = <<"read1">>,
    Key2 = <<"read2">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, C1} = antidotec_pb_socket:get_crdt(Key1, riak_dt_pncounter, Pid),
    {ok, C2} = antidotec_pb_socket:get_crdt(Key2, riak_dt_pncounter, Pid),
    C11 = antidotec_counter:increment(1, C1),
    C21 = antidotec_counter:increment(2, C2),
    Response = antidotec_pb_socket:atomic_store_crdts([C11,C21],Pid),
    ?assertMatch({ok,_},Response),

    %Read your writes
    Result = antidotec_pb_socket:snapshot_get_crdts([{Key1, riak_dt_pncounter}, {Key2,riak_dt_pncounter}], Pid),
    {ok, _Clock, [Counter1, Counter2]} = Result,
    antidotec_pb_socket:stop(Pid),
    ?assertMatch(1, antidotec_counter:value(Counter1)),
    ?assertMatch(2, antidotec_counter:value(Counter2)),
    %%TODO; Use Clock in next write/read transactions
    pass.

transaction_orset_test() ->
    lager:info("Testing transaction using orset"),
    Key1 = <<"set1">>,
    Key2 = <<"count1">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, C1} = antidotec_pb_socket:get_crdt(Key1, riak_dt_orset, Pid),
    {ok, C2} = antidotec_pb_socket:get_crdt(Key2, riak_dt_pncounter, Pid),
    C11 = antidotec_set:add(1, C1),
    C12 = antidotec_set:add(2, C11),
    C13 = antidotec_set:add(3, C12),
    ?assertMatch(true, antidotec_set:contains(1,C13)),
    C21 = antidotec_counter:increment(2, C2),
    Response = antidotec_pb_socket:atomic_store_crdts([C13,C21],Pid),
    ?assertMatch({ok,_},Response),

    %Read your writes
    Result = antidotec_pb_socket:snapshot_get_crdts([{Key1, riak_dt_orset}, {Key2,riak_dt_pncounter}], Pid),
    {ok, _Clock, [Set1, Counter2]} = Result,
    antidotec_pb_socket:stop(Pid),
%%    ?assertMatch(["1","2","3"], antidotec_set:value(Set1)),
    ?assertMatch(2, antidotec_counter:value(Counter2)),
    ?assertMatch(true, antidotec_set:contains(1,Set1)),
    ?assertMatch(true, antidotec_set:contains(2,Set1)),
    ?assertMatch(true, antidotec_set:contains(3,Set1)),
    %%TODO; Use Clock in next write/read transactions
    pass.
    
