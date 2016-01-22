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
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
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

    [Nodes1] = common:clean_clusters([Nodes]),
    rt:wait_for_service(Node, antidote),
    simple_transaction_test(hd(Nodes)),

    [Nodes2] = common:clean_clusters([Nodes1]),
    rt:wait_for_service(Node, antidote),
    read_write_test(hd(Nodes)),

    [Nodes3] = common:clean_clusters([Nodes2]),
    rt:wait_for_service(Node, antidote),
    get_empty_crdt_test(),

    [Nodes4] = common:clean_clusters([Nodes3]),
    rt:wait_for_service(Node, antidote),
    pb_test_counter_read_write(hd(Nodes)),

    [Nodes5] = common:clean_clusters([Nodes4]),
    rt:wait_for_service(Node, antidote),
    pb_test_set_read_write(hd(Nodes)),

    [Nodes6] = common:clean_clusters([Nodes5]),
    rt:wait_for_service(Node, antidote),
    pb_empty_txn_clock_test(),

    [Nodes7] = common:clean_clusters([Nodes6]),
    rt:wait_for_service(Node, antidote),
    pass = update_counter_crdt_test(<<"key1">>,<<"bucket">>, 10),

    [Nodes8] = common:clean_clusters([Nodes7]),
    rt:wait_for_service(Node, antidote),
    pass = update_counter_crdt_and_read_test(<<"key2">>, 15),
    [Nodes9] = common:clean_clusters([Nodes8]),
    rt:wait_for_service(Node, antidote),
    update_set_read_test(),
    [_] = common:clean_clusters([Nodes9]),
    rt:wait_for_service(Node, antidote),
    static_transaction_test(),
    pass.

start_stop_test() ->
    lager:info("Verifying pb connection..."),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(ok, Disconnected),
    pass.


%% starts and transaction and read a key
simple_transaction_test(Node) ->
    Bound_object = {key, riak_dt_pncounter, bucket},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [0]} = rpc:call(Node, antidote, read_objects, [[Bound_object], TxId]),
    rpc:call(Node, antidote, finish_transaction, [TxId]).


read_write_test(Node) ->
    Bound_object = {key, riak_dt_pncounter, bucket},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [0]} = rpc:call(Node, antidote, read_objects, [[Bound_object], TxId]),
    ok = rpc:call(Node, antidote, update_objects, [[{Bound_object, increment, 1}], TxId]),
    rpc:call(Node, antidote, finish_transaction, [TxId]).


%% Single object rea
get_empty_crdt_test() ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"key">>, riak_dt_pncounter, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

pb_test_counter_read_write(_Node) ->
    Key = <<"key_read_write">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, riak_dt_pncounter, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object, increment, 1}], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual(1, antidotec_counter:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).

pb_test_set_read_write(_Node) ->
    Key = <<"key_read_write_set">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, riak_dt_orset, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object, add, "a"}], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual(["a"],antidotec_set:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).

pb_empty_txn_clock_test() ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    {ok, CommitTime} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, CommitTime, {}),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    _Disconnected = antidotec_pb_socket:stop(Pid).


update_counter_crdt_test(Key, Bucket,  Amount) ->
    lager:info("Verifying retrieval of updated counter CRDT..."),
    BObj = {Key, riak_dt_pncounter, Bucket},
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Obj = antidotec_counter:new(),
    Obj2 = antidotec_counter:increment(Amount, Obj),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_counter:to_ops(BObj, Obj2),
                                     TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    pass.

update_counter_crdt_and_read_test(Key, Amount) ->
    pass = update_counter_crdt_test(Key, <<"bucket">>, Amount),
    pass = get_crdt_check_value(Key, riak_dt_pncounter, <<"bucket">>, Amount).

get_crdt_check_value(Key, Type, Bucket, Expected) ->
    lager:info("Verifying value of updated CRDT..."),
    BoundObject = {Key, Type, Bucket},
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [BoundObject], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    Mod = antidotec_datatype:module_for_term(Val),
    ?assertEqual(Expected,Mod:value(Val)),
    pass.

update_set_read_test() ->
    Key = <<"key_update_read_set">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, riak_dt_orset, <<"bucket">>},
    Set = antidotec_set:new(),
    Set1 = antidotec_set:add("a", Set),
    Set2 = antidotec_set:add("b", Set1),

    {ok, TxId} = antidotec_pb:start_transaction(Pid,
                                                term_to_binary(ignore), {}),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(Bound_object, Set2),
                                     TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual(2,length(antidotec_set:value(Val))),
    ?assertMatch(true, antidotec_set:contains("a", Val)),
    ?assertMatch(true, antidotec_set:contains("b", Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).

static_transaction_test() ->
    Key = <<"key_static_update_read_set">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, riak_dt_orset, <<"bucket">>},
    Set = antidotec_set:new(),
    Set1 = antidotec_set:add("a", Set),
    Set2 = antidotec_set:add("b", Set1),

    {ok, TxId} = antidotec_pb:start_transaction(Pid,
                                                term_to_binary(ignore), [{static, true}]),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(Bound_object, Set2),
                                     TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), [{static, true}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual(2,length(antidotec_set:value(Val))),
    ?assertMatch(true, antidotec_set:contains("a", Val)),
    ?assertMatch(true, antidotec_set:contains("b", Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).
    
