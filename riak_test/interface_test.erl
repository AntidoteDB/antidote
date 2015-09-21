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


%% Tests for interface, specifically protocol buffer

-module(interface_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

-define(ADDRESS, "localhost").
-define(PORT, 10017).

confirm() ->
    rt:update_app_config(all,[
                              {riak_core, [{ring_creation_size, 8}]}
                             ]),
    N = 1,
    [Nodes] = rt:build_clusters([N]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),
    Node = hd(Nodes),
    rt:wait_for_service(Node, antidote),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(Node,fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    simple_transaction_test(hd(Nodes)),
    read_write_test(hd(Nodes)),
    pb_test_read(hd(Nodes)),
    pb_test_counter_read_write(hd(Nodes)),
    pb_test_set_read_write(hd(Nodes)),
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


%% Single object read
pb_test_read(_Node) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"key">>, riak_dt_pncounter, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    {ok, [_Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid).

%% Single object read and write
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
