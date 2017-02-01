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
-module(pb_client_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([start_stop_test/1,
  simple_transaction_test/1,
  read_write_test/1,
  get_empty_crdt_test/1,
  client_fail_test/1,
  client_fail_test2/1,
  pb_test_counter_read_write/1,
  pb_test_set_read_write/1,
  pb_empty_txn_clock_test/1,
  update_counter_crdt_test/1,
  update_counter_crdt_and_read_test/1,
  update_set_read_test/1,
  static_transaction_test/1,
  crdt_integer_test/1,
  update_reg_test/1, crdt_mvreg_test/1, crdt_set_rw_test/1, crdt_map_aw_test/1, crdt_gmap_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(ADDRESS, "localhost").

-define(PORT, 10017).

init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = hd(Clusters),
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [start_stop_test,
        simple_transaction_test,
        read_write_test,
        get_empty_crdt_test,
        client_fail_test,
        client_fail_test2,
        pb_test_counter_read_write,
        pb_test_set_read_write,
        pb_empty_txn_clock_test,
        update_counter_crdt_test,
        update_counter_crdt_and_read_test,
        update_set_read_test,
        static_transaction_test,
        crdt_integer_test,
        crdt_mvreg_test,
        crdt_set_rw_test,
        crdt_gmap_test,
        crdt_map_aw_test,
        update_reg_test].

start_stop_test(_Config) ->
    lager:info("Verifying pb connection..."),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(ok, Disconnected),
    pass.

%% starts and transaction and read a key
simple_transaction_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bound_object = {pb_client_SUITE_simple_transaction_test, antidote_crdt_counter, bucket},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [0]} = rpc:call(Node, antidote, read_objects, [[Bound_object], TxId]),
    rpc:call(Node, antidote, commit_transaction, [TxId]).


read_write_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    Bound_object = {pb_client_SUITE_read_write_test, antidote_crdt_counter, bucket},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [0]} = rpc:call(Node, antidote, read_objects, [[Bound_object], TxId]),
    ok = rpc:call(Node, antidote, update_objects, [[{Bound_object, increment, 1}], TxId]),
    rpc:call(Node, antidote, commit_transaction, [TxId]).


%% Single object rea
get_empty_crdt_test(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"pb_client_SUITE_get_empty_crdt_test">>, antidote_crdt_counter, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

client_fail_test(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"pb_client_SUITE_get_empty_crdt_test">>, antidote_crdt_counter, <<"bucket">>},
    {ok, _TxIdFail} = antidotec_pb:start_transaction(Pid, ignore, {}),
    % Client fails and starts next transaction:
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).


client_fail_test2(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"pb_client_SUITE_get_empty_crdt_test">>, antidote_crdt_counter, <<"bucket">>},
    {ok, _TxIdFail} = antidotec_pb:start_transaction(Pid, ignore, {}),
    % Client fails and starts next transaction:
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),

    {ok, TxId2} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId2),

    {ok, TxId3} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId3),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId3),

    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

pb_test_counter_read_write(_Config) ->
    Key = <<"pb_client_SUITE_pb_test_counter_read_write">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_counter, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, {}),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object, increment, 1}], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual(1, antidotec_counter:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).

pb_test_set_read_write(_Config) ->
    Key = <<"pb_client_SUITE_pb_test_set_read_write">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_orset, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, {}),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object, add, <<"a">>}], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual([<<"a">>],antidotec_set:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).

pb_empty_txn_clock_test(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, CommitTime} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, CommitTime, {}),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    _Disconnected = antidotec_pb_socket:stop(Pid).


update_counter_crdt_test(_Config) ->
    lager:info("Verifying retrieval of updated counter CRDT..."),
    Key = <<"pb_client_SUITE_update_counter_crdt_test">>,
    Bucket = <<"bucket">>,
    Amount = 10,
    update_counter_crdt(Key, Bucket, Amount).

update_counter_crdt(Key, Bucket, Amount) ->
    BObj = {Key, antidote_crdt_counter, Bucket},
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Obj = antidotec_counter:new(),
    Obj2 = antidotec_counter:increment(Amount, Obj),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, {}),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_counter:to_ops(BObj, Obj2),
                                     TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    pass.

update_counter_crdt_and_read_test(_Config) ->
    Key = <<"pb_client_SUITE_update_counter_crdt_and_read_test">>,
    Amount = 15,
    pass = update_counter_crdt(Key, <<"bucket">>, Amount),
    pass = get_crdt_check_value(Key, antidote_crdt_counter, <<"bucket">>, Amount).

get_crdt_check_value(Key, Type, Bucket, Expected) ->
    lager:info("Verifying value of updated CRDT..."),
    BoundObject = {Key, Type, Bucket},
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [BoundObject], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    Mod = antidotec_datatype:module_for_term(Val),
    ?assertEqual(Expected,Mod:value(Val)),
    pass.

update_set_read_test(_Config) ->
    Key = <<"pb_client_SUITE_update_set_read_test">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_orset, <<"bucket">>},
    Set = antidotec_set:new(),
    Set1 = antidotec_set:add(<<"a">>, Set),
    Set2 = antidotec_set:add(<<"b">>, Set1),

    {ok, TxId} = antidotec_pb:start_transaction(Pid,
                                                ignore, {}),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(Bound_object, Set2),
                                     TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual(2,length(antidotec_set:value(Val))),
    ?assertMatch(true, antidotec_set:contains(<<"a">>, Val)),
    ?assertMatch(true, antidotec_set:contains(<<"b">>, Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).

update_reg_test(_Config) ->
    Key = <<"pb_client_SUITE_update_reg_test">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_lwwreg, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid,
                                                ignore, {}),
    ok = antidotec_pb:update_objects(Pid,
                                     [{Bound_object, assign, <<"10">>}],
                                     TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual(<<"10">>, antidotec_reg:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).


crdt_integer_test(_Config) ->
    Key = <<"pb_client_SUITE_crdt_integer_test">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_integer, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, {}),
    ok = antidotec_pb:update_objects(Pid,
                                     [{Bound_object, set, 40}],
                                     TxId),
    ok = antidotec_pb:update_objects(Pid,
                                       [{Bound_object, increment, 2}],
                                       TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_values(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual({integer, 42}, Val),
    _Disconnected = antidotec_pb_socket:stop(Pid).


crdt_mvreg_test(_Config) ->
    Key = <<"pb_client_SUITE_crdt_mvreg_test">>,
    {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_mvreg, <<"bucket">>},
    {ok, Tx1} = antidotec_pb:start_transaction(Pid1, ignore, {}),
    ok = antidotec_pb:update_objects(Pid1, [{Bound_object, assign, <<"a">>}], Tx1),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx1),
    %% Read committed updated
    {ok, Tx3} = antidotec_pb:start_transaction(Pid1, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx3),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx3),
    ?assertEqual({mvreg, [<<"a">>]}, Val),
    _Disconnected = antidotec_pb_socket:stop(Pid1).


crdt_set_rw_test(_Config) ->
  Key = <<"pb_client_SUITE_crdt_set_rw_test">>,
  {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
  Bound_object = {Key, antidote_crdt_set_rw, <<"bucket">>},
  {ok, Tx1} = antidotec_pb:start_transaction(Pid1, ignore, {}),
  ok = antidotec_pb:update_objects(Pid1, [{Bound_object, add, <<"a">>}], Tx1),
  ok = antidotec_pb:update_objects(Pid1, [{Bound_object, add_all, [<<"b">>,<<"c">>,<<"d">>,<<"e">>,<<"f">>]}], Tx1),
  ok = antidotec_pb:update_objects(Pid1, [{Bound_object, remove, <<"b">>}], Tx1),
  ok = antidotec_pb:update_objects(Pid1, [{Bound_object, remove_all, [<<"c">>,<<"d">>]}], Tx1),
  {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx1),
  %% Read committed updated
  {ok, Tx3} = antidotec_pb:start_transaction(Pid1, ignore, {}),
  {ok, [Val]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx3),
  {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx3),
  ?assertEqual({set, [<<"a">>,<<"e">>,<<"f">>]}, Val),
  _Disconnected = antidotec_pb_socket:stop(Pid1).



crdt_gmap_test(_Config) ->
  Key = <<"pb_client_SUITE_crdt_map_aw_test">>,
  {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
  Bound_object = {Key, antidote_crdt_gmap, <<"bucket">>},
  {ok, Tx1} = antidotec_pb:start_transaction(Pid1, ignore, {}),
  ok = antidotec_pb:update_objects(Pid1, [
    {Bound_object, update, {{<<"a">>, antidote_crdt_integer}, {set, 42}}}], Tx1),
  ok = antidotec_pb:update_objects(Pid1, [
    {Bound_object, update, [
      {{<<"b">>, antidote_crdt_lwwreg}, {assign, <<"X">>}},
      {{<<"c">>, antidote_crdt_mvreg}, {assign, <<"Paul">>}},
      {{<<"d">>, antidote_crdt_orset}, {add_all, [<<"Apple">>, <<"Banana">>]}},
      {{<<"e">>, antidote_crdt_set_rw}, {add_all, [<<"Apple">>, <<"Banana">>]}},
      {{<<"f">>, antidote_crdt_counter}, {increment , 7}},
      {{<<"g">>, antidote_crdt_gmap}, {update, [
        {{<<"x">>, antidote_crdt_integer}, {set, 17}}
      ]}},
      {{<<"h">>, antidote_crdt_map_aw}, {update, [
        {{<<"x">>, antidote_crdt_integer}, {set, 15}}
      ]}}
    ]}], Tx1),
  {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx1),
  %% Read committed updated
  {ok, Tx3} = antidotec_pb:start_transaction(Pid1, ignore, {}),
  {ok, [Val]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx3),
  {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx3),
  ExpectedRes = {map, [
    {{<<"a">>, antidote_crdt_integer}, 42},
    {{<<"b">>, antidote_crdt_lwwreg}, <<"X">>},
    {{<<"c">>, antidote_crdt_mvreg}, [<<"Paul">>]},
    {{<<"d">>, antidote_crdt_orset}, [<<"Apple">>, <<"Banana">>]},
    {{<<"e">>, antidote_crdt_set_rw}, [<<"Apple">>, <<"Banana">>]},
    {{<<"f">>, antidote_crdt_counter}, 7},
    {{<<"g">>, antidote_crdt_gmap}, [
      {{<<"x">>, antidote_crdt_integer}, 17}
    ]},
    {{<<"h">>, antidote_crdt_map_aw}, [
      {{<<"x">>, antidote_crdt_integer}, 15}
    ]}
  ]},
  ?assertEqual(ExpectedRes, Val),
  _Disconnected = antidotec_pb_socket:stop(Pid1).

crdt_map_aw_test(_Config) ->
  Key = <<"pb_client_SUITE_crdt_map_aw_test">>,
  {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
  Bound_object = {Key, antidote_crdt_map_aw, <<"bucket">>},
  {ok, Tx1} = antidotec_pb:start_transaction(Pid1, ignore, {}),
  ok = antidotec_pb:update_objects(Pid1, [
    {Bound_object, update, {{<<"a">>, antidote_crdt_integer}, {set, 42}}}], Tx1),
  ok = antidotec_pb:update_objects(Pid1, [
    {Bound_object, update, [
      {{<<"b">>, antidote_crdt_lwwreg}, {assign, <<"X">>}},
      {{<<"b1">>, antidote_crdt_lwwreg}, {assign, <<"X1">>}},
      {{<<"b2">>, antidote_crdt_lwwreg}, {assign, <<"X2">>}},
      {{<<"b3">>, antidote_crdt_lwwreg}, {assign, <<"X3">>}},
      {{<<"b4">>, antidote_crdt_lwwreg}, {assign, <<"X4">>}},
      {{<<"b5">>, antidote_crdt_lwwreg}, {assign, <<"X5">>}},
      {{<<"c">>, antidote_crdt_mvreg}, {assign, <<"Paul">>}},
      {{<<"d">>, antidote_crdt_orset}, {add_all, [<<"Apple">>, <<"Banana">>]}},
      {{<<"e">>, antidote_crdt_set_rw}, {add_all, [<<"Apple">>, <<"Banana">>]}},
      {{<<"f">>, antidote_crdt_counter}, {increment , 7}},
      {{<<"g">>, antidote_crdt_gmap}, {update, [
        {{<<"x">>, antidote_crdt_integer}, {set, 17}}
      ]}},
      {{<<"h">>, antidote_crdt_map_aw}, {update, [
        {{<<"x">>, antidote_crdt_integer}, {set, 15}}
      ]}}
    ]}], Tx1),
  ok = antidotec_pb:update_objects(Pid1, [
    {Bound_object, remove, {<<"b1">>, antidote_crdt_lwwreg}}], Tx1),
  ok = antidotec_pb:update_objects(Pid1,
    [{Bound_object, remove, [
      {<<"b2">>, antidote_crdt_lwwreg},
      {<<"b3">>, antidote_crdt_lwwreg}]}
    ], Tx1),
  ok = antidotec_pb:update_objects(Pid1,
    [{Bound_object, batch,
      {[ % updates
        {{<<"i">>, antidote_crdt_lwwreg}, {assign, <<"X">>}}
      ],[ % removes
        {<<"b4">>, antidote_crdt_lwwreg},
        {<<"b5">>, antidote_crdt_lwwreg}
      ]}}
    ], Tx1),
  {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx1),
  %% Read committed updated
  {ok, Tx3} = antidotec_pb:start_transaction(Pid1, ignore, {}),
  {ok, [Val]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx3),
  {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx3),
  ExpectedRes = {map, [
    {{<<"a">>, antidote_crdt_integer}, 42},
    {{<<"b">>, antidote_crdt_lwwreg}, <<"X">>},
    {{<<"c">>, antidote_crdt_mvreg}, [<<"Paul">>]},
    {{<<"d">>, antidote_crdt_orset}, [<<"Apple">>, <<"Banana">>]},
    {{<<"e">>, antidote_crdt_set_rw}, [<<"Apple">>, <<"Banana">>]},
    {{<<"f">>, antidote_crdt_counter}, 7},
    {{<<"g">>, antidote_crdt_gmap}, [
      {{<<"x">>, antidote_crdt_integer}, 17}
    ]},
    {{<<"h">>, antidote_crdt_map_aw}, [
      {{<<"x">>, antidote_crdt_integer}, 15}
    ]},
    {{<<"i">>, antidote_crdt_lwwreg}, <<"X">>}
  ]},
  ?assertEqual(ExpectedRes, Val),
  _Disconnected = antidotec_pb_socket:stop(Pid1).


static_transaction_test(_Config) ->
    Key = <<"pb_client_SUITE_static_transaction_test">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_orset, <<"bucket">>},
    Set = antidotec_set:new(),
    Set1 = antidotec_set:add(<<"a">>, Set),
    Set2 = antidotec_set:add(<<"b">>, Set1),

    {ok, TxId} = antidotec_pb:start_transaction(Pid,
                                                ignore, [{static, true}]),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(Bound_object, Set2),
                                     TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, [{static, true}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual(2,length(antidotec_set:value(Val))),
    ?assertMatch(true, antidotec_set:contains(<<"a">>, Val)),
    ?assertMatch(true, antidotec_set:contains(<<"b">>, Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).
