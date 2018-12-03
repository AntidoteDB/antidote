%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(pb_client_SUITE).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([
    start_stop_test/1,
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
    update_reg_test/1,
    crdt_mvreg_test/1,
    crdt_set_rw_test/1,
    crdt_gmap_test/1,
    crdt_map_rr_test/1,
    crdt_flag_tests/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ADDRESS, "localhost").
-define(PORT, 10017).
-define(BUCKET_BIN, term_to_binary(test_utils:bucket(pb_client_bucket))).


init_per_suite(Config) ->
    test_utils:init_single_dc(?MODULE, Config).


end_per_suite(Config) ->
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.


all() -> [
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
    crdt_mvreg_test,
    crdt_set_rw_test,
    crdt_gmap_test,
    update_reg_test,
    crdt_map_rr_test,
    crdt_flag_tests
].


start_stop_test(_Config) ->
    ct:log("Verifying pb connection..."),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(ok, Disconnected),
    pass.


%% Single object rea
get_empty_crdt_test(_Config) ->
    Bucket = ?BUCKET_BIN,
    Bound_object = {<<"key1">>, antidote_crdt_counter_pn, Bucket},

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, []),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),

    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).


client_fail_test(_Config) ->
    Bucket = ?BUCKET_BIN,
    Bound_object = {<<"key2">>, antidote_crdt_counter_pn, Bucket},

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, _TxIdFail} = antidotec_pb:start_transaction(Pid, ignore, []),

    % Client fails and starts next transaction:
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, []),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),

    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).


client_fail_test2(_Config) ->
    Bucket = ?BUCKET_BIN,
    Bound_object = {<<"key3">>, antidote_crdt_counter_pn, Bucket},

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, _TxIdFail} = antidotec_pb:start_transaction(Pid, ignore, []),

    % Client fails and starts next transaction:
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, []),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),

    {ok, TxId2} = antidotec_pb:start_transaction(Pid, ignore, []),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId2),

    {ok, TxId3} = antidotec_pb:start_transaction(Pid, ignore, []),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId3),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId3),

    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).


pb_test_counter_read_write(_Config) ->
    Bucket = ?BUCKET_BIN,
    Key = <<"pb_client_SUITE_pb_test_counter_read_write">>,
    Bound_object = {Key, antidote_crdt_counter_pn, Bucket},

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, []),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object, increment, 1}], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),

    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, []),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),

    ?assertEqual(1, antidotec_counter:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).


pb_test_set_read_write(_Config) ->
    Bucket = ?BUCKET_BIN,
    Key = <<"pb_client_SUITE_pb_test_set_read_write">>,
    Bound_object = {Key, antidote_crdt_set_aw, Bucket},

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, []),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object, add, <<"a">>}], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),

    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), []),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),

    ?assertEqual([<<"a">>], antidotec_set:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).


pb_empty_txn_clock_test(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, []),
    {ok, CommitTime} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, CommitTime, []),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    _Disconnected = antidotec_pb_socket:stop(Pid).


update_counter_crdt_test(_Config) ->
    ct:log("Verifying retrieval of updated counter CRDT..."),
    Key = <<"pb_client_SUITE_update_counter_crdt_test">>,
    Bucket = ?BUCKET_BIN,
    Amount = 10,
    update_counter_crdt(Key, Bucket, Amount).


update_counter_crdt(Key, Bucket, Amount) ->
    BObj = {Key, antidote_crdt_counter_pn, Bucket},
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Obj = antidotec_counter:new(),
    Obj2 = antidotec_counter:increment(Amount, Obj),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, []),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_counter:to_ops(BObj, Obj2),
                                     TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    pass.


update_counter_crdt_and_read_test(_Config) ->
    Bucket = ?BUCKET_BIN,
    Key = <<"pb_client_SUITE_update_counter_crdt_and_read_test">>,
    Amount = 15,

    pass = update_counter_crdt(Key, Bucket, Amount),
    pass = get_crdt_check_value(Key, antidote_crdt_counter_pn, Bucket, Amount).


get_crdt_check_value(Key, Type, Bucket, Expected) ->
    ct:log("Verifying value of updated CRDT..."),
    BoundObject = {Key, Type, Bucket},
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, []),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [BoundObject], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    Mod = antidotec_datatype:module_for_term(Val),
    ?assertEqual(Expected, Mod:value(Val)),
    pass.


update_set_read_test(_Config) ->
    Bucket = ?BUCKET_BIN,
    Key = <<"pb_client_SUITE_update_set_read_test">>,
    Bound_object = {Key, antidote_crdt_set_aw, Bucket},

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Set = antidotec_set:new(),
    Set1 = antidotec_set:add(<<"a">>, Set),
    Set2 = antidotec_set:add(<<"b">>, Set1),

    {ok, TxId} = antidotec_pb:start_transaction(Pid,
                                                ignore, []),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(Bound_object, Set2),
                                     TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, []),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual(2, length(antidotec_set:value(Val))),
    ?assertMatch(true, antidotec_set:contains(<<"a">>, Val)),
    ?assertMatch(true, antidotec_set:contains(<<"b">>, Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).


update_reg_test(_Config) ->
    Bucket = ?BUCKET_BIN,
    Key = <<"pb_client_SUITE_update_reg_test">>,
    Bound_object = {Key, antidote_crdt_register_lww, Bucket},

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, TxId} = antidotec_pb:start_transaction(Pid,
                                                ignore, []),
    ok = antidotec_pb:update_objects(Pid,
                                     [{Bound_object, assign, <<"10">>}],
                                     TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, []),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    ?assertEqual(<<"10">>, antidotec_reg:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).


crdt_mvreg_test(_Config) ->
    Bucket = ?BUCKET_BIN,
    Key = <<"pb_client_SUITE_crdt_mvreg_test">>,
    Bound_object = {Key, antidote_crdt_register_mv, Bucket},

    {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Tx1} = antidotec_pb:start_transaction(Pid1, ignore, []),
    ok = antidotec_pb:update_objects(Pid1, [{Bound_object, assign, <<"a">>}], Tx1),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx1),

    %% Read committed updated
    {ok, Tx3} = antidotec_pb:start_transaction(Pid1, ignore, []),
    {ok, [Val]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx3),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx3),

    ?assertEqual({mvreg, [<<"a">>]}, Val),
    _Disconnected = antidotec_pb_socket:stop(Pid1).


crdt_set_rw_test(_Config) ->
    Bucket = ?BUCKET_BIN,
    Key = <<"pb_client_SUITE_crdt_set_rw_test">>,
    {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_set_rw, Bucket},

    {ok, Tx1} = antidotec_pb:start_transaction(Pid1, ignore, []),
    ok = antidotec_pb:update_objects(Pid1, [{Bound_object, add, <<"a">>}], Tx1),
    ok = antidotec_pb:update_objects(Pid1, [{Bound_object, add_all, [<<"b">>, <<"c">>, <<"d">>, <<"e">>, <<"f">>]}], Tx1),
    ok = antidotec_pb:update_objects(Pid1, [{Bound_object, remove, <<"b">>}], Tx1),
    ok = antidotec_pb:update_objects(Pid1, [{Bound_object, remove_all, [<<"c">>, <<"d">>]}], Tx1),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx1),

    %% Read committed updated
    {ok, Tx3} = antidotec_pb:start_transaction(Pid1, ignore, []),
    {ok, [Val]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx3),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx3),

    ?assertEqual({set, [<<"a">>, <<"e">>, <<"f">>]}, Val),
    _Disconnected = antidotec_pb_socket:stop(Pid1).


crdt_gmap_test(_Config) ->
    Bucket = ?BUCKET_BIN,
    Key = <<"pb_client_SUITE_crdt_map_aw_test">>,
    {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_map_go, Bucket},

    {ok, Tx1} = antidotec_pb:start_transaction(Pid1, ignore, []),
    ok = antidotec_pb:update_objects(Pid1, [
        {Bound_object, update, {{<<"a">>, antidote_crdt_register_mv}, {assign, <<"42">>}}}], Tx1),
    ok = antidotec_pb:update_objects(Pid1, [
        {Bound_object, update, [
            {{<<"b">>, antidote_crdt_register_lww}, {assign, <<"X">>}},
            {{<<"c">>, antidote_crdt_register_mv}, {assign, <<"Paul">>}},
            {{<<"d">>, antidote_crdt_set_aw}, {add_all, [<<"Apple">>, <<"Banana">>]}},
            {{<<"e">>, antidote_crdt_set_rw}, {add_all, [<<"Apple">>, <<"Banana">>]}},
            {{<<"f">>, antidote_crdt_counter_pn}, {increment , 7}},
            {{<<"g">>, antidote_crdt_map_go}, {update, [
                {{<<"x">>, antidote_crdt_register_mv}, {assign, <<"17">>}}
            ]}},
            {{<<"h">>, antidote_crdt_map_rr}, {update, [
                {{<<"x">>, antidote_crdt_register_mv}, {assign, <<"15">>}}
            ]}}
        ]}], Tx1),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx1),

    %% Read committed updated
    {ok, Tx3} = antidotec_pb:start_transaction(Pid1, ignore, []),
    {ok, [Val]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx3),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx3),
    ExpectedRes = {map, [
        {{<<"a">>, antidote_crdt_register_mv}, [<<"42">>]},
        {{<<"b">>, antidote_crdt_register_lww}, <<"X">>},
        {{<<"c">>, antidote_crdt_register_mv}, [<<"Paul">>]},
        {{<<"d">>, antidote_crdt_set_aw}, [<<"Apple">>, <<"Banana">>]},
        {{<<"e">>, antidote_crdt_set_rw}, [<<"Apple">>, <<"Banana">>]},
        {{<<"f">>, antidote_crdt_counter_pn}, 7},
        {{<<"g">>, antidote_crdt_map_go}, [
            {{<<"x">>, antidote_crdt_register_mv}, [<<"17">>]}
        ]},
        {{<<"h">>, antidote_crdt_map_rr}, [
            {{<<"x">>, antidote_crdt_register_mv}, [<<"15">>]}
        ]}
    ]},

    ?assertEqual(ExpectedRes, Val),
    _Disconnected = antidotec_pb_socket:stop(Pid1).


crdt_map_rr_test(_Config) ->
    Bucket = ?BUCKET_BIN,

    Key = <<"pb_client_SUITE_crdt_map_rr_test">>,
    {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_map_rr, Bucket},

    {ok, Tx1} = antidotec_pb:start_transaction(Pid1, ignore, []),
    ok = antidotec_pb:update_objects(Pid1, [
        {Bound_object, update, {{<<"a">>, antidote_crdt_register_mv}, {assign, <<"42">>}}}], Tx1),
    ok = antidotec_pb:update_objects(Pid1, [
        {Bound_object, update, [
            {{<<"b">>, antidote_crdt_register_mv}, {assign, <<"X">>}},
            {{<<"b1">>, antidote_crdt_register_mv}, {assign, <<"X1">>}},
            {{<<"b2">>, antidote_crdt_register_mv}, {assign, <<"X2">>}},
            {{<<"b3">>, antidote_crdt_register_mv}, {assign, <<"X3">>}},
            {{<<"b4">>, antidote_crdt_register_mv}, {assign, <<"X4">>}},
            {{<<"b5">>, antidote_crdt_register_mv}, {assign, <<"X5">>}},
            {{<<"c">>, antidote_crdt_register_mv}, {assign, <<"Paul">>}},
            {{<<"d">>, antidote_crdt_set_aw}, {add_all, [<<"Apple">>, <<"Banana">>]}},
            {{<<"e">>, antidote_crdt_set_aw}, {add_all, [<<"Apple">>, <<"Banana">>]}},
            {{<<"f">>, antidote_crdt_counter_fat}, {increment , 7}},
            {{<<"g">>, antidote_crdt_map_rr}, {update, [
                {{<<"q">>, antidote_crdt_register_mv}, {assign, <<"Hello">>}},
                {{<<"x">>, antidote_crdt_counter_fat}, {increment, 17}}
            ]}},
            {{<<"h">>, antidote_crdt_map_rr}, {update, [
                {{<<"x">>, antidote_crdt_counter_fat}, {increment, 15}}
            ]}}
        ]}], Tx1),
    ok = antidotec_pb:update_objects(Pid1, [
        {Bound_object, remove, {<<"b1">>, antidote_crdt_register_mv}}], Tx1),
    ok = antidotec_pb:update_objects(Pid1,
        [{Bound_object, remove, [
            {<<"b2">>, antidote_crdt_register_mv},
            {<<"b3">>, antidote_crdt_register_mv}]}
        ], Tx1),
    ok = antidotec_pb:update_objects(Pid1,
        [{Bound_object, batch,
            {[ % updates
                {{<<"i">>, antidote_crdt_register_mv}, {assign, <<"X">>}}
            ], [ % removes
                {<<"b4">>, antidote_crdt_register_mv},
                {<<"b5">>, antidote_crdt_register_mv}
            ]}}
        ], Tx1),
    ok = antidotec_pb:update_objects(Pid1, [
        {Bound_object, remove, {<<"g">>, antidote_crdt_map_rr}}], Tx1),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx1),

    %% Read committed updated
    {ok, Tx3} = antidotec_pb:start_transaction(Pid1, ignore, []),
    {ok, [Val]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx3),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx3),
    ExpectedRes = {map, [
        {{<<"a">>, antidote_crdt_register_mv}, [<<"42">>]},
        {{<<"b">>, antidote_crdt_register_mv}, [<<"X">>]},
        {{<<"c">>, antidote_crdt_register_mv}, [<<"Paul">>]},
        {{<<"d">>, antidote_crdt_set_aw}, [<<"Apple">>, <<"Banana">>]},
        {{<<"e">>, antidote_crdt_set_aw}, [<<"Apple">>, <<"Banana">>]},
        {{<<"f">>, antidote_crdt_counter_fat}, 7},
        {{<<"h">>, antidote_crdt_map_rr}, [
            {{<<"x">>, antidote_crdt_counter_fat}, 15}
        ]},
        {{<<"i">>, antidote_crdt_register_mv}, [<<"X">>]}
    ]},

    ?assertEqual(ExpectedRes, Val),
    _Disconnected = antidotec_pb_socket:stop(Pid1).


crdt_flag_tests(Config) ->
    Bucket = ?BUCKET_BIN,
    [crdt_flag_test(Config, FlagCrdt, Bucket) || FlagCrdt <- [antidote_crdt_flag_ew, antidote_crdt_flag_dw]].


crdt_flag_test(_Config, FlagCrdt, Bucket) ->
  FlagCrdtBin = erlang:atom_to_binary(FlagCrdt, utf8),
  Key = <<"pb_client_SUITE_", FlagCrdtBin/binary>>,
  {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
  Bound_object = {Key, FlagCrdt, Bucket},
  {ok, Tx1} = antidotec_pb:start_transaction(Pid1, ignore, []),

  ok = antidotec_pb:update_objects(Pid1, [{Bound_object, enable, {}}], Tx1),
  {ok, [Val1]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx1),
  {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx1),
  {ok, Tx2} = antidotec_pb:start_transaction(Pid1, ignore, []),
  ok = antidotec_pb:update_objects(Pid1, [{Bound_object, disable, {}}], Tx2),
  {ok, [Val2]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx2),
  ok = antidotec_pb:update_objects(Pid1, [{Bound_object, reset, {}}], Tx2),
  {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx2),

  ?assertEqual({flag, true}, Val1),
  ?assertEqual({flag, false}, Val2),
  _Disconnected = antidotec_pb_socket:stop(Pid1).


static_transaction_test(_Config) ->
    Bucket = ?BUCKET_BIN,
    Key = <<"pb_client_SUITE_static_transaction_test">>,
    Bound_object = {Key, antidote_crdt_set_aw, Bucket},

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),

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

    ?assertEqual(2, length(antidotec_set:value(Val))),
    ?assertMatch(true, antidotec_set:contains(<<"a">>, Val)),
    ?assertMatch(true, antidotec_set:contains(<<"b">>, Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).
