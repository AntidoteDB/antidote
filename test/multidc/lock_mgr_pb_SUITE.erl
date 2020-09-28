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
-module(lock_mgr_pb_SUITE).

%% common_test callbacks
-export([%% suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([
    kram/1,
    kram2/1,
    lock_pb_test1/1,
    lock_pb_test2/1,
    lock_pb_test3/1,
    lock_pb_combination_test1/1,
    lock_pb_combination_test2/1,
    lock_pb_combination_test3/1,
    lock_pb_combination_test4/1,
    lock_pb_combination_test5/1,
    lock_pb_combination_test6/1,
    lock_pb_combination_test7/1,
    lock_pb_combination_test8/1,
    lock_pb_combination_test9/1,
    lock_pb_combination_test10/1,
    lock_pb_not_leader_dc_test1/1,
    lock_pb_not_leader_dc_test2/1,
    lock_pb_not_leader_dc_test3/1,
    transaction_locks_speed_test1/1,
    transaction_locks_speed_test2/1,
    transaction_locks_speed_test3/1,
    transaction_locks_speed_test4/1,
    transaction_locks_speed_test5/1,
    transaction_locks_speed_test6/1,
    transaction_locks_speed_test7/1,
    transaction_locks_speed_test8/1,
    transaction_locks_speed_test9/1,
    transaction_locks_speed_test10/1,
    transaction_locks_speed_test11/1,
    transaction_locks_speed_test12/1,
    pb_locks_speed_test1/1,
    pb_locks_speed_test2/1,
    pb_locks_speed_test3/1,
    pb_locks_speed_test4/1,
    pb_locks_speed_test5/1,
    pb_locks_speed_test6/1,
    pb_locks_speed_test7/1,
    pb_locks_speed_test8/1,
    pb_locks_speed_test9/1,
    pb_locks_speed_test10/1,
    pb_locks_speed_test11/1,
    pb_locks_speed_test12/1,
    transaction_locks_other_node_speed_test1/1,
    transaction_locks_other_node_speed_test2/1,
    transaction_locks_other_node_speed_test3/1,
    transaction_locks_other_node_speed_test4/1,
    transaction_locks_other_node_speed_test5/1,
    transaction_locks_other_node_speed_test6/1,
    transaction_locks_other_node_speed_test7/1,
    transaction_locks_other_node_speed_test8/1,
    transaction_locks_other_node_speed_test9/1,
    transaction_locks_other_node_speed_test10/1,
    transaction_locks_other_node_speed_test11/1,
    transaction_locks_other_node_speed_test12/1,
    pb_locks_other_node_speed_test1/1,
    pb_locks_other_node_speed_test2/1,
    pb_locks_other_node_speed_test3/1,
    pb_locks_other_node_speed_test4/1,
    pb_locks_other_node_speed_test5/1,
    pb_locks_other_node_speed_test6/1,
    pb_locks_other_node_speed_test7/1,
    pb_locks_other_node_speed_test8/1,
    pb_locks_other_node_speed_test9/1,
    pb_locks_other_node_speed_test10/1,
    pb_locks_other_node_speed_test11/1,
    pb_locks_other_node_speed_test12/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(ADDRESS, "localhost").
-define(PORT1, 10017).
-define(PORT2, 10037).
-define(PORT3, 10047).
-define(BUCKET, pb_client_bucket).
-define(BUCKET_BIN, <<"pb_client_bucket">>).

init_per_suite(InitialConfig) ->
    Config = test_utils:init_multi_dc(?MODULE, InitialConfig),
    Nodes = proplists:get_value(nodes, Config),
    Clusters = proplists:get_value(clusters, Config),

    %Ensure that the clocksi protocol is used
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
        [antidote, txn_prot, clocksi]) end, Nodes),

    %Check that indeed clocksi is running
    {ok, clocksi} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_prot]),

    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    kram,
    kram2,
    lock_pb_test1,
    lock_pb_test2,
    lock_pb_test3,
    lock_pb_combination_test1,
    lock_pb_combination_test2,
    lock_pb_combination_test3,
    lock_pb_combination_test4,
    lock_pb_combination_test5,
    lock_pb_combination_test6,
    lock_pb_combination_test7,
    lock_pb_combination_test8,
    lock_pb_combination_test9,
    lock_pb_combination_test10,
    lock_pb_not_leader_dc_test1,
    lock_pb_not_leader_dc_test2,
    lock_pb_not_leader_dc_test3,
    transaction_locks_speed_test1,
    transaction_locks_speed_test2,
    transaction_locks_speed_test3,
    transaction_locks_speed_test4,
    transaction_locks_speed_test5,
    transaction_locks_speed_test6,
    transaction_locks_speed_test7,
    transaction_locks_speed_test8,
    transaction_locks_speed_test9,
    transaction_locks_speed_test10,
    transaction_locks_speed_test11,
    transaction_locks_speed_test12,
    transaction_locks_other_node_speed_test1,
    transaction_locks_other_node_speed_test2,
    transaction_locks_other_node_speed_test3,
    transaction_locks_other_node_speed_test4,
    transaction_locks_other_node_speed_test5,
    transaction_locks_other_node_speed_test6,
    transaction_locks_other_node_speed_test7,
    transaction_locks_other_node_speed_test8,
    transaction_locks_other_node_speed_test9,
    transaction_locks_other_node_speed_test10,
    transaction_locks_other_node_speed_test11,
    transaction_locks_other_node_speed_test12,
    pb_locks_other_node_speed_test1,
    pb_locks_other_node_speed_test2,
    pb_locks_other_node_speed_test3,
    pb_locks_other_node_speed_test4,
    pb_locks_other_node_speed_test5,
    pb_locks_other_node_speed_test6,
    pb_locks_other_node_speed_test7,
    pb_locks_other_node_speed_test8,
    pb_locks_other_node_speed_test9,
    pb_locks_other_node_speed_test10,
    pb_locks_other_node_speed_test11,
    pb_locks_other_node_speed_test12
].


kram(Config)->
    [[Node1, Node2], [Node3] | _Nodes] = proplists:get_value(clusters, Config),
    Result1 = rpc:call(Node1, dc_utilities, get_stable_snapshot, []),
    ct:print("Snapshot Node1: ~p", [Result1]),
    Result2 = rpc:call(Node2, dc_utilities, get_stable_snapshot, []),
    ct:print("Snapshot Node2: ~p", [Result2]),
    Result3 = rpc:call(Node3, dc_utilities, get_stable_snapshot, []),
    ct:print("Snapshot Node3: ~p", [Result3]).

kram2(_Config)->
    {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    {ok, Pid2} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    {ok, Pid3} = antidotec_pb_socket:start(?ADDRESS, ?PORT3),
    Exclusive_Locks = [<<"kram2_key_1">>],
    get_locks_helper3(Pid1, [], Exclusive_Locks),
    get_locks_helper3(Pid2, [], Exclusive_Locks),
    get_locks_helper3(Pid3, [], Exclusive_Locks),
    antidotec_pb_socket:stop(Pid1),
    antidotec_pb_socket:stop(Pid2),
    antidotec_pb_socket:stop(Pid3).


%% Testing lock acquisition via protocol buffer interface
lock_pb_test1(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_test1_key_1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_test1_key_1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing shared_lock acquisition via protocol buffer interface
lock_pb_test2(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_test2_key_1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_test2_key_1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing exclusive_lock acquisition via protocol buffer interface
lock_pb_test3(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_test2_key_1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_test2_key_1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test1(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test1_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test1_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}, {exclusive_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test2(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test2_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test2_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}, {shared_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test3(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test3_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test3_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}, {shared_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test4(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test4_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test4_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}, {exclusive_locks, Locks}, {shared_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test5(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test5_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test5_key1">>,
        <<"lock_pb_combination_test5_key2">>,
        <<"lock_pb_combination_test5_key3">>,
        <<"lock_pb_combination_test5_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}, {exclusive_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test6(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test6_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test6_key1">>,
        <<"lock_pb_combination_test6_key2">>,
        <<"lock_pb_combination_test6_key3">>,
        <<"lock_pb_combination_test6_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks, Locks}, {exclusive_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test7(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test7_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test7_key1">>,
        <<"lock_pb_combination_test7_key2">>,
        <<"lock_pb_combination_test7_key3">>,
        <<"lock_pb_combination_test7_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks, Locks}, {exclusive_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test8(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test8_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test8_key1">>,
        <<"lock_pb_combination_test8_key2">>,
        <<"lock_pb_combination_test8_key3">>,
        <<"lock_pb_combination_test8_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks, Locks}, {exclusive_locks, Locks}, {exclusive_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test9(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test9_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test9_key1">>,
        <<"lock_pb_combination_test9_key2">>,
        <<"lock_pb_combination_test9_key3">>,
        <<"lock_pb_combination_test9_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}, {shared_locks, Locks}, {exclusive_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test10(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test10_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test10_key1">>,
        <<"lock_pb_combination_test10_key2">>,
        <<"lock_pb_combination_test10_key3">>,
        <<"lock_pb_combination_test10_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}, {exclusive_locks, Locks}, {shared_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface using not the leader DC (The one which may create)
%% new locks)
lock_pb_not_leader_dc_test1(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Bound_object = {<<"lock_pb_not_leader_dc_test1_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_not_leader_dc_test1_key1">>,
        <<"lock_pb_not_leader_dc_test1_key2">>,
        <<"lock_pb_not_leader_dc_test1_key3">>,
        <<"lock_pb_not_leader_dc_test1_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks, Locks}, {exclusive_locks, Locks}, {exclusive_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface using not the leader DC (The one which may create)
%% new locks)
lock_pb_not_leader_dc_test2(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT3),
    Bound_object = {<<"lock_pb_not_leader_dc_test2_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_not_leader_dc_test2_key1">>,
        <<"lock_pb_not_leader_dc_test2_key2">>,
        <<"lock_pb_not_leader_dc_test2_key3">>,
        <<"lock_pb_not_leader_dc_test2_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}, {shared_locks, Locks}, {exclusive_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface using not the leader DC (The one which may create)
%% new locks)
lock_pb_not_leader_dc_test3(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Bound_object = {<<"lock_pb_not_leader_dc_test3_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_not_leader_dc_test3_key1">>,
        <<"lock_pb_not_leader_dc_test3_key2">>,
        <<"lock_pb_not_leader_dc_test3_key3">>,
        <<"lock_pb_not_leader_dc_test3_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Locks}, {exclusive_locks, Locks}, {shared_locks, Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).





% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock
transaction_locks_speed_test1(Config)->
    [[Node1 | _] | _] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test1_key1">>],
    get_locks_helper2(Node1, Locks, []).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock
transaction_locks_speed_test2(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test2_key1">>],
    get_locks_helper2(Node1, [], Locks).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock
transaction_locks_speed_test3(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test3_key1">>],
    get_locks_helper2(Node1, Locks, Locks).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring multiple locks
transaction_locks_speed_test4(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test4_key1">>,
        <<"transaction_lock_speed_test4_key2">>,
        <<"transaction_lock_speed_test4_key3">>,
        <<"transaction_lock_speed_test4_key4">>],
    get_locks_helper2(Node1, Locks, []).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring multiple locks
transaction_locks_speed_test5(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test5_key1">>,
        <<"transaction_lock_speed_test5_key2">>,
        <<"transaction_lock_speed_test5_key3">>,
        <<"transaction_lock_speed_test5_key4">>],
    get_locks_helper2(Node1, [], Locks).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring multiple locks
transaction_locks_speed_test6(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test6_key1">>,
        <<"transaction_lock_speed_test6_key2">>,
        <<"transaction_lock_speed_test6_key3">>,
        <<"transaction_lock_speed_test6_key4">>],
    get_locks_helper2(Node1, Locks, Locks).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock multiple times
transaction_locks_speed_test7(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test7_key1">>],
    multiple_get_locks_helper2(Node1, Locks, [], 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock multiple times
transaction_locks_speed_test8(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test8_key1">>],
    multiple_get_locks_helper2(Node1, [], Locks, 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock multiple times
transaction_locks_speed_test9(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test9_key1">>],
    multiple_get_locks_helper2(Node1, Locks, Locks, 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring multiple lock multiple times
transaction_locks_speed_test10(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test10_key1">>,
        <<"transaction_lock_speed_test10_key2">>,
        <<"transaction_lock_speed_test10_key3">>,
        <<"transaction_lock_speed_test10_key4">>],
    multiple_get_locks_helper2(Node1, Locks, [], 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple lock multiple times
transaction_locks_speed_test11(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test11_key1">>,
        <<"transaction_lock_speed_test11_key2">>,
        <<"transaction_lock_speed_test11_key3">>,
        <<"transaction_lock_speed_test11_key4">>],
    multiple_get_locks_helper2(Node1, [], Locks, 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple lock multiple times
transaction_locks_speed_test12(Config)->
    [[Node1 | _] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_lock_speed_test12_key1">>,
        <<"transaction_lock_speed_test12_key2">>,
        <<"transaction_lock_speed_test12_key3">>,
        <<"transaction_lock_speed_test12_key4">>],
    multiple_get_locks_helper2(Node1, Locks, Locks, 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock
pb_locks_speed_test1(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test1_key1">>],
    get_locks_helper3(Pid, Locks, []),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock
pb_locks_speed_test2(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test2_key1">>],
    get_locks_helper3(Pid, [], Locks),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock
pb_locks_speed_test3(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test3_key1">>],
    get_locks_helper3(Pid, Locks, Locks),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple locks
pb_locks_speed_test4(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test4_key1">>,
        <<"pb_locks_speed_test4_key2">>,
        <<"pb_locks_speed_test4_key3">>,
        <<"pb_locks_speed_test4_key4">>],
    get_locks_helper3(Pid, Locks, []),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple locks
pb_locks_speed_test5(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test5_key1">>,
        <<"pb_locks_speed_test5_key2">>,
        <<"pb_locks_speed_test5_key3">>,
        <<"pb_locks_speed_test5_key4">>],
    get_locks_helper3(Pid, [], Locks),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple locks
pb_locks_speed_test6(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test6_key1">>,
        <<"pb_locks_speed_test6_key2">>,
        <<"pb_locks_speed_test6_key3">>,
        <<"pb_locks_speed_test6_key4">>],
    get_locks_helper3(Pid, Locks, Locks),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock multiple times
pb_locks_speed_test7(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test7_key1">>],
    multiple_get_locks_helper3(Pid, Locks, [], 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock multiple times
pb_locks_speed_test8(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test8_key1">>],
    multiple_get_locks_helper3(Pid, [], Locks, 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock multiple times
pb_locks_speed_test9(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test9_key1">>],
    multiple_get_locks_helper3(Pid, Locks, Locks, 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple lock multiple times
pb_locks_speed_test10(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test10_key1">>,
        <<"pb_locks_speed_test10_key2">>,
        <<"pb_locks_speed_test10_key3">>,
        <<"pb_locks_speed_test10_key4">>],
    multiple_get_locks_helper3(Pid, Locks, [], 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple lock multiple times
pb_locks_speed_test11(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test11_key1">>,
        <<"pb_locks_speed_test11_key2">>,
        <<"pb_locks_speed_test11_key3">>,
        <<"pb_locks_speed_test11_key4">>],
    multiple_get_locks_helper3(Pid, [], Locks, 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple lock multiple times
pb_locks_speed_test12(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"pb_locks_speed_test12_key1">>,
        <<"pb_locks_speed_test12_key2">>,
        <<"pb_locks_speed_test12_key3">>,
        <<"pb_locks_speed_test12_key4">>],
    multiple_get_locks_helper3(Pid, Locks, Locks, 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).


get_locks_helper2(Node, [], Exclusive_Locks) ->
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks, Exclusive_Locks}]]),
    {ok, _Clock} = rpc:call(Node, antidote, commit_transaction, [TxId]);
get_locks_helper2(Node, Shared_Locks, []) ->
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, [{shared_locks, Shared_Locks}]]),
    {ok, _Clock} = rpc:call(Node, antidote, commit_transaction, [TxId]);
get_locks_helper2(Node, Shared_Locks, Exclusive_Locks) ->
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, [{shared_locks, Shared_Locks}, {exclusive_locks, Exclusive_Locks}]]),
    {ok, _Clock} = rpc:call(Node, antidote, commit_transaction, [TxId]).

multiple_get_locks_helper2(_, _, _, 0) ->
    ok;
multiple_get_locks_helper2(Node, Shared_Locks, Exclusive_Locks, Repetitions) ->
    get_locks_helper2(Node, Shared_Locks, Exclusive_Locks),
    multiple_get_locks_helper2(Node, Shared_Locks, Exclusive_Locks, Repetitions-1).

get_locks_helper3(Pid, [], Exclusive_Locks) ->
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks, Exclusive_Locks}]),
    {ok, _Clock} = antidotec_pb:commit_transaction(Pid, TxId);
get_locks_helper3(Pid, Shared_Locks, []) ->
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks, Shared_Locks}]),
    {ok, _Clock} = antidotec_pb:commit_transaction(Pid, TxId);
get_locks_helper3(Pid, Shared_Locks, Exclusive_Locks) ->
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks, Shared_Locks}, {exclusive_locks, Exclusive_Locks}]),
    {ok, _Clock} = antidotec_pb:commit_transaction(Pid, TxId).

multiple_get_locks_helper3(_, _, _, 0) ->
    ok;
multiple_get_locks_helper3(Pid, Shared_Locks, Exclusive_Locks, Repetitions) ->
    get_locks_helper3(Pid, Shared_Locks, Exclusive_Locks),
    multiple_get_locks_helper3(Pid, Shared_Locks, Exclusive_Locks, Repetitions-1).




% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock
transaction_locks_other_node_speed_test1(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test1_key1">>],
    get_locks_helper2(Node2, Locks, []).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock
transaction_locks_other_node_speed_test2(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test2_key1">>],
    get_locks_helper2(Node2, [], Locks).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock
transaction_locks_other_node_speed_test3(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test3_key1">>],
    get_locks_helper2(Node2, Locks, Locks).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring multiple locks
transaction_locks_other_node_speed_test4(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test4_key1">>,
        <<"transaction_locks_other_node_speed_test4_key2">>,
        <<"transaction_locks_other_node_speed_test4_key3">>,
        <<"transaction_locks_other_node_speed_test4_key4">>],
    get_locks_helper2(Node2, Locks, []).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring multiple locks
transaction_locks_other_node_speed_test5(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test5_key1">>,
        <<"transaction_locks_other_node_speed_test5_key2">>,
        <<"transaction_locks_other_node_speed_test5_key3">>,
        <<"transaction_locks_other_node_speed_test5_key4">>],
    get_locks_helper2(Node2, [], Locks).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring multiple locks
transaction_locks_other_node_speed_test6(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test6_key1">>,
        <<"transaction_locks_other_node_speed_test6_key2">>,
        <<"transaction_locks_other_node_speed_test6_key3">>,
        <<"transaction_locks_other_node_speed_test6_key4">>],
    get_locks_helper2(Node2, Locks, Locks).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock multiple times
transaction_locks_other_node_speed_test7(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test7_key1">>],
    multiple_get_locks_helper2(Node2, Locks, [], 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock multiple times
transaction_locks_other_node_speed_test8(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test8_key1">>],
    multiple_get_locks_helper2(Node2, [], Locks, 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring a single lock multiple times
transaction_locks_other_node_speed_test9(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test9_key1">>],
    multiple_get_locks_helper2(Node2, Locks, Locks, 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the antidote interface
% While acquiring multiple lock multiple times
transaction_locks_other_node_speed_test10(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test10_key1">>,
        <<"transaction_locks_other_node_speed_test10_key2">>,
        <<"transaction_locks_other_node_speed_test10_key3">>,
        <<"transaction_locks_other_node_speed_test10_key4">>],
    multiple_get_locks_helper2(Node2, Locks, [], 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple lock multiple times
transaction_locks_other_node_speed_test11(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test11_key1">>,
        <<"transaction_locks_other_node_speed_test11_key2">>,
        <<"transaction_locks_other_node_speed_test11_key3">>,
        <<"transaction_locks_other_node_speed_test11_key4">>],
    multiple_get_locks_helper2(Node2, [], Locks, 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple lock multiple times
transaction_locks_other_node_speed_test12(Config)->
    [[_, Node2] | _Nodes] = proplists:get_value(clusters, Config),
    Locks = [<<"transaction_locks_other_node_speed_test12_key1">>,
        <<"transaction_locks_other_node_speed_test12_key2">>,
        <<"transaction_locks_other_node_speed_test12_key3">>,
        <<"transaction_locks_other_node_speed_test12_key4">>],
    multiple_get_locks_helper2(Node2, Locks, Locks, 99).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock
pb_locks_other_node_speed_test1(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test1_key1">>],
    get_locks_helper3(Pid, Locks, []),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock
pb_locks_other_node_speed_test2(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test2_key1">>],
    get_locks_helper3(Pid, [], Locks),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock
pb_locks_other_node_speed_test3(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test3_key1">>],
    get_locks_helper3(Pid, Locks, Locks),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple locks
pb_locks_other_node_speed_test4(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test4_key1">>,
        <<"pb_locks_other_node_speed_test4_key2">>,
        <<"pb_locks_other_node_speed_test4_key3">>,
        <<"pb_locks_other_node_speed_test4_key4">>],
    get_locks_helper3(Pid, Locks, []),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple locks
pb_locks_other_node_speed_test5(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test5_key1">>,
        <<"pb_locks_other_node_speed_test5_key2">>,
        <<"pb_locks_other_node_speed_test5_key3">>,
        <<"pb_locks_other_node_speed_test5_key4">>],
    get_locks_helper3(Pid, [], Locks),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple locks
pb_locks_other_node_speed_test6(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test6_key1">>,
        <<"pb_locks_other_node_speed_test6_key2">>,
        <<"pb_locks_other_node_speed_test6_key3">>,
        <<"pb_locks_other_node_speed_test6_key4">>],
    get_locks_helper3(Pid, Locks, Locks),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock multiple times
pb_locks_other_node_speed_test7(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test7_key1">>],
    multiple_get_locks_helper3(Pid, Locks, [], 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock multiple times
pb_locks_other_node_speed_test8(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test8_key1">>],
    multiple_get_locks_helper3(Pid, [], Locks, 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring a single lock multiple times
pb_locks_other_node_speed_test9(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test9_key1">>],
    multiple_get_locks_helper3(Pid, Locks, Locks, 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple lock multiple times
pb_locks_other_node_speed_test10(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test10_key1">>,
        <<"pb_locks_other_node_speed_test10_key2">>,
        <<"pb_locks_other_node_speed_test10_key3">>,
        <<"pb_locks_other_node_speed_test10_key4">>],
    multiple_get_locks_helper3(Pid, Locks, [], 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple lock multiple times
pb_locks_other_node_speed_test11(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test11_key1">>,
        <<"pb_locks_other_node_speed_test11_key2">>,
        <<"pb_locks_other_node_speed_test11_key3">>,
        <<"pb_locks_other_node_speed_test11_key4">>],
    multiple_get_locks_helper3(Pid, [], Locks, 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).

% Tests the speed of the lock_mgr_es function get_locks() when using the pb_buffer interface
% While acquiring multiple lock multiple times
pb_locks_other_node_speed_test12(_Config)->

    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"pb_locks_other_node_speed_test12_key1">>,
        <<"pb_locks_other_node_speed_test12_key2">>,
        <<"pb_locks_other_node_speed_test12_key3">>,
        <<"pb_locks_other_node_speed_test12_key4">>],
    multiple_get_locks_helper3(Pid, Locks, Locks, 99),
    _Disconnected = antidotec_pb_socket:stop(Pid).


