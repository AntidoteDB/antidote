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

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([
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
        internal_data_test1/1,
        internal_data_test2/1,
        internal_data_test3/1
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

init_per_suite(Config) ->
    ct:print("Starting test suite ~p with pb client at ~s:~p", [?MODULE, ?ADDRESS, ?PORT1]),
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Node1 = hd(hd(Clusters)),
    Node2 = hd(hd(tl(Clusters))),
    Node3 = hd(hd(tl(tl(Clusters)))),
    [{nodes, [Node1,Node2,Node3]}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
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
        internal_data_test1,
        internal_data_test2,
        internal_data_test3
        ].



%% Testing lock acquisition via protocol buffer interface
lock_pb_test1(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_test1_key_1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_test1_key_1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing shared_lock acquisition via protocol buffer interface
lock_pb_test2(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_test2_key_1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_test2_key_1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing exclusive_lock acquisition via protocol buffer interface
lock_pb_test3(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_test2_key_1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_test2_key_1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test1(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test1_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test1_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test2(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test2_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test2_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test3(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test3_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test3_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{locks,Locks},{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test4(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test4_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test4_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{locks,Locks},{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test5(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test5_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test5_key1">>,<<"lock_pb_combination_test5_key2">>,<<"lock_pb_combination_test5_key3">>,<<"lock_pb_combination_test5_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{locks,Locks},{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test6(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test6_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test6_key1">>,<<"lock_pb_combination_test6_key2">>,<<"lock_pb_combination_test6_key3">>,<<"lock_pb_combination_test6_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks,Locks},{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test7(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test7_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test7_key1">>,<<"lock_pb_combination_test7_key2">>,<<"lock_pb_combination_test7_key3">>,<<"lock_pb_combination_test7_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks,Locks},{locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test8(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test8_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test8_key1">>,<<"lock_pb_combination_test8_key2">>,<<"lock_pb_combination_test8_key3">>,<<"lock_pb_combination_test8_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks,Locks},{locks,Locks},{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test9(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test9_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test9_key1">>,<<"lock_pb_combination_test9_key2">>,<<"lock_pb_combination_test9_key3">>,<<"lock_pb_combination_test9_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{locks,Locks},{shared_locks,Locks},{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test10(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Bound_object = {<<"lock_pb_combination_test10_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test10_key1">>,<<"lock_pb_combination_test10_key2">>,<<"lock_pb_combination_test10_key3">>,<<"lock_pb_combination_test10_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{locks,Locks},{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface using not the leader DC (The one which may create)
%% new locks)
lock_pb_not_leader_dc_test1(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Bound_object = {<<"lock_pb_not_leader_dc_test1_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_not_leader_dc_test1_key1">>,<<"lock_pb_not_leader_dc_test1_key2">>,<<"lock_pb_not_leader_dc_test1_key3">>,<<"lock_pb_not_leader_dc_test1_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks,Locks},{locks,Locks},{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface using not the leader DC (The one which may create)
%% new locks)
lock_pb_not_leader_dc_test2(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT3),
    Bound_object = {<<"lock_pb_not_leader_dc_test2_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_not_leader_dc_test2_key2">>,<<"lock_pb_not_leader_dc_test2_key2">>,<<"lock_pb_not_leader_dc_test2_key3">>,<<"lock_pb_not_leader_dc_test2_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{locks,Locks},{shared_locks,Locks},{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface using not the leader DC (The one which may create)
%% new locks)
lock_pb_not_leader_dc_test3(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Bound_object = {<<"lock_pb_not_leader_dc_test3_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_not_leader_dc_test3_key1">>,<<"lock_pb_not_leader_dc_test3_key2">>,<<"lock_pb_not_leader_dc_test3_key3">>,<<"lock_pb_not_leader_dc_test3_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{locks,Locks},{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Tests the internal data of lock_mgr_es and lock_mgr when using pb interface
internal_data_test1(Config) ->
    [Node1, _Node2 | _Nodes] = proplists:get_value(nodes, Config),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT1),
    Locks = [<<"internal_data_test1_key1">>,<<"internal_data_test1_key2">>,<<"internal_data_test1_key3">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{locks,Locks},{shared_locks,Locks}]),
    Lock_Info1 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    [{_,{using,Locks,Locks}}] = Lock_Info1,
    Lock_Info2 = rpc:call(Node1, lock_mgr, local_locks_info, []),
    [{_,{using,Locks}}] = Lock_Info2,
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid).

%% Tests the internal data of lock_mgr_es and lock_mgr when using pb interface
internal_data_test2(Config) ->
    [_Node1, Node2 | _Nodes] = proplists:get_value(nodes, Config),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT2),
    Locks = [<<"internal_data_test2_key1">>,<<"internal_data_test2_key2">>,<<"internal_data_test2_key3">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{locks,Locks},{shared_locks,Locks}]),
    Lock_Info1 = rpc:call(Node2, lock_mgr_es, local_locks_info, []),
    [{_,{using,Locks,Locks}}] = Lock_Info1,
    Lock_Info2 = rpc:call(Node2, lock_mgr, local_locks_info, []),
    [{_,{using,Locks}}] = Lock_Info2,
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid).


%% Tests the internal data of lock_mgr_es and lock_mgr when using pb interface
internal_data_test3(Config) ->
    [_Node1, _Node2,Node3 | _Nodes] = proplists:get_value(nodes, Config),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT3),
    Locks = [<<"internal_data_test3_key1">>,<<"internal_data_test3_key2">>,<<"internal_data_test3_key3">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{locks,Locks},{shared_locks,Locks}]),
    Lock_Info1 = rpc:call(Node3, lock_mgr_es, local_locks_info, []),
    [{_,{using,Locks,Locks}}] = Lock_Info1,
    Lock_Info2 = rpc:call(Node3, lock_mgr, local_locks_info, []),
    [{_,{using,Locks}}] = Lock_Info2,
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid).
