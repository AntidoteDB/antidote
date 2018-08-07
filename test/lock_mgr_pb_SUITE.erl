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
        lock_pb_not_leader_dc_test3/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(ADDRESS, "localhost").
-define(PORT, 10017).
-define(BUCKET, pb_client_bucket).
-define(BUCKET_BIN, <<"pb_client_bucket">>).

init_per_suite(Config) ->
    ct:print("Starting test suite ~p with pb client at ~s:~p", [?MODULE, ?ADDRESS, ?PORT]),
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = hd(Clusters),
    [{nodes, Nodes}|Config].

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
        lock_pb_not_leader_dc_test3
        ].



%% Testing lock acquisition via protocol buffer interface
lock_pb_test1(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_test1_key_1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_test1_key_1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing shared_lock acquisition via protocol buffer interface
lock_pb_test2(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_test2_key_1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_test2_key_1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing exclusive_lock acquisition via protocol buffer interface
lock_pb_test3(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_test2_key_1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_test2_key_1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test1(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_combination_test1_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test1_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test2(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_combination_test2_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test2_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test3(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_combination_test3_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test3_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{locks,Locks},{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test4(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_combination_test4_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test4_key1">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{locks,Locks},{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test5(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_combination_test5_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test5_key1">>,<<"lock_pb_combination_test5_key2">>,<<"lock_pb_combination_test5_key3">>,<<"lock_pb_combination_test5_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{locks,Locks},{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test6(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_combination_test6_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test6_key1">>,<<"lock_pb_combination_test6_key2">>,<<"lock_pb_combination_test6_key3">>,<<"lock_pb_combination_test6_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks,Locks},{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test7(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_combination_test7_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test7_key1">>,<<"lock_pb_combination_test7_key2">>,<<"lock_pb_combination_test7_key3">>,<<"lock_pb_combination_test7_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks,Locks},{locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test8(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_combination_test8_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test8_key1">>,<<"lock_pb_combination_test8_key2">>,<<"lock_pb_combination_test8_key3">>,<<"lock_pb_combination_test8_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{shared_locks,Locks},{locks,Locks},{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test9(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_combination_test9_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_combination_test9_key1">>,<<"lock_pb_combination_test9_key2">>,<<"lock_pb_combination_test9_key3">>,<<"lock_pb_combination_test9_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{locks,Locks},{shared_locks,Locks},{exclusive_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

%% Testing combinations of lock acquisitions via protocol buffer interface
lock_pb_combination_test10(_Config) ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
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
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
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
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
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
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"lock_pb_not_leader_dc_test3_key1">>, antidote_crdt_counter_pn, ?BUCKET_BIN},
    Locks = [<<"lock_pb_not_leader_dc_test3_key1">>,<<"lock_pb_not_leader_dc_test3_key2">>,<<"lock_pb_not_leader_dc_test3_key3">>,<<"lock_pb_not_leader_dc_test3_key4">>],
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{exclusive_locks,Locks},{locks,Locks},{shared_locks,Locks}]),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).
