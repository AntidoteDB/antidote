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

-module(bcountermgr_SUITE).

%% common_test callbacks
-export([init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([new_bcounter_test/1,
    update_bcounter_test/1,
    actor_does_not_matter_test/1,
    invalid_bcounter_test/1,
    self_transfer_is_increment_test/1,
    invalid_dcid_test/1,
    add_existing_pending_request/1,
    process_transfer_on_self_has_no_effect/1,
    process_transfer_to_invalid_dcid_has_no_effect/1,
    crash_recovery_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(bcountermgr_bucket)).


init_per_suite(InitialConfig) ->
    Config = test_utils:init_single_dc(?MODULE, InitialConfig),
    Clusters = proplists:get_value(clusters, Config),
    Nodes = lists:flatten(Clusters),

    % Ensure that write operations are certified
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env, [antidote, txn_cert, true])
                    end, Nodes),

    % Check that indeed transactions certification is turned on
    {ok, true} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_cert]),

    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    new_bcounter_test,
    update_bcounter_test,
    actor_does_not_matter_test,
    invalid_bcounter_test,
    self_transfer_is_increment_test,
    invalid_dcid_test,
    add_existing_pending_request,
    process_transfer_on_self_has_no_effect,
    process_transfer_to_invalid_dcid_has_no_effect,
    crash_recovery_test
].

new_bcounter_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter1_mgr_single,
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0).

update_bcounter_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter2_mgr_single,
    {_, CommitTime1} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    {ok, CommitTime2} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime1, antidote_utils:bcounter_get_increment_op(Node, 5)),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, CommitTime2, 5).

actor_does_not_matter_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter3_mgr_single,
    Actor = no_dcid,
    {_, CommitTime1} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    {ok, CommitTime2} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime1, {increment, {5, Actor}}),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, CommitTime2, 5),
    {ok, CommitTime3} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime2, {decrement, {4, Actor}}),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, CommitTime3, 1).

invalid_bcounter_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter4_mgr_single,
    {_, CommitTime1} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    {error, no_permissions} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime1, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, CommitTime1, 0).

self_transfer_is_increment_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter5_mgr_single,
    {_, CommitTime1} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    {ok, CommitTime2} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime1, antidote_utils:bcounter_get_increment_op(Node, 5)),
    {ok, CommitTime3} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime2, antidote_utils:bcounter_get_transfer_op(Node, 5, rpc:call(Node, dc_utilities, get_my_dc_id, []))),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, CommitTime3, 10).

invalid_dcid_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter6_mgr_single,
    {_, CommitTime1} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    {error, invalid_dcid} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime1, {transfer, {5, no_dcid, no_dcid_either}}),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, CommitTime1, 0).

add_existing_pending_request(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter7_mgr_single,
    {_, _} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    ok = rpc:call(Node, bcounter_mgr, set_transfer_periodic_active, [false]),
    {error, no_permissions} = antidote_utils:bcounter_update_single(Node, Key, Bucket, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    {error, no_permissions} = antidote_utils:bcounter_update_single(Node, Key, Bucket, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    PendingTransferRequests = rpc:call(Node, bcounter_mgr, get_pending_transfer_requests, []),
    PendingTransferRequestsForKey = orddict:fetch({Key, Bucket}, PendingTransferRequests),
    {[5, 5], _} = lists:unzip(PendingTransferRequestsForKey),
    timer:sleep(200), %%make sure that periodic transfer is inactive %%TODO this is dependent on ?TRANSFER_FREQ
    ok = rpc:call(Node, bcounter_mgr, set_transfer_periodic_active, [true]),
    timer:sleep(600), %%TODO this is dependent on ?REQUEST_TIMEOUT
    %%Now there should be no pending transfer requests
    PendingTransferRequests2 = rpc:call(Node, bcounter_mgr, get_pending_transfer_requests, []),
    error = orddict:find({Key, Bucket}, PendingTransferRequests2),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0).

process_transfer_on_self_has_no_effect(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter8_mgr_single,
    {_, CommitTime1} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    rpc:call(Node, bcounter_mgr, process_transfer, [{transfer, {{Key, Bucket}, 1, antidote_utils:get_dcid(Node)}}]),
    %%Was a cast so we make sure it was processed by the following calls
    {ok, CommitTime2} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime1, antidote_utils:bcounter_get_increment_op(Node, 5)),
    {ok, CommitTime3} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime2, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, CommitTime3, 0).

process_transfer_to_invalid_dcid_has_no_effect(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter9_mgr_single,
    {_, CommitTime1} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    rpc:call(Node, bcounter_mgr, process_transfer, [{transfer, {{Key, Bucket}, 1, invalid_dcid}}]),
    %%Was a cast so we make sure it was processed by the following calls
    {ok, CommitTime2} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime1, antidote_utils:bcounter_get_increment_op(Node, 5)),
    {ok, CommitTime3} = antidote_utils:bcounter_update_single(Node, Key, Bucket, CommitTime2, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    antidote_utils:bcounter_check_read_value(Node, Key, Bucket, CommitTime3, 0).

crash_recovery_test(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = bcounter10_mgr_single,
    ok = rpc:call(Node, bcounter_mgr, set_transfer_periodic_active, [false]),
    {_, _} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    {error, no_permissions} = antidote_utils:bcounter_update_single(Node, Key, Bucket, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    {error, no_permissions} = antidote_utils:bcounter_update_single(Node, Key, Bucket, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    PendingTransferRequests = rpc:call(Node, bcounter_mgr, get_pending_transfer_requests, []),
    PendingTransferRequestsForKey = orddict:fetch({Key, Bucket}, PendingTransferRequests),
    {[5, 5], _} = lists:unzip(PendingTransferRequestsForKey),

    Pid = rpc:call(Node, erlang, whereis, [bcounter_mgr]),
    rpc:call(Node, erlang, exit, [Pid, "test"]),
    timer:sleep(100), %%Some restart time

    PendingTransferRequests2 = rpc:call(Node, bcounter_mgr, get_pending_transfer_requests, []),
    error = orddict:find({Key, Bucket}, PendingTransferRequests2),

    {_, _} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    {error, no_permissions} = antidote_utils:bcounter_update_single(Node, Key, Bucket, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    {error, no_permissions} = antidote_utils:bcounter_update_single(Node, Key, Bucket, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    %%Check that periodic transfers are working
    timer:sleep(600), %%TODO this is dependent on ?REQUEST_TIMEOUT
    PendingTransferRequests3 = rpc:call(Node, bcounter_mgr, get_pending_transfer_requests, []),
    error = orddict:find({Key, Bucket}, PendingTransferRequests3),


    ok = rpc:call(Node, bcounter_mgr, set_transfer_periodic_active, [false]),
    {_, _} = antidote_utils:bcounter_check_read_value(Node, Key, Bucket, 0),
    {error, no_permissions} = antidote_utils:bcounter_update_single(Node, Key, Bucket, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    {error, no_permissions} = antidote_utils:bcounter_update_single(Node, Key, Bucket, antidote_utils:bcounter_get_decrement_op(Node, 5)),
    PendingTransferRequests4 = rpc:call(Node, bcounter_mgr, get_pending_transfer_requests, []),
    PendingTransferRequestsForKey4 = orddict:fetch({Key, Bucket}, PendingTransferRequests4),
    {[5, 5], _} = lists:unzip(PendingTransferRequestsForKey4),
    ok = rpc:call(Node, bcounter_mgr, set_transfer_periodic_active, [true]).
