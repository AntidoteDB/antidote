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

-module(pb_client_cluster_management_SUITE).

%% common_test callbacks
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([
    setup_cluster_test/1,
    setup_single_dc_handoff_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ADDRESS, "localhost").
-define(PORT, 10017).
-define(BUCKET_BIN, term_to_binary(test_utils:bucket(pb_client_bucket))).


init_per_suite(_Config) ->
    [].


end_per_suite(Config) ->
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.


all() -> [
    setup_cluster_test,
    setup_single_dc_handoff_test
].


setup_single_dc_handoff_test(Config) ->
    NodeNames = [clusterdev5, clusterdev6],
    Nodes = test_utils:pmap(fun(Node) -> test_utils:start_node(Node, Config) end, NodeNames),
    [Node1, Node2] = test_utils:unpack(Nodes),

    % write counter on clusterdev5:
    Bucket = ?BUCKET_BIN,
    Bound_object = {<<"handoff_test">>, antidote_crdt_counter_pn, Bucket},
    {ok, Pb1} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev5) + 2),
    {ok, TxId4} = antidotec_pb:start_transaction(Pb1, ignore, []),
    ok = antidotec_pb:update_objects(Pb1, [{Bound_object, increment, 4242}], TxId4),
    {ok, CommitTime} = antidotec_pb:commit_transaction(Pb1, TxId4),
    ct:pal("Wrote value to counter"),
    _Disconnected3 = antidotec_pb_socket:stop(Pb1),

    % join cluster:
    P1 = spawn_link(fun() ->
        {ok, Pb} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev5) + 2),
        ct:pal("joining clusterdev5, clusterdev6"),
        Response = antidotec_pb_management:create_dc(Pb, [Node1, Node2]),
        ct:pal("joined clusterdev5, clusterdev6: ~p", [Response]),
        ?assertEqual(ok, Response),
        _Disconnected = antidotec_pb_socket:stop(Pb)
                    end),
    wait_for_process(P1),
    {ok, Pb2} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev6) + 2),

    % wait for converged ring
    riak_utils:wait_until_no_pending_changes([Node1, Node2]),

    % node 1 leave cluster, force data to transfer to Node2
    rpc:call(Node1, antidote_dc_manager, leave, []),

    ct:pal("Waiting for node to leave the cluster"),

    % wait until Node1 down (and therefore handoff to be complete)
    time_utils:wait_until_offline(Node1),

    ct:pal("Node left cluster successfully"),

    % read data from Node2
    {ok, TxId1} = antidotec_pb:start_transaction(Pb2, CommitTime, []),
    {ok, [Counter]} = antidotec_pb:read_objects(Pb2, [Bound_object], TxId1),
    {ok, _} = antidotec_pb:commit_transaction(Pb2, TxId1),

    ?assertMatch(true, antidotec_counter:is_type(Counter)),
    ?assertEqual(4242, antidotec_counter:value(Counter)),

    _Disconnected3 = antidotec_pb_socket:stop(Pb2).

setup_cluster_test(Config) ->
    NodeNames = [clusterdev1, clusterdev2, clusterdev3, clusterdev4],
    Nodes = test_utils:pmap(fun(Node) -> test_utils:start_node(Node, Config) end, NodeNames),
    [Node1, Node2, Node3, Node4] = test_utils:unpack(Nodes),

    % join cluster 1:
    P1 = spawn_link(fun() ->
        {ok, Pb} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev1) + 2),
        ct:pal("joining clusterdev1, clusterdev2"),

        Response = antidotec_pb_management:create_dc(Pb, [Node1, Node2]),
        ct:pal("joined clusterdev1, clusterdev2: ~p", [Response]),
        ?assertEqual(ok, Response),
        _Disconnected = antidotec_pb_socket:stop(Pb)
    end),

    % join cluster 2:
    P2 = spawn_link(fun() ->
        {ok, Pb} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev3) + 2),
        ct:pal("joining clusterdev3, clusterdev4"),
        Response = antidotec_pb_management:create_dc(Pb, [Node3, Node4]),
        ct:pal("joined clusterdev3, clusterdev4: ~p", [Response]),
        ?assertEqual(ok, Response),
        _Disconnected = antidotec_pb_socket:stop(Pb)
    end),

    wait_for_process(P1),
    wait_for_process(P2),

    % get descriptor of cluster 2:
    {ok, Pb3} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev3) + 2),
    {ok, DescriptorBin3} = antidotec_pb_management:get_connection_descriptor(Pb3),

    % use descriptor to connect both dcs
    {ok, Pb1} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev1) + 2),
    ct:pal("connecting clusters"),
    Response1 = antidotec_pb_management:connect_to_dcs(Pb1, [DescriptorBin3]),
    ct:pal("connected clusters: ~p", [Response1]),
    ?assertEqual(ok, Response1),

    Bucket = ?BUCKET_BIN,
    Bound_object = {<<"key1">>, antidote_crdt_counter_pn, Bucket},

    % write counter on clusterdev4:
    {ok, Pb4} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev4) + 2),
    {ok, TxId4} = antidotec_pb:start_transaction(Pb3, ignore, []),
    ok = antidotec_pb:update_objects(Pb3, [{Bound_object, increment, 4711}], TxId4),
    {ok, CommitTime} = antidotec_pb:commit_transaction(Pb3, TxId4),

    % read counter on clusterdev1
    {ok, TxId1} = antidotec_pb:start_transaction(Pb1, CommitTime, []),
    {ok, [Counter]} = antidotec_pb:read_objects(Pb1, [Bound_object], TxId1),
    {ok, _} = antidotec_pb:commit_transaction(Pb1, TxId1),

    ?assertMatch(true, antidotec_counter:is_type(Counter)),
    ?assertEqual(4711, antidotec_counter:value(Counter)),

    _Disconnected1 = antidotec_pb_socket:stop(Pb1),
    _Disconnected3 = antidotec_pb_socket:stop(Pb3),
    _Disconnected3 = antidotec_pb_socket:stop(Pb4).


wait_for_process(P) ->
    MonitorRef = monitor(process, P),
    receive
        {_Tag, MonitorRef, _Type, _Object, _Info} -> ok
    end.
