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
    setup_cluster_test/1
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
    setup_cluster_test
].

-spec send_pb_message(pid(), antidote_pb_codec:request()) -> antidote_pb_codec:response_in().
send_pb_message(Pid, Message) ->
    EncMsg = antidote_pb_codec:encode_request(Message),
    ResponseRaw = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, infinity}),
    antidote_pb_codec:decode_response(ResponseRaw).


%% Single object rea
setup_cluster_test(Config) ->
    NodeNames = [clusterdev1, clusterdev2, clusterdev3, clusterdev4],
    Nodes = test_utils:pmap(fun(Node) -> test_utils:start_node(Node, Config) end, NodeNames),
    [Node1, Node2, Node3, Node4] = Nodes,

    % join cluster 1:
    P1 = spawn_link(fun() ->
        {ok, Pb} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev1) + 2),
        ct:pal("joining clusterdev1, clusterdev2"),
        Response = send_pb_message(Pb, {create_dc, [Node1, Node2]}),
        ct:pal("joined clusterdev1, clusterdev2: ~p", [Response]),
        {operation_response,ok} = Response,
        _Disconnected = antidotec_pb_socket:stop(Pb)
    end),

    % join cluster 2:
    P2 = spawn_link(fun() ->
        {ok, Pb} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev3) + 2),
        ct:pal("joining clusterdev3, clusterdev4"),
        Response = send_pb_message(Pb, {create_dc, [Node3, Node4]}),
        ct:pal("joined clusterdev3, clusterdev4: ~p", [Response]),
        {operation_response,ok} = Response,
        _Disconnected = antidotec_pb_socket:stop(Pb)
    end),

    wait_for_process(P1),
    wait_for_process(P2),

    % get descriptor of cluster 2:
    {ok, Pb3} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev3) + 2),
    {get_connection_descriptor_resp, {ok, DescriptorBin3}} = send_pb_message(Pb3, get_connection_descriptor),

    % use descriptor to connect both dcs
    {ok, Pb1} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(clusterdev1) + 2),
    ct:pal("connecting clusters"),
    Response1 = send_pb_message(Pb1, {connect_to_dcs, [DescriptorBin3]}),
    ct:pal("connected clusters: ~p", [Response1]),
    ?assertEqual({operation_response, ok}, Response1),

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
