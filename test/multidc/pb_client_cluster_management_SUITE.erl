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

-spec send_pb_message(pid(), antidote_pb_codec:request()) -> any().
send_pb_message(Pid, Message) ->
    EncMsg = antidote_pb_codec:encode_message(Message),
    send_pb_message_raw(Pid, EncMsg).

send_pb_message_raw(Pid, EncMsg) ->
    antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, infinity}).

%% Single object rea
setup_cluster_test(Config) ->
    NodeNames = [dev1, dev2, dev3, dev4],
    Nodes = [test_utils:start_node(Node, Config) || Node <- NodeNames],
    [Node1, Node2, Node3, Node4] = Nodes,
    [NodeAddr1, NodeAddr2, NodeAddr3, NodeAddr4] = [atom_to_list(N) || N <- Nodes],

    ct:pal("Check1"),

    {ok, Pb1} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(dev1) + 2),
    ct:pal("Check2"),
    Response1 = send_pb_message_raw(Pb1, antidote_pb_codec:encode_create_DC([Node1, Node2])),
    ct:pal("Check3"),
    ct:pal("Response = ~p", [Response1]),
    _Disconnected = antidotec_pb_socket:stop(Pb1),

    Bucket = ?BUCKET_BIN,
    Bound_object = {<<"key1">>, antidote_crdt_counter_pn, Bucket},

    % test whether we can do a simple query:

    {ok, Pb} = antidotec_pb_socket:start(?ADDRESS, test_utils:web_ports(dev1) + 2),
    {ok, TxId} = antidotec_pb:start_transaction(Pb, ignore, []),
    {ok, [Val]} = antidotec_pb:read_objects(Pb, [Bound_object], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pb, TxId),

    _Disconnected = antidotec_pb_socket:stop(Pb),
    ?assertMatch(true, antidotec_counter:is_type(Val)).
