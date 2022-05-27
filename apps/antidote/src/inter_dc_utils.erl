%% -------------------------------------------------------------------
%%
%% Copyright <2013-2020> <
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

-module(inter_dc_utils).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-export([
    get_address/0,
    get_address_list/1,
    close_socket/1,
    get_my_partitions/0,
    generate_random_id/0
]).

%% Provides utility functions for binary inter_dc messages.
-export([
    check_message_version/1,
    check_version_and_req_id/1
]).

-spec get_address() -> inet:ip_address().
get_address() ->
    %% first try resolving our hostname according to the node name
    [_, Hostname] = string:tokens(atom_to_list(erlang:node()), "@"),
    case inet:getaddr(Hostname, inet) of
        {ok, HostIp} ->
            HostIp;
        {error, _} ->
            %% cannot resolve hostname locally, fall back to interface ip
            %% TODO check if we do not return a link-local address
            {ok, List} = inet:getif(),
            {IIp, _, _} = hd(List),
            IIp
    end.

-spec get_address_list(inet:port_number()) -> [socket_address()].
get_address_list(Port) ->
    {ok, List} = inet:getif(),
    List1 = [Ip1 || {Ip1, _, _} <- List],

    %% get host name from node name
    [_, Hostname] = string:tokens(atom_to_list(erlang:node()), "@"),
    IpList =
        case inet:getaddr(Hostname, inet) of
            {ok, HostIp} ->
                [HostIp | List1];
            {error, _} ->
                List1
        end,
    [{Ip1, Port} || Ip1 <- IpList, Ip1 /= {127, 0, 0, 1}].

-spec close_socket(zmq_socket()) -> ok.
close_socket(Socket) ->
    _ = zmq_utils:close_socket(Socket),
    ok.

%% Returns the partition indices hosted by the local (caller) node.
-spec get_my_partitions() -> [partition_id()].
get_my_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:my_indices(Ring).

-spec generate_random_id() -> string().
generate_random_id() ->
    %% slow but not used often
    %% only to open sockets with a unique id
    SeedState = crypto:rand_seed_s(),
    {N, _} = rand:uniform_s(10000000, SeedState),
    integer_to_list(N).

%% --------- binary utilities

%% Check a binary message version for inter_dc messages
%% performed by inter_dc_query_dealer
-spec check_message_version(<<_:?VERSION_BITS, _:_*8>>) -> <<_:_*8>>.
check_message_version(<<Version:?VERSION_BYTES/binary, Rest/binary>>) ->
    %% Only support one version now
    ?MESSAGE_VERSION = Version,
    Rest.

%% Check a binary message version and the message id for inter_dc messages
%% performed by inter_dc_query_dealer
-spec check_version_and_req_id(<<_:?MESSAGE_HEADER_BIT_LENGTH, _:_*8>>) ->
    {<<_:?REQUEST_ID_BIT_LENGTH>>, binary()}.
check_version_and_req_id(Binary) ->
    <<ReqId:?REQUEST_ID_BYTE_LENGTH/binary, Rest/binary>> = check_message_version(Binary),
    {ReqId, Rest}.
