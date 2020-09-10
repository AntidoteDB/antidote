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
    get_my_partitions/0
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
    catch (gen_server:stop(Socket)),
    close_socket(Socket, true).

close_socket(Socket, true) ->
%%    logger:warning("Waiting until dead: ~p",[Socket]),
    timer:sleep(50),
    close_socket(Socket, is_process_alive(Socket));
close_socket(Socket, false) ->
%%    logger:warning("Socket closed and Pid dead: ~p",[Socket]),
    ok.


%% Returns the partition indices hosted by the local (caller) node.
-spec get_my_partitions() -> [partition_id()].
get_my_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:my_indices(Ring).
