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

-module(zmq_utils).

-export([create_connect_socket/3, create_bind_socket/3, sub_filter/2, close_socket/1]).

create_socket(Type, Active) ->
  Ctx = zmq_context:get(),
  Result = case Active of
    true -> erlzmq:socket(Ctx, [Type, {active, true}]);
    false -> erlzmq:socket(Ctx, Type)
  end,
  case Result of
    {ok, Socket} -> Socket;
    _ -> throw(failed_to_create_zmq_socket)
  end.

create_connect_socket(Type, Active, Address) ->
  Socket = create_socket(Type, Active),
  ok = erlzmq:connect(Socket, connection_string(Address)),
  Socket.

create_bind_socket(Type, Active, Port) ->
  Socket = create_socket(Type, Active),
  ok = erlzmq:bind(Socket, connection_string({"*", Port})),
  Socket.

connection_string({Ip, Port}) ->
  IpString = case Ip of
    "*" -> Ip;
    _ -> inet_parse:ntoa(Ip)
  end,
  lists:flatten(io_lib:format("tcp://~s:~p", [IpString, Port])).

sub_filter(Socket, Prefix) ->
  erlzmq:setsockopt(Socket, subscribe, Prefix).

close_socket(Socket) ->
  erlzmq:close(Socket).
