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

%% @doc Provides utilities for creating/closing zeromq sockets which are used
%%      by interdc processes.

-module(zmq_utils).

-export([create_connect_socket/3, create_bind_socket/3, sub_filter/2, close_socket/1]).

-type address() :: {string(), integer()}.

-spec create_socket(erlzmq:erlzmq_socket_type(), boolean()) -> no_return() | erlzmq:erlzmq_socket().
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

-spec create_connect_socket(erlzmq:erlzmq_socket_type(), boolean(), address()) -> erlzmq:erlzmq_socket().
create_connect_socket(Type, Active, Address) ->
  Socket = create_socket(Type, Active),
  ok = erlzmq:connect(Socket, connection_string(Address)),
  Socket.

-spec create_bind_socket(erlzmq:erlzmq_socket_type(), boolean(), integer()) -> erlzmq:erlzmq_socket().
create_bind_socket(Type, Active, Port) ->
  Socket = create_socket(Type, Active),
  ok = erlzmq:bind(Socket, connection_string({"*", Port})),
  Socket.

-spec connection_string({string(), integer()}) -> erlzmq:erlzmq_endpoint().
connection_string({Ip, Port}) ->
  IpString = case Ip of
    "*" -> Ip;
    _ -> inet_parse:ntoa(Ip)
  end,
  lists:flatten(io_lib:format("tcp://~s:~p", [IpString, Port])).

-spec sub_filter(erlzmq:erlzmq_socket(), binary()) -> ok.
sub_filter(Socket, Prefix) ->
  ok = erlzmq:setsockopt(Socket, subscribe, Prefix).

-spec close_socket(erlzmq:erlzmq_socket()) -> ok.
close_socket(Socket) ->
  erlzmq:close(Socket).
