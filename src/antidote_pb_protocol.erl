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

-module(antidote_pb_protocol).
% This module handles the protocol buffer protocol.
% It provides callbacks used by the ranch library.

-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

start_link(Ref, Socket, Transport, Opts) ->
  Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
  {ok, Pid}.

init(Ref, Socket, Transport, _Opts) ->
  ok = ranch:accept_ack(Ref),
  % Each message starts with 4 byte denoting the length of the
  % package. The setting {packet, 4} tells the socket library
  % to use this encoding (it is one of the builtin protocols of Erlang)
  ok = Transport:setopts(Socket, [{packet, 4}]),
  loop(Socket, Transport).

% Receive-Respond loop for handling connections:
loop(Socket, Transport) ->
  case Transport:recv(Socket, 0, infinity) of
    {ok, Data} ->
      handle(Socket, Transport, Data),
      loop(Socket, Transport);
    {error, closed} ->
      ok = Transport:close(Socket);
    {error, timeout} ->
      logger:info("Socket timed out~n"),
      ok = Transport:close(Socket);
    {error, Reason} ->
      logger:error("Socket error: ~p~n", [Reason]),
      ok = Transport:close(Socket)
  end.


% handles a single request
-spec handle(_Socket, _Transport, binary()) -> ok.
handle(Socket, Transport, Msg) ->
  % A message consists of an 8 bit message code and the actual protocol buffer message:
  <<MsgCode:8, ProtoBufMsg/bits>> = Msg,
  DecodedMessage = antidote_pb_codec:decode_message(antidote_pb_codec:decode_msg(MsgCode, ProtoBufMsg)),
  try
    Response = antidote_pb_process:process(DecodedMessage),
    PbResponse = antidote_pb_codec:encode_message(Response),
    PbMessage = antidote_pb_codec:encode_msg(PbResponse),
    ok = Transport:send(Socket, PbMessage)
  catch
    ExceptionType:Error ->
      % log errors and reply with error message:
      logger:error("Error ~p: ~p~nWhen handling request ~p~n", [ExceptionType, Error, DecodedMessage]),
      % when formatting the error message, we use a maximum depth of 9001.
      % This should be big enough to include useful information, but avoids sending a lot of data
      MessageStr = erlang:iolist_to_binary(io_lib:format("~P: ~P~n", [ExceptionType, 9001, Error, 9001])),
      Message = antidote_pb_codec:encode_msg(antidote_pb_codec:encode_message({error_response, {unknown, MessageStr}})),
      ok = Transport:send(Socket, Message),
      ok
  end.
