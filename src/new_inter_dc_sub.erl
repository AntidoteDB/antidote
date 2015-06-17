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
-module(new_inter_dc_sub).
-behaviour(gen_server).
-include("antidote.hrl").

-export([start_link/0, add_publishers/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type zmq_socket() :: term(). %% erlzmq_socket()
-record(state, {connections :: [{pub_address(), zmq_socket()}]}).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) -> {ok, #state{connections = []}}.

handle_call({add, DcAddresses}, _From, State) ->
  NewConnections = lists:map(fun(A) -> {A, create_subscriber_socket(A)} end, DcAddresses),
  {reply, ok, State#state{connections = NewConnections ++ State#state.connections}}.

%% Called when a new message is received from any of the publishers.
handle_info({zmq, _Socket, BinaryMsg, _Flags}, State) ->
  Msg = binary_to_term(BinaryMsg),
  lager:info("Received MSG=~p", [Msg]),
  handle_inbound_message(Msg),
  {noreply, State}.

%% Gracefully close all sockets
terminate(_Reason, State) -> lists:foreach(fun({_,S}) -> erlzmq:close(S) end, State#state.connections).
handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

-spec add_publishers(DcAddresses :: [pub_address()]) -> ok.
add_publishers(DcAddresses) -> gen_server:call(?MODULE, {add, DcAddresses}).

%%%%%%%%%%%%%%%

create_subscriber_socket(Address) ->
  {Ip, Port} = Address,
  ConnectionString = lists:flatten(io_lib:format("tcp://~s:~p", [inet_parse:ntoa(Ip), Port])),
  Ctx = zmq_context:get(),
  %% We open the new socket and declare it as active.
  %% This way the messages are redirected to current fsm PID and received by handle_info method.
  {ok, Socket} = erlzmq:socket(Ctx, [sub, {active, true}]),
  ok = erlzmq:connect(Socket, ConnectionString),
  ok = erlzmq:setsockopt(Socket, subscribe, <<>>),
  Socket.

handle_inbound_message(Msg) ->
  case Msg of
    {replicate, Update} -> inter_dc_recvr_vnode:store_updates([Update]);
    _ -> {error, {unknown_message, Msg}}
  end.
