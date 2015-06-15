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
-module(new_inter_dc_publisher).
-behaviour(gen_server).
-include("antidote.hrl").

-export([start_link/1, start_link/0, broadcast/1, get_address/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {socket, port :: inet:port_number()}). %% socket :: erlzmq_socket()

start_link() ->
  {ok, Port} = application:get_env(antidote, pubsub_port), %% default port number provided in app.config
  start_link(Port).

start_link(Port) ->
  gen_server:start_link({global, ?MODULE}, ?MODULE, [Port], []).

init([Port]) ->
  Socket = create_socket(Port),
  lager:info("Publisher started on port ~p", [Port]),
  {ok, #state{socket = Socket, port = Port}}.

handle_call({publish, Message}, _From, State) ->
  Status = erlzmq:send(State#state.socket, Message),
  {reply, Status, State};

handle_call(get_port, _From, State) ->
  {reply, State#state.port, State}.

terminate(_Reason, State) -> erlzmq:close(State#state.socket).
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

broadcast(Message) -> gen_server:call({global, ?MODULE}, {publish, term_to_binary(Message)}).

-spec get_address() -> dc_address().
get_address() ->
  %% TODO check if we do not return a link-local address
  {ok, List} = inet:getif(),
  {Ip, _, _} = hd(List),
  Port = gen_server:call({global, ?MODULE}, get_port),
  {Ip, Port}.

create_socket(Port) ->
  Ctx = zmq_context:get(),
  {ok, Socket} = erlzmq:socket(Ctx, pub),
  ConnectionString = lists:flatten(io_lib:format("tcp://~s:~p", ["*", Port])),
  ok = erlzmq:bind(Socket, ConnectionString),
  Socket.
