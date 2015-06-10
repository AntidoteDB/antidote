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
-module(pubsub_sender).
-behaviour(gen_server).

-export([start_link/1, broadcast/1, start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-record(state, {socket}).

start_link() ->
  Ctx = zmq_context:get(),
  {ok, Socket} = erlzmq:socket(Ctx, pub),
  {ok, Port} = application:get_env(antidote, pubsub_port),
  ConnectionString = lists:flatten(io_lib:format("tcp://~s:~p", ["*", Port])),
  lager:info("Publisher starting on port ~p", [Port]),
  ok = erlzmq:bind(Socket, ConnectionString),
  start_link(Socket).

start_link(Socket) -> gen_server:start_link({local, ?SERVER}, ?MODULE, [Socket], []).

init([Socket]) -> {ok, #state{socket = Socket}}.

handle_call({publish, Message}, _From, State) ->
  Status = erlzmq:send(State#state.socket, Message),
  {reply, Status, State}.

terminate(_Reason, State) -> erlzmq:close(State#state.socket).
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

broadcast(Message) -> gen_server:call(?SERVER, {publish, term_to_binary(Message)}).


