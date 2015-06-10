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
-module(pubsub_receiver).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {sockets :: list(), queue :: queue()}).

start_link() -> gen_server:start_link(?MODULE, [], []).

init([]) -> {ok, #state{sockets = [], queue = queue:new()}}.

handle_call({add, {Ip, Port}}, _From, State) ->
  Address = lists:flatten(io_lib:format("tcp://~s:~p", [Ip, Port])),
  Ctx = zmq_context:get(),
  %% We open the new socket and declare it as active.
  %% This way the messages are redirected to current fsm PID and received by handle_info method.
  {ok, Socket} = erlzmq:socket(Ctx, [sub, {active, true}]),
  ok = erlzmq:connect(Socket, Address),
  ok = erlzmq:setsockopt(Socket, subscribe, <<>>),
  Sockets = [Socket] ++ State#state.sockets,
  {reply, ok, State#state{sockets = Sockets}};

handle_call(listen, _From, State) ->
  case queue:out(State#state.queue) of
    {{value, Msg}, Q} -> {reply, {ok, Msg}, State#state{queue = Q}};
    {empty, _} -> {reply, none, State}
  end.

%% Called when a new message is received from any of the publishers.
handle_info({zmq, _Socket, BinaryMsg, _Flags}, State) ->
  Msg = binary_to_term(BinaryMsg),
  {noreply, State#state{queue = queue:in(Msg, State#state.queue)}}.

%% Gracefully close all sockets
terminate(_Reason, State) ->
  %% close all the sockets
  Results = lists:map(fun erlzmq:close/1, State#state.sockets),
  IsOk = fun(X) -> X == ok end,
  case lists:all(IsOk, Results) of
    true -> ok;
    false -> {error, Results}
  end.

handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

