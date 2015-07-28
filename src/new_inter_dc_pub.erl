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
-module(new_inter_dc_pub).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-export([start_link/0, broadcast/1, get_address/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {socket, port :: inet:port_number()}). %% socket :: erlzmq_socket()

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  {_, Port} = get_address(),
  Socket = zmq_utils:create_bind_socket(pub, false, Port),
  lager:info("Publisher started on port ~p", [Port]),
  {ok, #state{socket = Socket, port = Port}}.

handle_call({publish, Message}, _From, State) -> {reply, erlzmq:send(State#state.socket, Message), State};
handle_call(get_port, _From, State) -> {reply, State#state.port, State}.

terminate(_Reason, State) -> erlzmq:close(State#state.socket).
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_address() -> socket_address().
get_address() ->
  %% TODO check if we do not return a link-local address
  {ok, List} = inet:getif(),
  {Ip, _, _} = hd(List),
  {ok, Port} = application:get_env(antidote, pubsub_port),
  {Ip, Port}.

broadcast(Txn) ->
  gen_server:call(?MODULE, {publish, new_inter_dc_utils:txn_to_bin(Txn)}).

