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

%% InterDC publisher - holds a ZeroMQ PUB socket and makes it available for Antidote processes.
%% This vnode is used to publish interDC transactions.

-module(inter_dc_pub).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([
  broadcast/1,
  get_address/0,
  get_address_list/0]).

%% Server methods
-export([
  init/1,
  start_link/0,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

%% State
-record(state, {socket}). %% socket :: erlzmq_socket()

%%%% API --------------------------------------------------------------------+

-spec get_address() -> socket_address().
get_address() ->
  %% first try resolving our hostname according to the node name
  [_, Hostname] = string:tokens(atom_to_list(erlang:node()), "@"),
  Ip = case inet:getaddr(Hostname, inet) of
    {ok, HostIp} -> HostIp;
    {error, _} ->
      %% cannot resolve hostname locally, fall back to interface ip
      %% TODO check if we do not return a link-local address
      {ok, List} = inet:getif(),
      {IIp, _, _} = hd(List),
      IIp
  end,
  Port = application:get_env(antidote, pubsub_port, ?DEFAULT_PUBSUB_PORT),
  {Ip, Port}.

-spec get_address_list() -> [socket_address()].
get_address_list() ->
    {ok, List} = inet:getif(),
    List1 = [Ip1 || {Ip1, _, _} <- List],
    %% get host name from node name
    [_, Hostname] = string:tokens(atom_to_list(erlang:node()), "@"),
    IpList = case inet:getaddr(Hostname, inet) of
      {ok, HostIp} -> [HostIp|List1];
      {error, _} -> List1
    end,
    Port = application:get_env(antidote, pubsub_port, ?DEFAULT_PUBSUB_PORT),
    [{Ip1, Port} || Ip1 <- IpList, Ip1 /= {127, 0, 0, 1}].

-spec broadcast(#interdc_txn{}) -> ok.
broadcast(Txn) ->
  case catch gen_server:call(?MODULE, {publish, inter_dc_txn:to_bin(Txn)}) of
    {'EXIT', _Reason} -> lager:warning("Failed to broadcast a transaction."); %% this can happen if a node is shutting down.
    Normal -> Normal
  end.

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  {_, Port} = get_address(),
  Socket = zmq_utils:create_bind_socket(pub, false, Port),
  lager:info("Publisher started on port ~p", [Port]),
  {ok, #state{socket = Socket}}.

handle_call({publish, Message}, _From, State) -> {reply, erlzmq:send(State#state.socket, Message), State}.

terminate(_Reason, State) -> erlzmq:close(State#state.socket).
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
