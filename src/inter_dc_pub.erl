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

%% InterDC publisher - holds a connection to RabbitMQ and makes it available for Antidote processes.
%% This vnode is used to publish interDC transactions.

-module(inter_dc_pub).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% API
-export([
  broadcast/1,
  send_retry/0]).

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
-record(state, {connection, channel, connected}). %% connection :: amqp_connection, channel :: amqp_channel

%%%% API --------------------------------------------------------------------+

-spec broadcast(#interdc_txn{}) -> ok.
broadcast(Txn = #interdc_txn{partition = P}) ->
  RoutingKey = list_to_binary(io_lib:format("P~p", [P])),
  case catch gen_server:call(?MODULE, {publish, RoutingKey, term_to_binary(Txn, [{compressed, 6}])}) of
    {'EXIT', _Reason} -> lager:warning("Failed to broadcast a transaction."); %% this can happen if a node is shutting down.
    Normal -> Normal
  end.

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  maybe_connect().

maybe_connect() ->
  Host = application:get_env(antidote, rabbitmq_host, ?DEFAULT_RABBITMQ_HOST),
  lager:info("Connecting to RabbitMQ on host ~p", [Host]),
  case amqp_connection:start(#amqp_params_network{host=Host}) of
    {ok, Connection} ->
      {ok, Channel} = amqp_connection:open_channel(Connection),
      amqp_channel:call(Channel, #'exchange.declare'{exchange = <<"transactions">>,
                                                      type = <<"direct">>}),
      lager:info("Publisher started"),
      {ok, #state{connection = Connection, channel = Channel, connected = true}};
    {error, Error} ->
      lager:error("Error connecting to RabbitMQ: ~p", [Error]),
      % set timeout to try again
      timer:apply_after(?MESSAGING_RETRY_TIME, ?MODULE, send_retry, []),
      {ok, #state{connected = false}}
  end.

send_retry() ->
  gen_server:cast(?MODULE, try_connect).

handle_call({publish, Partition, Message}, _From, State=#state{connected = Connected}) ->
  case Connected of
    true ->
      {reply, amqp_channel:cast(State#state.channel, #'basic.publish'{
                                                                      exchange = <<"transactions">>,
                                                                      routing_key = Partition
                                                                    },
                                                                    #amqp_msg{payload = Message}), State};
    false ->
      {reply, ok, State}
  end.

terminate(_Reason, State = #state{connected = Connected}) ->
  case Connected of
    true ->
      amqp_channel:close(State#state.channel),
      amqp_connection:close(State#state.connection);
    false ->
      ok
  end.

handle_cast(try_connect, State=#state{connected = false}) ->
  case maybe_connect() of
    {ok, NewState} ->
      {noreply, NewState};
    {error, _Error} ->
      {noreply, State}
  end;
handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
