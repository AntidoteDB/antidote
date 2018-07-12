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

%% API
-export([
  broadcast/1]).

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
-record(state, {}). %% the Kafka connections are handled by the brod client process under the name antidote_pub

%%%% API --------------------------------------------------------------------+

-spec broadcast(#interdc_txn{}) -> ok.
broadcast(Txn = #interdc_txn{partition = P}) ->
  % lager:info("Sending ~p", [Txn]),
  SimpleIndex = P div (chash:ring_increment(dc_utilities:get_partitions_num())),
  case catch gen_server:call(?MODULE, {publish, SimpleIndex, term_to_binary(Txn, [{compressed, 6}])}) of
    {'EXIT', _Reason} -> lager:warning("Failed to broadcast a transaction."); %% this can happen if a node is shutting down.
    Normal -> Normal
  end.

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  Host = application:get_env(antidote, kafka_host, ?DEFAULT_KAFKA_HOST),
  lager:info("Connecting to Kafka on host ~p", [Host]),
  KafkaBootstrapEndpoints = [{Host, 9092}],
  ok = brod:start_client(KafkaBootstrapEndpoints, antidote_pub),
  ok = brod:start_producer(antidote_pub, <<"interdc">>, _ProducerConfig=[]),
  {ok, #state{}}.

handle_call({publish, Partition, Message}, _From, State) ->
  {reply, brod:produce_sync(antidote_pub, <<"interdc">>, Partition, <<"">>, Message), State}.

terminate(_Reason, _State) ->
  brod:stop_client(antidote_pub).
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
