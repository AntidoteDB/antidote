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

%% InterDC subscriber - connects to RabbitMQ and listens to a defined subset of messages.
%% The messages are filter based on the routing key.

-module(inter_dc_sub).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("brod/include/brod.hrl").

%% Server methods
-export([
  init/1,
  start_link/0,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
  ]).

%% State
-record(state, {}). %% state is managed by the brod Kafka client

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) ->
    Host = application:get_env(antidote, kafka_host, ?DEFAULT_KAFKA_HOST),
    lager:info("Connecting to Kafka on host ~p", [Host]),
    KafkaBootstrapEndpoints = [{Host, 9092}],
    ok = brod:start_client(KafkaBootstrapEndpoints, antidote_sub),
    Indices = lists:map(fun(P)->
            P div (chash:ring_increment(dc_utilities:get_partitions_num()))
        end,
        dc_utilities:get_my_partitions()),
    lager:info("Subscribing to partitions ~p", [Indices]),
    brod_topic_subscriber:start_link(antidote_sub, <<"interdc">>, _Partitions=Indices,
        _ConsumerConfig=[{begin_offset, earliest}],
        _CommittedOffsets=[],
        messages,
        fun(_Partition, #kafka_message{key = _DCId, value = BinaryMsg}, State) ->
            %% decode the message
            Msg = binary_to_term(BinaryMsg),
            LocalDCId = dc_utilities:get_my_dc_id(),
            case Msg#interdc_txn.dcid of
            LocalDCId ->
                % ignore own messages
                done;
            _ ->
                %% deliver the message to an appropriate vnode
                ok = inter_dc_sub_vnode:deliver_txn(Msg),
                done
            end,
            {ok, ack, State}
        end, _InitialState=[]),
    {ok, #state{}}.

handle_call(_Request, _From, Ctx) ->
    {reply, not_implemented, Ctx}.

%% handle an incoming interDC transaction from a remote node.
handle_info(_Msg, State) ->
    {noreply, State}.

handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) ->
  brod:stop_client(antidote_sub).

