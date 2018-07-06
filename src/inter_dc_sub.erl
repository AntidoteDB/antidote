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
-include_lib("amqp_client/include/amqp_client.hrl").

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
-record(state, {
  connection, %% amqp_connection
  channel %% amqp_channel
}).

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) ->
    Host = application:get_env(antidote, rabbitmq_host, ?DEFAULT_RABBITMQ_HOST),
    lager:info("Connecting to RabbitMQ on host ~p", [Host]),
    case amqp_connection:start(#amqp_params_network{host=Host}) of
        {ok, Connection} ->
            {ok, Channel} = amqp_connection:open_channel(Connection),
            amqp_channel:call(Channel, #'exchange.declare'{exchange = <<"transactions">>,
                                                            type = <<"direct">>}),
            #'queue.declare_ok'{queue = Queue} =
                amqp_channel:call(Channel, #'queue.declare'{exclusive = true}),
            lists:foreach(fun(P) ->
                            RoutingKey = list_to_binary(io_lib:format("P~p", [P])),
                            lager:info("Subscribing to routing key ~p", [RoutingKey]),
                            amqp_channel:call(Channel, #'queue.bind'{exchange = <<"transactions">>,
                                                                    routing_key = RoutingKey,
                                                                    queue = Queue})
                        end, dc_utilities:get_my_partitions()),
            amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue}, self()),
            receive
                #'basic.consume_ok'{} ->

                    lager:info("Subscriber started"),
                    {ok, #state{connection = Connection, channel = Channel}}
            end;
        {error, Error} ->
            lager:error("Error connecting to RabbitMQ: ~p", [Error]),
            {error, Error}
    end.

handle_call(_Request, _From, Ctx) ->
    {reply, not_implemented, Ctx}.

%% handle an incoming interDC transaction from a remote node.
handle_info({#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = BinaryMsg}}, State) ->
  %% decode the message
  Msg = binary_to_term(BinaryMsg),
  LocalDCId = dc_utilities:get_my_dc_id(),
  case Msg#interdc_txn.dcid of
    LocalDCId ->
        % ignore own messages
        {noreply, State};
    _ ->
        %% deliver the message to an appropriate vnode
        ok = inter_dc_sub_vnode:deliver_txn(Msg),
        amqp_channel:cast(State#state.channel, #'basic.ack'{delivery_tag = Tag}),
        {noreply, State}
  end.

handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, State) ->
  amqp_channel:close(State#state.channel),
  amqp_connection:close(State#state.connection).

