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
-module(update_stream_pub).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-export([start_link/0, broadcast/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {socket, port :: inet:port_number()}). %% socket :: erlzmq_socket()

start_link() ->
  {ok, Port} = application:get_env(antidote, updatestream_port),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [Port], []).

init([Port]) ->
  Socket = zmq_utils:create_bind_socket(pub, false, Port),
  lager:info("Update stream publisher started on port ~p", [Port]),
  {ok, #state{socket = Socket, port = Port}}.

-spec broadcast(#interdc_txn{}) -> ok.
broadcast(Txn) ->
  lists:foreach(fun broadcast_op/1, inter_dc_txn:ops_by_type(Txn, update)).

broadcast_op(Op) ->
  %% Each message is prefixed with the SHA1 hash of the key.
  %% Subscribers can filter the operation stream and get only a subset, related to chosen keys.
  LogRecord = Op#operation.payload,
  Payload = LogRecord#log_record.op_payload,
  {Key, _, _} = Payload,
  Prefix = crypto:hash(sha, term_to_binary(Key)),
  BinMsg = term_to_binary(Payload),
  gen_server:call(?MODULE, {publish, <<Prefix/binary, BinMsg/binary>>}).

handle_call({publish, Message}, _From, State) -> {reply, erlzmq:send(State#state.socket, Message), State}.
terminate(_Reason, State) -> erlzmq:close(State#state.socket).
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
