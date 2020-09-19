%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% InterDC publisher - holds a ZeroMQ PUB socket and makes it available for Antidote processes.
%% This process is used to publish only valid interDC transactions records #interdc_txn.
%% It prepends all publish messages with a "P" char as a binary byte as a topic delimiter.

-module(inter_dc_pub).

-behaviour(gen_server).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-include_lib("kernel/include/logger.hrl").

%% API
-export([broadcast/1, get_address/0, get_address_list/0]).

%% Server methods
-export([init/1, start_link/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% State
-record(state, {socket :: zmq_socket()}).

%%%% API --------------------------------------------------------------------+

-spec broadcast(interdc_txn()) -> ok.
broadcast(Txn) ->
    BinTxn = inter_dc_txn:to_bin(Txn),
    ok = gen_server:call(?MODULE, {publish, <<<<"P">>/binary, BinTxn/binary>>}).

-spec get_address() -> socket_address().
get_address() ->
    Ip = inter_dc_utils:get_address(),
    {Ip, get_pub_port()}.

-spec get_address_list() -> [socket_address()].
get_address_list() ->
    inter_dc_utils:get_address_list(get_pub_port()).

%%%% Server methods ---------------------------------------------------------+

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% the chumak socket needs to be cleaned up explicitly
    %% gen_servers that are part of a supervision tree need to trap exit signals
    %% so that terminate/2 is called
    %% https://erlang.org/doc/design_principles/gen_server_concepts.html
    process_flag(trap_exit, true),

    % bind on ip and port
    Ip = get_pub_bind_ip(),
    Port = get_pub_port(),

    {ok, Socket} = chumak:socket(pub),
    {ok, Pid} = chumak:bind(Socket, tcp, Ip, Port),

    ?LOG_NOTICE("InterDC publisher started on port ~p binding on IP ~s (Pid ~p)", [Port, Ip, Pid]),
    {ok, #state{socket = Socket}}.

handle_call({publish, Message}, _From, State) ->
    ok = chumak:send(State#state.socket, Message),
    {reply, ok, State}.

terminate(_Reason, State) ->
    inter_dc_utils:close_socket(State#state.socket),
    ok.

handle_cast(_Request, State) ->
    {noreply, State}.

%% bind shutdown
handle_info({'EXIT', Pid, Reason}, State) ->
    ?LOG_CRITICAL("Bind router socket ~p shutdown: ~p", [Pid, Reason]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%% Internal --------------------------------------------------------------------+

-spec get_pub_port() -> inet:port_number().
get_pub_port() ->
    application:get_env(antidote, pubsub_port, ?DEFAULT_PUBSUB_PORT).

-spec get_pub_bind_ip() -> string().
get_pub_bind_ip() ->
    application:get_env(antidote, pubsub_bind_ip, "0.0.0.0").
