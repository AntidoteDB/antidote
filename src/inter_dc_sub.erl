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

%% InterDC subscriber - connects to remote PUB sockets and listens to a defined subset of messages.
%% The messages are filter based on a binary prefix.

-module(inter_dc_sub).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("antidote_channels/include/antidote_channel.hrl").

%% API
-export([
  add_dc/2,
  del_dc/1
]).

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
    channels :: dict:dict(dcid(), pid()) % DCID -> socket
}).

%%%% API --------------------------------------------------------------------+

%% TODO: persist added DCs in case of a node failure, reconnect on node restart.
-spec add_dc(dcid(), [socket_address()]) -> ok.
add_dc(DCID, Publishers) -> gen_server:call(?MODULE, {add_dc, DCID, Publishers}, ?COMM_TIMEOUT).

-spec del_dc(dcid()) -> ok.
del_dc(DCID) -> gen_server:call(?MODULE, {del_dc, DCID}, ?COMM_TIMEOUT).

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) -> {ok, #state{channels = dict:new()}}.

handle_call({add_dc, DCID, Publishers}, _From, OldState) ->
    %% First delete the DC if it is alread connected
    {_, State} = del_dc(DCID, OldState),
    case connect_to_nodes(Publishers, []) of
        {ok, Channels} ->
            %% TODO maybe intercept a situation where the vnode location changes and reflect it in sub socket filer rules,
            %% optimizing traffic between nodes inside a DC. That could save a tiny bit of bandwidth after node failure.
            {reply, ok, State#state{channels = dict:store(DCID, Channels, State#state.channels)}};
        connection_error ->
            {reply, error, State}
    end;

handle_call({del_dc, DCID}, _From, State) ->
    {Resp, NewState} = del_dc(DCID, State),
    {reply, Resp, NewState}.

handle_cast(#interdc_txn{} = Msg, State) ->
    inter_dc_sub_vnode:deliver_txn(Msg),
    {noreply, State};

handle_cast(_Request, State) -> {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, State) ->
    F = fun({_, Channels}) -> lists:foreach(fun antidote_channel:stop/1, Channels) end,
    lists:foreach(F, dict:to_list(State#state.channels)).

del_dc(DCID, State) ->
    case dict:find(DCID, State#state.channels) of
        {ok, Channels} ->
            lists:foreach(fun antidote_channel:stop/1, Channels),
            {ok, State#state{channels = dict:erase(DCID, State#state.channels)}};
        error ->
            {ok, State}
    end.

connect_to_nodes([], Acc) ->
    {ok, Acc};
connect_to_nodes([Node | Rest], Acc) ->
    case connect_to_node(Node) of
        {ok, Channel} ->
            connect_to_nodes(Rest, [Channel | Acc]);
        connection_error ->
            lists:foreach(fun antidote_channel:stop/1, Acc),
            connection_error
    end.

connect_to_node([]) ->
    logger:error("Unable to subscribe to DC"),
    connection_error;
connect_to_node([Address | Rest]) ->
    case antidote_channel:is_alive(channel_zeromq, Address) of
        true ->
            PartBin = lists:map(
                fun(P) ->
                    inter_dc_txn:partition_to_bin(P)
                end, dc_utilities:get_my_partitions()),

            Config = #{
                module => channel_zeromq,
                pattern => pub_sub,
                handler => self(),
                topics => PartBin,
                namespace => <<>>,
                network_params => #{
                    publishersAddresses => [Address]
                }
            },
            antidote_channel:start_link(Config);

        false ->
            connect_to_node(Rest)
    end.
