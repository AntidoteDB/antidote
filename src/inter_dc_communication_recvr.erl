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

%% @doc : Process an incoming TCP connection request  from remote DCs
%% When a transaction to be replicated from remote DC is received it is 
%% forwarded to inter_dc_repl_vnode to process and store updates

-module(inter_dc_communication_recvr).
-behaviour(gen_fsm).

-record(state, {socket}). % the current socket

-export([start_link/1]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([accept/2,
         wait_for_message/2
         %%stop_server/2
        ]).

-define(TIMEOUT,20000).

start_link(Socket) ->
    gen_fsm:start_link(?MODULE, Socket, []).

init(Socket) ->
    lager:info("started"),
    {ok, accept, #state{socket=Socket},0}.

accept(timeout, State=#state{socket=ListenSocket}) ->
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    ok = inet:setopts(AcceptSocket, [{active, once}]),
    {next_state, wait_for_message, State#state{socket=AcceptSocket}, ?TIMEOUT}.

wait_for_message({replicate,Updates}, State=#state{socket=Socket}) ->
    inter_dc_recvr_vnode:store_updates(Updates),
    _ = gen_tcp:send(Socket, term_to_binary(
                               {acknowledge, inter_dc_manager:get_my_dc()})),
    stop_server(State),
    {stop,normal,State};

wait_for_message(timeout, State) ->
    stop_server(State),
    {stop, normal, State};

wait_for_message(Message, State) ->
    lager:error("Unexpected Message: ~p", [Message]),
    {next_state, wait_for_message, State, ?TIMEOUT}.

stop_server(_State=#state{socket=Socket}) ->
    %% start a new listener
    {ok, _Pid} = inter_dc_communication_sup:start_socket(),
    gen_tcp:close(Socket).

handle_info({tcp, Socket, Bin}, StateName, #state{socket=Socket} = StateData) ->
    %% flow control: enable forwarding of next tcp message
    ok = inet:setopts(Socket, [{active, once}]),
    gen_fsm:send_event(self(),binary_to_term(Bin)),
    {next_state, StateName, StateData};

handle_info({tcp_closed, Socket}, _StateName,
            #state{socket=Socket} = StateData) ->
    lager:debug("TCP disconnect."),
    stop_server(StateData),
    {stop, normal, StateData};

handle_info(Message, _StateName, StateData) ->
    lager:error("Recevied info:  ~p",[Message]),
    stop_server(StateData),
    {stop, normal, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
