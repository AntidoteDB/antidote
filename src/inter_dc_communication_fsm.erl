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

-module(inter_dc_communication_fsm).
-behaviour(gen_fsm).

%% This module handles incoming socket connections requests from other DC.
%% Currently handles only "replicate update" requests.
%% Other request messages must be added if required

-record(state, {socket}). % the current socket

-export([start_link/1]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([receive_message/2,
         close_socket/2
        ]).

-define(TIMEOUT,10000).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(Socket) ->
    gen_fsm:start_link(?MODULE, [Socket], []).


%% ===================================================================
%% gen_fsm callbacks
%% ===================================================================

init([Socket]) ->
    {ok, receive_message, #state{socket=Socket},0}.


receive_message(timeout, State=#state{socket=Socket}) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Message} ->
            case binary_to_term(Message) of
                {replicate, Updates} ->
                    ok =  inter_dc_recvr_vnode:store_updates(Updates) ,
                    ok = gen_tcp:send(Socket, term_to_binary(acknowledge));
                Unknown -> %% Add more messages to be handled
                    lager:error("Weird message received ~p", [Unknown])
            end;
        {error, Reason} ->
            lager:error("Problem with the socket, reason: ~p", [Reason])
    end,
    {next_state, close_socket,State,0}.

close_socket(timeout, State=#state{socket=Socket}) ->
    gen_tcp:close(Socket),
    {stop, normal, State}.

handle_info(Message, _StateName, StateData) ->
    lager:error("Recevied info:  ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
