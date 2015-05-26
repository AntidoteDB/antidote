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

-record(state, {port, listener}). % the current socket

-export([start_link/2]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([accept/2
        ]).

-define(TIMEOUT,10000).

start_link(Pid, Port) ->
    gen_fsm:start_link(?MODULE, [Pid, Port], []).

init([Pid, Port]) ->
    {ok, ListenSocket} = gen_tcp:listen(
                           Port,
                           [{active,false}, binary,
                            {packet,4},{reuseaddr, true}
                           ]),
    Pid ! ready,
    {ok, accept, #state{port=Port, listener=ListenSocket},0}.

%% Accepts an incoming tcp connection and spawn and new fsm 
%% to process the connection, so that new connections could 
%% be processed in parallel
accept(timeout, State=#state{listener=ListenSocket}) ->
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    {ok, _} = inter_dc_communication_fsm_sup:start_fsm([AcceptSocket]),
    {next_state, accept, State, 0}.

handle_info(Message, _StateName, StateData) ->
    lager:error("Recevied info:  ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD=#state{listener=ListenSocket}) ->
    gen_tcp:close(ListenSocket),
    lager:info("Closing socket"),
    ok.
