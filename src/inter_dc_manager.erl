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

-module(inter_dc_manager).
-behaviour(gen_server).

-include("antidote.hrl").

-export([start_link/0,
         get_my_dc/0,
         start_receiver/1,
         get_dcs/0,
         add_dc/1,
         add_list_dcs/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-record(state, {
        dcs,
        port
    }).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_my_dc() ->
    gen_server:call(?MODULE, get_my_dc, infinity).

%% Starts listening to TCP port for incomming requests from other DCs
%% Returns the address of the DC, which could be used by others to communicate
-spec start_receiver(port()) -> {ok, dc_address()}.
start_receiver(Port) ->
    gen_server:call(?MODULE, {start_receiver, Port}, infinity).

%% Returns all DCs known to this DC.
-spec get_dcs() ->{ok, [dc_address()]}.
get_dcs() ->
    gen_server:call(?MODULE, get_dcs, infinity).

%% Add info about a new DC. This info could be
%% used by other modules to communicate to other DC
-spec add_dc(dc_address()) -> ok.
add_dc(NewDC) ->
    gen_server:call(?MODULE, {add_dc, NewDC}, infinity).

%% Add a list of DCs to this DC
-spec add_list_dcs([dc_address()]) -> ok.
add_list_dcs(DCs) ->
    gen_server:call(?MODULE, {add_list_dcs, DCs}, infinity).


%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    {ok, #state{dcs=[]}}.

handle_call(get_my_dc, _From, #state{port=Port} = State) ->
    {reply, {ok, {my_ip(),Port}}, State};

handle_call({start_receiver, Port}, _From, State) ->
    {ok, _} = antidote_sup:start_rep(Port),
    {reply, {ok, {my_ip(),Port}}, State#state{port=Port}};

handle_call(get_dcs, _From, #state{dcs=DCs} = State) ->
    {reply, {ok, DCs}, State};

handle_call({add_dc, NewDC}, _From, #state{dcs=DCs0} = State) ->
    DCs = DCs0 ++ [NewDC],
    {reply, ok, State#state{dcs=DCs}};

handle_call({add_list_dcs, DCs}, _From, #state{dcs=DCs0} = State) ->
    DCs1 = DCs0 ++ DCs,
    {reply, ok, State#state{dcs=DCs1}}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

my_ip() ->
    {ok, List} = inet:getif(),
    {Ip, _, _} = hd(List),
    inet_parse:ntoa(Ip).
