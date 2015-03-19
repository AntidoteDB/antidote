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

handle_call({start_receiver, Port}, _From, State) ->
    {ok, _} = antidote_sup:start_rep(self(), Port),
    receive
        ready -> {reply, {ok, my_dc(Port)}, State#state{port=Port}}
    end;

handle_call(get_dcs, _From, #state{dcs=DCs} = State) ->
    {reply, {ok, DCs}, State};

handle_call({add_dc, OtherDC}, _From, #state{dcs=DCs} = State) ->
    NewDCs = add_dc(OtherDC, DCs),
    {reply, ok, State#state{dcs=NewDCs}};

handle_call({add_list_dcs, OtherDCs}, _From, #state{dcs=DCs} = State) ->
    NewDCs = add_dcs(OtherDCs, DCs),
    {reply, ok, State#state{dcs=NewDCs}}.

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

my_dc(DcPort) ->
    {ok, List} = inet:getif(),
    {Ip, _, _} = hd(List),
    DcIp = inet_parse:ntoa(Ip),
    DcId = dc_utilities:get_my_dc_id(),
    {DcId, {DcIp, DcPort}}.

add_dc({DcId, DcAddress}, DCs) -> 
    orddict:store(DcId, DcAddress, DCs).

add_dcs(OtherDCs, DCs) ->
    lists:foldl(fun add_dc/2, DCs, OtherDCs).
