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

-module(meta_data_manager).
-behaviour(gen_server).

-include("antidote.hrl").

-export([start_link/0,
	 generate_server_name/1,
	 remove_node/1,
	 update_time/3]).
-export([init/1,
	 handle_cast/2,
	 handle_call/3,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-record(state, {
	  table}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({global,generate_server_name(node())}, ?MODULE, [], []).

%% Add a list of DCs to this DC
-spec update_time(atom(),atom(),dict()) -> ok.
update_time(DestinationNodeId,NodeId,Time) ->
    gen_server:cast({global,generate_server_name(DestinationNodeId)}, {update_time, NodeId, Time}).

-spec remove_node(atom()) -> ok.
remove_node(NodeId) ->
    gen_server:cast({global,generate_server_name(node())}, {remove_node,NodeId}).


%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    Table = ets:new(?REMOTE_META_TABLE_NAME, [set, named_table, protected, ?META_TABLE_CONCURRENCY]),
    {ok, #state{table=Table}}.

handle_cast({update_time, NodeId, Time}, State) ->
    true = ets:insert(?REMOTE_META_TABLE_NAME, {NodeId, Time}),
    {noreply, State};

handle_cast({remove_node,NodeId}, State) ->
    lager:info("removing node ~p from meta data table", [NodeId]),
    true = ets:delete(?REMOTE_META_TABLE_NAME, NodeId),
    {noreply, State};

%% handle_call({update_stable}, _From, State = #state{time_dict=TimeDict, last_time=LastTime}) ->
%%     NewTime = update_stable(TimeDict,LastTime),
%%     {ok, State#state{last_time=NewTime}}.

%% handle_call({start_receiver, Port}, _From, State) ->
%%     {ok, _} = antidote_sup:start_rep(self(), Port),
%%     receive
%%         ready -> {reply, {ok, my_dc(Port)}, State#state{port=Port}}
%%     end;

%% handle_call({stop_receiver}, _From, State) ->
%%     case antidote_sup:stop_rep() of
%%         ok -> 
%%             {reply, ok, State#state{port=0}}
%%     end;

%% handle_call(get_dcs, _From, #state{dcs=DCs} = State) ->
%%     {reply, {ok, DCs}, State};

%% handle_call({add_dc, OtherDC}, _From, #state{dcs=DCs} = State) ->
%%     NewDCs = add_dc(OtherDC, DCs),
%%     {reply, ok, State#state{dcs=NewDCs}};

%% handle_call({add_list_dcs, OtherDCs}, _From, #state{dcs=DCs} = State) ->
%%     NewDCs = add_dcs(OtherDCs, DCs),
%%     {reply, ok, State#state{dcs=NewDCs}}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_call(_Info, _From, State) ->
    {ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

generate_server_name(Node) ->
    list_to_atom("meta_manager" ++ atom_to_list(Node)).


%% my_dc(DcPort) ->
%%     {ok, List} = inet:getif(),
%%     {Ip, _, _} = hd(List),
%%     DcIp = inet_parse:ntoa(Ip),
%%     DcId = dc_utilities:get_my_dc_id(),
%%     {DcId, {DcIp, DcPort}}.

%% add_dc({DcId, DcAddress}, DCs) -> 
%%     orddict:store(DcId, DcAddress, DCs).

%% add_dcs(OtherDCs, DCs) ->
%%     lists:foldl(fun add_dc/2, DCs, OtherDCs).
