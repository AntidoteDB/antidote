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
-define(META_TABLE_NAME, a_meta_data_table).
-define(META_TABLE_STABLE_NAME, a_meta_data_table_stable).

-include("antidote.hrl").

-export([start_link/0,
	 put_meta_dict/2,
	 put_meta_data/3,
	 get_stable_time/0,
         remove_partition/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-record(state, {
	  table,
	  table2,
	  time_dict,
	  last_time
	 }).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec put_meta_dict(partition_id(), dict()) -> ok.
put_meta_dict(Partition, Dict) ->
    true = ets:insert(?META_TABLE_NAME, {Partition, Dict}),
    ok.

-spec put_meta_data(partition_id(), term(), term()) -> ok.
put_meta_data(Partition, Key, Value) ->
    Dict = case ets:lookup(?META_TABLE_NAME, Partition) of
	       [] ->
		   dict:new();
	       [Other] ->
		   Other
	   end,
    NewDict = dict:store(Key, Value, Dict),
    put_meta_dict(Partition, NewDict).

-spec remove_partition(partition_id()) -> ok.
remove_partition(Partition) ->
    true = ets:delete(?META_TABLE_NAME, Partition),
    ok.

%% Add info about a new DC. This info could be
%% used by other modules to communicate to other DC
-spec get_stable_time() -> vectorclock().
get_stable_time() ->
    case ets:lookup(?META_TABLE_STABLE_NAME, stable_time) of
	[] ->
	    dict:new();
	[Other] ->
	    Other
    end.

%% Add a list of DCs to this DC
%% -spec add_list_dcs([dc_address()]) -> ok.
%% add_list_dcs(DCs) ->
%%     gen_server:call(?MODULE, {add_list_dcs, DCs}, infinity).


%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    Table = ets:new(?META_TABLE_NAME, [set, named_table, public, ?META_TABLE_CONCURRENCY]),
    Table2 = ets:new(?META_TABLE_STABLE_NAME, [set, named_table, ?META_TABLE_STABLE_CONCURRENCY]),
    TimeDict = dict:new(),
    {ok, #state{table=Table, table2=Table2, time_dict=TimeDict, last_time = 0}}.

handle_call({update_time, NodeId, Time}, _From, State = #state{time_dict=TimeDict, last_time=LastTime}) ->
    NewTimeDict = dict:store(NodeId, Time, TimeDict),
    Min = get_min_time(NewTimeDict),
    % This needs to be a dict per DC!
    NewTime = case Min > LastTime of
		  true ->
		      true = ets:insert(?META_TABLE_STABLE_NAME, {stable_time, Min}),
		      Min;
		  false ->
		      LastTime
	      end,
    {ok, State#state{time_dict=NewTimeDict, last_time=NewTime}}.

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

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



get_min_time(Dict) ->
    dict:fold(fun(_Key, Value, Prev) ->
		      case Prev > Value of
			  true ->
			      Value;
			  false ->
			      Prev
		      end
	      end, 0, Dict).



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
