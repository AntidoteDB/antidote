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

%% This is for storing meta-data that rarely changes
%% It all updates are broadcast to all nodes in the DC and stored locally
%% at each node so that reading is just looking in an ets table
%% Updates are synchronous and should be one at a time for the whole DC
%% otherwise concurrent updates could overwrite eachother

-module(stable_meta_data_server).
-behaviour(gen_server).
-include("antidote.hrl").

-define(TABLE_NAME, a_stable_meta_data_table).

%% API
-export([
	 sync_meta_data/0,
	 broadcast_meta_data/2,
	 broadcast_meta_data_list/1,
	 broadcast_meta_data_merge/4,
	 generate_server_name/1,
	 read_meta_data/1]).
	 
%% Internal API
-export([
	 init/1,
	 start_link/0,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-record(state, {table, dets_table}).

%%% --------------------------------------------------------------+

-spec start_link() -> {ok,pid()} | ignore | {error,term()}.
start_link() ->
    gen_server:start_link({global,generate_server_name(node())}, ?MODULE, [], []).

-spec read_meta_data(term()) -> {ok, term()} | error.
read_meta_data(Key) ->
    case ets:lookup(?TABLE_NAME, Key) of
	[] ->
	    error;
	[{Key, Value}]->
	    {ok, Value}
    end.

-spec sync_meta_data() -> ok.
sync_meta_data() ->
    NodeList = dc_utilities:get_my_dc_nodes(),
    ok = lists:foreach(fun(Node) ->
			       ok = gen_server:call({global,generate_server_name(Node)}, {broadcast_meta_data})
		       end, NodeList).

-spec broadcast_meta_data_list([{term(),term()}]) -> ok.
broadcast_meta_data_list(KeyValueList) ->
    NodeList = dc_utilities:get_my_dc_nodes(),
    ok = lists:foreach(fun(Node) ->
			       ok = gen_server:call({global,generate_server_name(Node)}, {update_meta_data, KeyValueList})
		       end, NodeList).    

-spec broadcast_meta_data(term(), term()) -> ok.
broadcast_meta_data(Key, Value) ->
    NodeList = dc_utilities:get_my_dc_nodes(),
    ok = lists:foreach(fun(Node) ->
			       ok = gen_server:call({global,generate_server_name(Node)}, {update_meta_data, [{Key, Value}]})
		       end, NodeList).    

-spec broadcast_meta_data_merge(term(), term(), function(), function()) -> ok.
broadcast_meta_data_merge(Key, Value, MergeFunc, InitFunc) ->
    NodeList = dc_utilities:get_my_dc_nodes(),
    ok = lists:foreach(fun(Node) ->
			       ok = gen_server:call({global,generate_server_name(Node)}, {merge_meta_data, Key, Value, MergeFunc, InitFunc})
		       end, NodeList).    


%% -------------------------------------------------------------------+

init([]) ->
    {ok, DetsTable} = dets:open_file(?TABLE_NAME,[{type,set}]),
    Table = ets:new(?TABLE_NAME, [set, named_table, protected, ?META_TABLE_STABLE_CONCURRENCY]),
    
    LoadFromDisk = case application:get_env(antidote,recover_meta_data_on_start) of
		       {ok, true} ->
			   true;
		       _ ->
			   false
		   end,
    case LoadFromDisk of
	true ->
	    Table = dets:to_ets(DetsTable,Table),
	    lager:info("Loaded meta data from disk");
	false ->
	    ok = dets:delete_all_objects(DetsTable),
	    Table
    end,
    {ok, #state{table = Table, dets_table = DetsTable}}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_call({update_meta_data, KeyValueList}, _Sender, State = #state{table = Table, dets_table = DetsTable}) ->
    true = ets:insert(Table, KeyValueList),
    ok = dets:insert(DetsTable, KeyValueList),
    {reply, ok, State};

handle_call({merge_meta_data,Key,Value,MergeFunc,InitFunc}, _Sender, State = #state{table = Table, dets_table = DetsTable}) ->
    Prev = case ets:lookup(?TABLE_NAME, Key) of
	       [] ->
		   InitFunc();
	       [{Key, PrevVal}]->
		   PrevVal
	   end,
    true = ets:insert(Table, {Key,MergeFunc(Value,Prev)}),
    ok = dets:insert(DetsTable, {Key,Value}),
    {reply, ok, State};

handle_call({sync_meta_data, NewList}, _Sender, State = #state{table = Table, dets_table = DetsTable}) ->
    true = ets:insert(Table, NewList),
    ok = dets:insert(DetsTable, NewList),
    {reply, ok, State};

handle_call({broadcast_meta_data}, _Sender, State = #state{table = Table}) ->
    NodeList = dc_utilities:get_my_dc_nodes(),
    List = ets:tab2list(Table),
    ok = lists:foreach(fun(Node) ->
			       ok = gen_server:call({global,generate_server_name(Node)}, {sync_meta_data, List})
		       end, NodeList),
    {reply, ok, State};

handle_call(_Info, _From, State) ->
    {reply, error, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

generate_server_name(Node) ->
    list_to_atom("stable_meta_data" ++ atom_to_list(Node)).
