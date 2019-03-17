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

%% This is for storing meta-data that rarely changes
%% All updates are broadcast to all nodes in the DC and stored locally
%% in memory and on disk at each node so that reading is just looking in an ets table
%% Updates are synchronous and should be one at a time for the whole DC
%% otherwise concurrent updates could overwrite each other

-module(stable_meta_data_server).
-behaviour(gen_server).
-include("antidote.hrl").

-define(TABLE_NAME, a_stable_meta_data_table).

%% API
-export([
     check_tables_ready/0,
     sync_meta_data/0,
     broadcast_meta_data_env/2,
     broadcast_meta_data/2,
     broadcast_meta_data_list/1,
     broadcast_meta_data_merge/4,
     generate_server_name/1,
     read_all_meta_data/0,
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

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({global, generate_server_name(node())}, ?MODULE, [], []).

-spec check_tables_ready() -> boolean().
check_tables_ready() ->
    case ets:info(?TABLE_NAME) of
        undefined ->
            false;
        _ ->
            true
    end.

%% Reads the value of a key stored in the table
-spec read_meta_data(term()) -> {ok, term()} | error.
read_meta_data(Key) ->
    case ets:lookup(?TABLE_NAME, Key) of
        [] ->
            error;
        [{Key, Value}]->
            {ok, Value}
    end.

-spec read_all_meta_data() -> [tuple()].
read_all_meta_data() ->
    ets:tab2list(?TABLE_NAME).

%% Tells each node in the DC to broadcast all its entries to all other DCs
-spec sync_meta_data() -> ok.
sync_meta_data() ->
    NodeList = dc_utilities:get_my_dc_nodes(),
    ok = lists:foreach(fun(Node) ->
                           ok = gen_server:call({global, generate_server_name(Node)}, {broadcast_meta_data})
                       end, NodeList).

%% Broadcasts a list of key, value pairs to all nodes in the DC
-spec broadcast_meta_data_list([{term(), term()}]) -> ok.
broadcast_meta_data_list(KeyValueList) ->
    NodeList = dc_utilities:get_my_dc_nodes(),
    ok = lists:foreach(fun(Node) ->
                           ok = gen_server:call({global, generate_server_name(Node)}, {update_meta_data, KeyValueList, false})
                       end, NodeList).

%% Broadcasts a key, value pair to all nodes in the DC
-spec broadcast_meta_data_env({env, atom()}, term()) -> ok.
broadcast_meta_data_env(Key, Value) ->
    broadcast_meta_data_internal(Key, Value, true).

%% Broadcasts a key, value pair to all nodes in the DC
-spec broadcast_meta_data(term(), term()) -> ok.
broadcast_meta_data(Key, Value) ->
    broadcast_meta_data_internal(Key, Value, false).

-spec broadcast_meta_data_internal(term(), term(), boolean()) -> ok.
broadcast_meta_data_internal(Key, Value, IsEnv) ->
    NodeList = dc_utilities:get_my_dc_nodes(),
    ok = lists:foreach(fun(Node) ->
                           ok = gen_server:call({global, generate_server_name(Node)}, {update_meta_data, [{Key, Value}], IsEnv})
                       end, NodeList).

%% Broadcasts a key, value pair to all nodes in the DC
%% Uses the provided MergeFunc which should take as input two values to merge the new value
%% with an exisiting value (if there is no existing value, it uses the InitFunc to create the value to merge with)
-spec broadcast_meta_data_merge(term(), term(), function(), function()) -> ok.
broadcast_meta_data_merge(Key, Value, MergeFunc, InitFunc) ->
    NodeList = dc_utilities:get_my_dc_nodes(),
    ok = lists:foreach(fun(Node) ->
                           ok = gen_server:call({global, generate_server_name(Node)}, {merge_meta_data, Key, Value, MergeFunc, InitFunc})
                       end, NodeList).


%% -------------------------------------------------------------------+

init([]) ->
    Path = filename:join(
         app_helper:get_env(riak_core, platform_data_dir), ?TABLE_NAME),

    {ok, DetsTable} = dets:open_file(Path, [{type, set}]),
    Table = ets:new(?TABLE_NAME, [set, named_table, protected, ?META_TABLE_STABLE_CONCURRENCY]),

    LoadFromDisk = case application:get_env(antidote, recover_meta_data_on_start) of
                       {ok, true} ->
                          true;
                       _ ->
                          false
                   end,
    case LoadFromDisk of
        true ->
            Table = dets:to_ets(DetsTable, Table),
            logger:info("Loaded meta data from disk");
        false ->
            ok = dets:delete_all_objects(DetsTable),
            Table
    end,
    ok = dets:sync(DetsTable),
    {ok, #state{table = Table, dets_table = DetsTable}}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_call({update_meta_data, KeyValueList, IsEnv}, _Sender, State = #state{table = Table, dets_table = DetsTable}) ->
    case IsEnv of
        true ->
            lists:foreach(fun({{env, Key}, Val}) ->
                      application:set_env(antidote, Key, Val)
                  end, KeyValueList);
        false -> ok
    end,
    true = ets:insert(Table, KeyValueList),
    ok = dets:insert(DetsTable, KeyValueList),
    ok = dets:sync(DetsTable),
    {reply, ok, State};

handle_call({merge_meta_data, Key, Value, MergeFunc, InitFunc}, _Sender, State = #state{table = Table, dets_table = DetsTable}) ->
    Prev = case ets:lookup(Table, Key) of
               [] ->
                  InitFunc();
               [{Key, PrevVal}]->
                  PrevVal
           end,
    true = ets:insert(Table, {Key, MergeFunc(Value, Prev)}),
    ok = dets:insert(DetsTable, {Key, MergeFunc(Value, Prev)}),
    ok = dets:sync(DetsTable),
    {reply, ok, State};

handle_call({sync_meta_data, NewList}, _Sender, State = #state{table = Table, dets_table = DetsTable}) ->
    true = ets:insert(Table, NewList),
    ok = dets:insert(DetsTable, NewList),
    ok = dets:sync(DetsTable),
    {reply, ok, State};

handle_call({broadcast_meta_data}, _Sender, State = #state{table = Table}) ->
    NodeList = dc_utilities:get_my_dc_nodes(),
    List = ets:tab2list(Table),
    ok = lists:foreach(fun(Node) ->
                           ok = gen_server:call({global, generate_server_name(Node)}, {sync_meta_data, List})
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
