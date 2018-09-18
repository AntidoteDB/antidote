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

%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc An Antidote module that manages the indexing data structures
%%%      of the database, including the primary and secondary indexes.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(index_manager).
-behaviour(gen_server).

-include("querying.hrl").

-define(PINDEX_ENTRY_DT, antidote_crdt_register_lww).
-record(state, {}).

%% API
-export([index_name/1,
         table_name/1,
         attributes/1,
         read_index/3,
         read_index_function/4,
         generate_index_key/2,
         generate_index_key/3,
         get_indexed_values/1,
         get_database_keys/2,
         lookup_index/2,
         create_index_hooks/1]).

%% gen_server API
-export([init/1,
    start_link/0,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

index_name(?INDEX(IndexName, _TableName, _Attributes)) -> IndexName.

table_name(?INDEX(_IndexName, TableName, _Attributes)) -> TableName.

attributes(?INDEX(_IndexName, _TableName, Attributes)) -> Attributes.

read_index(primary, TableName, TxId) ->
    %gen_server:call(?MODULE, {read_index, primary, TableName, TxId}, infinity);
    IName = pindex_key(TableName),
    ObjKeys = querying_utils:build_keys(IName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_keys(value, ObjKeys, TxId),
    {ok, IdxObj};
read_index(secondary, {TableName, IndexName}, TxId) ->
    %gen_server:call(?MODULE, {read_index, secondary, {TableName, IndexName}, TxId}, infinity).
    %% The secondary index is identified by the notation #2i_<IndexName>, where
    %% <IndexName> = <table_name>.<index_name>

    FullIName = sindex_key(TableName, IndexName),
    ObjKeys = querying_utils:build_keys(FullIName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_keys(value, ObjKeys, TxId),
    {ok, IdxObj}.

read_index_function(primary, TableName, {Function, Args}, TxId) ->
    %gen_server:call(?MODULE, {read_function, primary, TableName, {Function, Args}, TxId}, infinity);
    IName = pindex_key(TableName),
    ObjKeys = querying_utils:build_keys(IName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_function(ObjKeys, {Function, Args}, TxId),
    {ok, IdxObj};
read_index_function(secondary, {TableName, IndexName}, {Function, Args}, TxId) ->
    %gen_server:call(?MODULE, {read_function, secondary, {TableName, IndexName}, {Function, Args}, TxId}, infinity).
    FullIName = sindex_key(TableName, IndexName),
    ObjKeys = querying_utils:build_keys(FullIName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_function(ObjKeys, {Function, Args}, TxId),
    {ok, IdxObj}.

generate_index_key(primary, TableName) ->
    %gen_server:call(?MODULE, {generate_index_key, primary, TableName}, infinity).
    {ok, pindex_key(TableName)}.
generate_index_key(secondary, TableName, IndexName) ->
    %gen_server:call(?MODULE, {generate_index_key, secondary, TableName, IndexName}, infinity).
    {ok, sindex_key(TableName, IndexName)}.

get_indexed_values(IndexObj) ->
    gen_server:call(?MODULE, {get_indexed_values, IndexObj}, infinity).

get_database_keys(IndexedValues, IndexObj) ->
    gen_server:call(?MODULE, {get_database_keys, IndexedValues, IndexObj}, infinity).

lookup_index(ColumnName, Indexes) ->
    %gen_server:call(?MODULE, {lookup_index, ColumnName, Indexes}, infinity).
    {ok, lookup_index(ColumnName, Indexes, [])}.

create_index_hooks(Updates) ->
    %gen_server:call(?MODULE, {index_hooks, Updates}, infinity).
    {ok, index_triggers:create_index_hooks(Updates, ignore)}.

%% ====================================================================
%% gen_server functions
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    lager:info("Started Index Manager at node ~p", [node()]),
    {ok, #state{}}.

handle_call({read_index, primary, TName, TxId}, _From, State) ->
    Result = read_index(primary, TName, TxId),
    {reply, Result, State};

handle_call({read_index, secondary, {TName, IName}, TxId}, _From, State) ->
    Result = read_index(secondary, {TName, IName}, TxId),
    {reply, Result, State};

handle_call({read_function, primary, TName, {Function, Args}, TxId}, _From, State) ->
    Result = read_index_function(primary, TName, {Function, Args}, TxId),
    {reply, Result, State};

handle_call({read_function, secondary, {TName, IName}, {Function, Args}, TxId}, _From, State) ->
    Result = read_index_function(secondary, {TName, IName}, {Function, Args}, TxId),
    {reply, Result, State};

handle_call({generate_index_key, primary, TName}, _From, State) ->
    Result = generate_index_key(primary, TName),
    {reply, Result, State};

handle_call({generate_index_key, secondary, TName, IName}, _From, State) ->
    Result = generate_index_key(secondary, TName, IName),
    {reply, Result, State};

handle_call({lookup_index, CName, Indexes}, _From, State) ->
    Result = lookup_index(CName, Indexes),
    {reply, Result, State};

handle_call({get_indexed_values, IndexObj}, _From, State) ->
    IValues = indexed_values(IndexObj),
    {reply, {ok, IValues}, State};

handle_call({get_database_keys, IndexedValues, IndexObj}, _From, State) ->
    PKeys = database_keys(IndexedValues, IndexObj),
    {reply, {ok, PKeys}, State};

handle_call({index_hooks, Updates}, _From, State) ->
    %{reply, {ok, index_triggers:create_index_hooks(Updates, ignore)}, State}.
    {reply, create_index_hooks(Updates), State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
pindex_key(TName) ->
    IName = lists:concat([?PINDEX_PREFIX, TName]),
    querying_utils:to_atom(IName).
sindex_key(TName, IName) ->
    FullIndexName = lists:concat([?SINDEX_PREFIX, TName, '.', IName]),
    querying_utils:to_atom(FullIndexName).

lookup_index(ColumnName, [Index | Tail], Acc) ->
    Attributes = attributes(Index),
    case lists:member(ColumnName, Attributes) of
        true -> lookup_index(ColumnName, Tail, lists:append(Acc, [Index]));
        false -> lookup_index(ColumnName, Tail, Acc)
    end;
lookup_index(_ColumnName, [], Acc) ->
    Acc.

indexed_values([]) -> [];
indexed_values(IndexObj) ->
    orddict:fetch_keys(IndexObj).

database_keys(_IndexedValues, []) -> [];
database_keys(IndexedValues, IndexObj) when is_list(IndexedValues) ->
    database_keys(IndexedValues, IndexObj, []);
database_keys(IndexedValue, IndexObj) ->
    database_keys([IndexedValue], IndexObj).

database_keys([IdxValue | Tail], IndexObj, Acc) ->
    case orddict:find(IdxValue, IndexObj) of
        error -> database_keys(Tail, IndexObj, Acc);
        {ok, PKs} -> database_keys(Tail, IndexObj, lists:append(Acc, ordsets:to_list(PKs)))
    end;
database_keys([], _Index, Acc) ->
    Acc.
