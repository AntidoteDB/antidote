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
-module(indexing).

-include("querying.hrl").

-define(PINDEX_ENTRY_DT, antidote_crdt_register_lww).

%% API
-export([index_name/1,
         table_name/1,
         attributes/1,
         read_index/3,
         read_index_function/4,
         create_index/2,
         generate_pindex_key/1,
         generate_sindex_key/2,
         apply_updates/2,
         get_indexed_values/1,
         get_primary_keys/2,
         lookup_index/2]).

index_name(?INDEX(IndexName, _TableName, _Attributes)) -> IndexName.

table_name(?INDEX(_IndexName, TableName, _Attributes)) -> TableName.

attributes(?INDEX(_IndexName, _TableName, Attributes)) -> Attributes.

read_index(primary, TableName, TxId) ->
    IndexName = generate_pindex_key(TableName),
    ObjKeys = querying_utils:build_keys(IndexName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_keys(value, ObjKeys, TxId),
    IdxObj;
read_index(secondary, {TableName, IndexName}, TxId) ->
    %% The secondary index is identified by the notation #2i_<IndexName>, where
    %% <IndexName> = <table_name>.<index_name>

    FullIndexName = generate_sindex_key(TableName, IndexName),
    ObjKeys = querying_utils:build_keys(FullIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_keys(value, ObjKeys, TxId),
    IdxObj.

read_index_function(primary, TableName, {Function, Args}, TxId) ->
    IndexName = generate_pindex_key(TableName),
    ObjKeys = querying_utils:build_keys(IndexName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_function(ObjKeys, {Function, Args}, TxId),
    IdxObj;
read_index_function(secondary, {TableName, IndexName}, {Function, Args}, TxId) ->
    FullIndexName = generate_sindex_key(TableName, IndexName),
    ObjKeys = querying_utils:build_keys(FullIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_function(ObjKeys, {Function, Args}, TxId),
    IdxObj.

%% TODO
create_index(_IndexName, _TxId) -> {error, not_implemented}.

generate_pindex_key(TableName) ->
    IndexName = lists:concat([?PINDEX_PREFIX, TableName]),
    querying_utils:to_atom(IndexName).

generate_sindex_key(TableName, IndexName) ->
    FullIndexName = lists:concat([?SINDEX_PREFIX, TableName, '.', IndexName]),
    querying_utils:to_atom(FullIndexName).

apply_updates(Update, TxId) when ?is_index_upd(Update) ->
    apply_updates([Update], TxId);
apply_updates([Update | Tail], TxId) when ?is_index_upd(Update) ->
    DatabaseUpdates = index_triggers:build_index_updates([Update], TxId),
    ok = querying_utils:write_keys(DatabaseUpdates, TxId),
    apply_updates(Tail, TxId);
apply_updates([], _TxId) ->
    ok.

get_indexed_values([]) -> [];
get_indexed_values(IndexObj) ->
    orddict:fetch_keys(IndexObj).

get_primary_keys(_IndexedValues, []) -> [];
get_primary_keys(IndexedValues, IndexObj) when is_list(IndexedValues) ->
    get_primary_keys(IndexedValues, IndexObj, []);
get_primary_keys(IndexedValue, IndexObj) ->
    get_primary_keys([IndexedValue], IndexObj).

get_primary_keys([IdxValue | Tail], IndexObj, Acc) ->
    case orddict:find(IdxValue, IndexObj) of
        error -> get_primary_keys(Tail, IndexObj, Acc);
        {ok, PKs} -> get_primary_keys(Tail, IndexObj, lists:append(Acc, ordsets:to_list(PKs)))
    end;
get_primary_keys([], _Index, Acc) ->
    Acc.

lookup_index(ColumnName, Indexes) ->
    lookup_index(ColumnName, Indexes, []).
lookup_index(ColumnName, [Index | Tail], Acc) ->
    Attributes = attributes(Index),
    case lists:member(ColumnName, Attributes) of
        true -> lookup_index(ColumnName, Tail, lists:append(Acc, [Index]));
        false -> lookup_index(ColumnName, Tail, Acc)
    end;
lookup_index(_ColumnName, [], Acc) ->
    Acc.
