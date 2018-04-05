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

%% API
-export([read_index/3,
         create_index/2,
         update_index/2,
         get_indexed_values/1,
         get_primary_keys/2]).

read_index(primary, TableName, TxId) ->
    %% TODO add the capability to read the primary index from memory/cache
    io:format(">> read_index(primary):~n", []),
    IndexName = lists:concat([?PINDEX_PREFIX, TableName]),
    ObjKeys = querying_commons:build_keys(querying_commons:to_atom(IndexName), ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_commons:read_keys(ObjKeys, TxId),
    io:format("IdxObj: ~p~n", [IdxObj]),
    IdxObj;
read_index(secondary, IndexName, TxId) ->
    %% TODO add the capability to read the primary index from memory/cache
    %% the secondary index is identified by the notation "#2i_IndexName", where
    %% IndexName = "table_name.index_name"
    io:format(">> read_index(secondary):~n", []),
    FullIndexName = lists:concat([?SINDEX_PREFIX, IndexName]),
    ObjKeys = querying_commons:build_keys(querying_commons:to_atom(FullIndexName), ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_commons:read_keys(ObjKeys, TxId),
    io:format("IdxObj: ~p~n", [IdxObj]),
    IdxObj.

create_index(_IndexName, _TxId) -> ok.

update_index(Update, TxId) when is_tuple(Update) ->
    update_index([Update], TxId);
update_index([Update | Tail], TxId) when is_tuple(Update) ->
    {TableName, IndexName, NewEntry} = Update,
    {EntryKey, EntryValue} = NewEntry,

    IndexKey = querying_commons:build_keys(generate_index_key(TableName, IndexName), ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [IndexObj] = querying_commons:read_keys(IndexKey, TxId),
    Updates = case get_primary_keys(EntryKey, IndexObj) of
                  [] ->
                      UpdEntry = {EntryKey, [EntryValue]},
                      create_database_update(IndexKey, add_all, UpdEntry);
                  PKs ->
                      % first, remove the current entry; then, add the new entry
                      % %NewPkSet = lists:append(PKs, [EntryValue]),
                      CurrEntry = {EntryKey, PKs},
                      RemoveUpdate = create_database_update(IndexKey, remove, CurrEntry),
                      UpdEntry = {EntryKey, lists:append(PKs, [EntryValue])},
                      AddUpdate = create_database_update(IndexKey, add, UpdEntry),
                      lists:append([RemoveUpdate], [AddUpdate])
    end,
    ok = querying_commons:write_keys(IndexKey, Updates, TxId),
    update_index(Tail, TxId);
update_index([], _TxId) ->
    ok.

get_indexed_values([]) -> [];
get_indexed_values(Index) when is_list(Index) ->
    lists:map(fun(Entry) ->
        {IndexedValues, _PrimaryKeys} = Entry,
        IndexedValues
    end, Index).

get_primary_keys(_IndexedValues, []) -> [];
get_primary_keys(IndexedValues, Index) when is_list(IndexedValues) ->
    get_primary_keys(IndexedValues, Index, []);
get_primary_keys(IndexedValue, Index) ->
    get_primary_keys([IndexedValue], Index).

get_primary_keys([IdxValue | Tail], Index, Acc) ->
    case lists:keyfind(IdxValue, 1, Index) of
        false -> get_primary_keys(Tail, Index, Acc);
        {_, PKs} when is_list(PKs) -> get_primary_keys(Tail, Index, lists:append(Acc, PKs));
        {_, PK} -> get_primary_keys(Tail, Index, lists:append(Acc, [PK]))
    end;
get_primary_keys([], _Index, Acc) ->
    Acc.

%% ====================================================================
%% Internal functions
%% ====================================================================

generate_index_key(TableName, IndexName) ->
    FullIndexName = lists:concat([?SINDEX_PREFIX, TableName, '.', IndexName]),
    querying_commons:to_atom(FullIndexName).

%% Each update is on the form: [{ {key, crdt_type, bucket}, operation, values}]
%% For the OrSet, 'operation' can be one of these: add, add_all, removem remove_all
%% For the OrSet, 'values' are the values to be inserted in/removed from the set
create_database_update(ObjectKey, add, Value) ->
    {ObjectKey, add, Value};
create_database_update(ObjectKey, add_all, Values) when is_list(Values) ->
    {ObjectKey, add_all, Values}.
