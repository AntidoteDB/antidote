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
-export([index_name/1,
         table_name/1,
         attributes/1,
         check_object_updates/2,
         read_index/3,
         create_index/2,
         build_index_updates/2,
         apply_updates/2,
         get_indexed_values/1,
         get_primary_keys/2,
         lookup_index/2]).

index_name(?INDEX(IndexName, _TableName, _Attributes)) -> IndexName.

table_name(?INDEX(_IndexName, TableName, _Attributes)) -> TableName.

attributes(?INDEX(_IndexName, _TableName, Attributes)) -> Attributes.

%% Check if the object updates trigger index updates.
%% If so, generate updates for the respective indexes.
check_object_updates(Updates, TxId) when is_list(Updates) ->
    io:format(">> check_object_updates:~n", []),
    io:format("Updates: ~p~n", [Updates]),
    lists:foldl(fun(ObjUpdate, IdxUpdates) ->
        ?OBJECT_UPDATE(Key, Type, Bucket, _Op, Param) = ObjUpdate, % TODO we assume the Op is always update
        case update_type({Key, Type, Bucket}) of
            ?TABLE_UPD_TYPE ->
                io:format("Is a table update...~n", []),
                SIdxUpdates = fill_index(ObjUpdate, TxId),
                lists:append(IdxUpdates, build_index_updates(SIdxUpdates, TxId));
            ?RECORD_UPD_TYPE ->
                io:format("Is a record update...~n", []),
                Table = table_utils:table_metadata(Bucket, TxId),
                TableName = table_utils:table(Table),
                case Table of
                    undefined -> IdxUpdates;
                    _Else ->
                        io:format("A table exists! Metadata: ~p~n", [Table]),
                        [PIdxKey] = querying_commons:build_keys(generate_pindex_key(TableName), ?PINDEX_DT, ?AQL_METADATA_BUCKET),
                        PIdxUpdate = create_database_update(PIdxKey, add, Key),
                        Indexes = table_utils:indexes(Table),
                        SIdxUpdates = lists:foldl(fun(Operation, IdxUpdates2) ->
                            {{Col, _CRDT}, {_Oper, Val}} = Operation,
                            case lookup_index(Col, Indexes) of
                                [] -> IdxUpdates2;
                                Idxs ->
                                    AuxUpdates = lists:map(fun(Idx) ->
                                        NewEntry = {Val, Key},
                                        {TableName, index_name(Idx), NewEntry}
                                    end, Idxs),
                                    lists:append(IdxUpdates2, AuxUpdates)
                            end
                        end, [], Param),
                        ToDBUpdate = lists:append([PIdxUpdate], build_index_updates(SIdxUpdates, TxId)),
                        io:format("ToDBUpdate: ~p~n", [ToDBUpdate]),
                        lists:append(IdxUpdates, ToDBUpdate)
                end;
            _ -> IdxUpdates
        end
    end, [], Updates).

read_index(primary, TableName, TxId) ->
    %% TODO add the capability to read the primary index from memory/cache
    %io:format(">> read_index(primary):~n", []),
    IndexName = generate_pindex_key(TableName),
    ObjKeys = querying_commons:build_keys(IndexName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_commons:read_keys(ObjKeys, TxId),
    %io:format("IdxObj: ~p~n", [IdxObj]),
    IdxObj;
read_index(secondary, {TableName, IndexName}, TxId) ->
    %% TODO add the capability to read the primary index from memory/cache
    %% the secondary index is identified by the notation "#2i_IndexName", where
    %% IndexName = "table_name.index_name"
    %io:format(">> read_index(secondary):~n", []),
    FullIndexName = generate_sindex_key(TableName, IndexName),
    ObjKeys = querying_commons:build_keys(FullIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_commons:read_keys(ObjKeys, TxId),
    %io:format("IdxObj: ~p~n", [IdxObj]),
    IdxObj.

create_index(_IndexName, _TxId) -> ok.

%% Given a list of index updates on the form {TableName, IndexName, {EntryKey, EntryValue}},
%% build the database updates given that it may be necessary to delete old entries and
%% insert the new ones.
build_index_updates([], _TxId) -> [];
build_index_updates(Updates, TxId) when is_list(Updates) ->
    io:format(">> build_index_updates:~n", []),
    Map = to_list_of_maps(Updates),
    io:format("Map: ~p~n", [Map]),

    maps:fold(fun(Key, Value, AccList) ->
        %?INDEX_UPDATE(TableName, IndexName, EntryKey, EntryValue) = Update,
        {DBIndexName, EntryKey} = Key,
        EntryValue = sets:to_list(Value),

        [IndexKey] = querying_commons:build_keys(DBIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
        [IndexObj] = querying_commons:read_keys(IndexKey, TxId),

        io:format("IndexKey: ~p~n", [IndexKey]),
        io:format("IndexObj: ~p~n", [IndexObj]),
        io:format("EntryKey: ~p~n", [EntryKey]),
        io:format("EntryValue: ~p~n", [EntryValue]),
        case get_primary_keys(EntryKey, IndexObj) of
            [] ->
                io:format("Inserting new entry...~n", []),
                UpdEntry = {EntryKey, EntryValue},
                %generate_sindex_update(UpdEntry, IndexName, TableName, add);
                lists:append(AccList, [create_database_update(IndexKey, add, UpdEntry)]);
            PKs ->
                % first, remove the current entry; then, add the new entry
                % %NewPkSet = lists:append(PKs, [EntryValue]),
                CurrEntry = {EntryKey, PKs},
                io:format("Updating the current entry: ~p~n", [CurrEntry]),
                RemoveUpdate = create_database_update(IndexKey, remove, CurrEntry),
                UpdEntry = {EntryKey, lists:append(PKs, EntryValue)},
                AddUpdate = create_database_update(IndexKey, add, UpdEntry),

                lists:append([AccList, [RemoveUpdate], [AddUpdate]])
        end
    end, [], Map);
build_index_updates(Update, TxId) when ?is_index_upd(Update) ->
    build_index_updates([Update], TxId).

apply_updates(Update, TxId) when ?is_index_upd(Update) ->
    apply_updates([Update], TxId);
apply_updates([Update | Tail], TxId) when ?is_index_upd(Update) ->
    DatabaseUpdates = build_index_updates([Update], TxId),
    ?INDEX_UPDATE(TableName, IndexName, _, _) = Update,
    IndexKey = generate_sindex_key(TableName, IndexName),
    ok = querying_commons:write_keys(IndexKey, DatabaseUpdates, TxId),
    apply_updates(Tail, TxId);
apply_updates([], _TxId) ->
    ok.

get_indexed_values([]) -> [];
get_indexed_values(Index) when is_list(Index) ->
    lists:map(fun(Entry) ->
        {IndexedValues, _PrimaryKeys} = Entry,
        IndexedValues
    end, Index).

get_primary_keys(_IndexedValues, []) -> [];
get_primary_keys([IndexedValues], Index) ->
    get_primary_keys([IndexedValues], Index, []);
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

%% ====================================================================
%% Internal functions
%% ====================================================================

generate_pindex_key(TableName) ->
    IndexName = lists:concat([?PINDEX_PREFIX, TableName]),
    querying_commons:to_atom(IndexName).

generate_sindex_key(TableName, IndexName) ->
    FullIndexName = lists:concat([?SINDEX_PREFIX, TableName, '.', IndexName]),
    %io:format("FullIndexName: ~p~n", [FullIndexName]),
    querying_commons:to_atom(FullIndexName).

%% Each update is on the form: [{ {key, crdt_type, bucket}, operation, values}]
%% For the OrSet, 'operation' can be one of these: add, add_all, remove, remove_all
%% For the OrSet, 'values' are the values to be inserted in/removed from the set
create_database_update(ObjectKey, add, Value) ->
    {ObjectKey, add, Value};
create_database_update(ObjectKey, add_all, Values) when is_list(Values) ->
    {ObjectKey, add_all, Values};
create_database_update(ObjectKey, remove, Value) ->
    {ObjectKey, remove, Value};
create_database_update(ObjectKey, remove_all, Values) when is_list(Values) ->
    {ObjectKey, remove_all, Values}.

update_type({?TABLE_METADATA_KEY, ?TABLE_METADATA_DT, ?AQL_METADATA_BUCKET}) -> ?TABLE_UPD_TYPE;
update_type({_Key, ?TABLE_METADATA_DT, ?AQL_METADATA_BUCKET}) -> ?METADATA_UPD_TYPE;
update_type({_Key, ?TABLE_DT, _Bucket}) -> ?RECORD_UPD_TYPE;
update_type(_) -> ?OTHER_UPD_TYPE.

%generate_pindex_update(Value, TableName, Operation) ->
%    IdxKey = generate_pindex_key(TableName),
%    Key = {IdxKey, ?PINDEX_DT, ?AQL_METADATA_BUCKET},
%    create_database_update(Key, Operation, Value).

%generate_sindex_update(Value, IndexName, TableName, Operation) ->
%    IdxKey = generate_sindex_key(TableName, IndexName),
%    Key = {IdxKey, ?SINDEX_DT, ?AQL_METADATA_BUCKET},
%    create_database_update(Key, Operation, Value).
%generate_sindex_update(Value, Index) when ?is_index(Index) ->
%    IndexName = index_name(Index),
%    TableName = table_name(Index),
%    generate_sindex_update(Value, IndexName, TableName).

to_list_of_maps(Updates) when is_list(Updates) ->
    lists:foldl(fun(Update, Acc) ->
        %io:format("Current Acc: ~p~n", [Acc]),
        ?INDEX_UPDATE(TableName, IndexName, EntryKey, EntryValue) = Update,
        DBIndexName = generate_sindex_key(TableName, IndexName),
        MapKey = {DBIndexName, EntryKey},
        try maps:get(MapKey, Acc) of
            CurrSet ->
                %io:format("Does exist: ~p~n", [MapKey]),
                maps:put(MapKey, sets:add_element(EntryValue, CurrSet), Acc)
        catch
            error:Error ->
                case Error of
                    {badkey, _} ->
                        %io:format("Does not exist: ~p~n", [MapKey]),
                        maps:put(MapKey, sets:add_element(EntryValue, sets:new()), Acc);
                    _Other -> Acc
                end;
            Other -> io:format("An error occurred: ~p~n", [Other])
        end
    end, maps:new(), Updates).

retrieve_new_index(ObjUpdate, TxId) ->
    io:format(">> retrieve_new_index~n", []),
    io:format("ObjUpdate: ~p~n", [ObjUpdate]),
    ?OBJECT_UPDATE(_Key, _Type, _Bucket, _UpdOp, [Assign]) = ObjUpdate,
    {{_TableName, _CRDT}, {_CRDTOp, NewTableMeta}} = Assign,
    NTableName = table_utils:table(NewTableMeta),
    NIdx = table_utils:indexes(NewTableMeta),
    ReadTableMeta = table_utils:table_metadata(NTableName, TxId),
    io:format("ReadTableMeta: ~p~n", [ReadTableMeta]),
    case ReadTableMeta of
        [] -> io:format("Table metadata is empty~n", []), {undefined, []};
        _Else ->
            RIdx = table_utils:indexes(ReadTableMeta),
            io:format("NIdx: ~p~n", [NIdx]),
            io:format("RIdx: ~p~n", [RIdx]),
            NewIndex = lists:subtract(NIdx, RIdx),
            io:format("NewIndex: ~p~n", [NewIndex]),
            {ReadTableMeta, NewIndex}
    end.

fill_index(ObjUpdate, TxId) ->
    case retrieve_new_index(ObjUpdate, TxId) of
        {_, []} ->
            io:format("No index was created...~n", []),
            [];
        {Table, [NewIndex]} ->
            io:format("A new index was created...~n", []),
            ?INDEX(IndexName, TableName, [IndexedColumn]) = NewIndex, %% TODO support for more than one column
            [PrimaryKey] = maps:get(?PK_COLUMN, table_utils:columns(Table)),
            io:format("PrimaryKey: ~p~n", [PrimaryKey]),
            PIndexObject = read_index(primary, TableName, TxId),
            io:format("PIndexObject: ~p~n", [PIndexObject]),
            Records = table_utils:record_data(PIndexObject, TableName, TxId),
            %Filtered = query_optimizer:get_partial_object(Records, lists:append(PrimaryKey, Cols)),
            io:format("Records: ~p~n", [Records]),
            lists:map(fun(Record) ->
                PkValue = querying_commons:to_atom(table_utils:lookup_column(PrimaryKey, Record)),
                IndexedValue = table_utils:lookup_column(IndexedColumn, Record),
                NewEntry = {IndexedValue, PkValue},
                io:format("NewEntry: ~p~n", [NewEntry]),
                {TableName, IndexName, NewEntry}
            end, Records)
    end.
