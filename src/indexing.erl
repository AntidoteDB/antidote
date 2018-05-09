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

%-define(INDEX_ENTRY(IndexedValue, DTType, PKs), {{IndexedValue, DTType}, PKs}).
-define(INDEX_UPDATE(TableName, IndexName, EntryKey, EntryValue), {TableName, IndexName, EntryKey, EntryValue}).
-define(MAP_OPERATION, update).

%% API
-export([index_name/1,
         table_name/1,
         attributes/1,
         create_index_hooks/1,
         check_object_update/1,
         read_index/3,
         read_index_function/4,
         create_index/2,
         build_index_updates/2,
         apply_updates/2,
         get_indexed_values/1,
         get_primary_keys/2,
         lookup_index/2]).

index_name(?INDEX(IndexName, _TableName, _Attributes)) -> IndexName.

table_name(?INDEX(_IndexName, TableName, _Attributes)) -> TableName.

attributes(?INDEX(_IndexName, _TableName, Attributes)) -> Attributes.

create_index_hooks(Updates) when is_list(Updates) ->
    lists:foreach(fun(ObjUpdate) ->
        ?OBJECT_UPDATE(Key, Type, Bucket, _Op, _Param) = ObjUpdate,
        case update_type({Key, Type, Bucket}) of
            ?TABLE_UPD_TYPE ->
                %[{{TableName, _CRDT}, {_CRDTOp, _Meta}}] = Param,
                %io:format("Is a table update, where table = ~p~n", [TableName]),
                antidote_hooks:register_post_hook(Bucket, ?MODULE, check_object_update);
            ?RECORD_UPD_TYPE ->
                antidote_hooks:register_post_hook(Bucket, ?MODULE, check_object_update);
            _ -> ok
        end
    end, Updates),
    ok.

%% Check if the object updates trigger index updates.
%% If so, generate updates for the respective indexes.
check_object_update({{Key, Bucket}, Type, Param}) ->

    %% TODO use {ok, _CT} = antidote:read_objects(ignore, [], [{Key, Type, Bucket}, ...])
    %% TODO use {ok, _CT} = antidote:update_objects(ignore, [], [{{Key, Type, Bucket}, update, ...}])

    lager:info(">> check_object_updates:", []),
    %lager:info("ObjUpdate: ~p", [ObjUpdate]),
    lager:info("{Key, Bucket} = ~p", [{Key, Bucket}]),
    lager:info("Type = ~p", [Type]),
    lager:info("Param = ~p", [Param]),
    {UpdateOp, Updates} = Param,
    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates), % TODO we assume the Op is always update
    SendUpdates = case update_type({Key, Type, Bucket}) of
        ?TABLE_UPD_TYPE ->
            lager:info("Is a table update...~n", []),
            SIdxUpdates = fill_index(ObjUpdate),
            io:format("SIdxUpdates: ~p~n", [SIdxUpdates]),
            Upds = build_index_updates(SIdxUpdates, ignore),
            io:format("Upds: ~p~n", [Upds]),
            Upds;
        ?RECORD_UPD_TYPE ->
            lager:info("Is a record update...~n", []),
            Table = table_utils:table_metadata(Bucket, ignore),
            TableName = table_utils:table(Table),
            case Table of
                undefined -> [];
                _Else ->
                    lager:info("A table exists! Metadata: ~p~n", [Table]),
                    [PIdxKey] = querying_utils:build_keys(generate_pindex_key(TableName), ?PINDEX_DT, ?AQL_METADATA_BUCKET),
                    PIdxUpdate = {PIdxKey, add, Key},
                    Indexes = table_utils:indexes(Table),
                    SIdxUpdates = lists:foldl(fun(Operation, IdxUpdates2) ->
                        {{Col, CRDT}, {_CRDTOper, Val} = Op} = Operation,
                        case lookup_index(Col, Indexes) of
                            [] -> IdxUpdates2;
                            Idxs ->
                                AuxUpdates = lists:map(fun(Idx) ->
                                    ?INDEX_UPDATE(TableName, index_name(Idx), {Val, CRDT}, {Key, Op})
                                end, Idxs),
                                lists:append(IdxUpdates2, AuxUpdates)
                        end
                    end, [], Updates),
                    ToDBUpdate = lists:append([PIdxUpdate], build_index_updates(SIdxUpdates, ignore)),
                    io:format("ToDBUpdate: ~p~n", [ToDBUpdate]),
                    ToDBUpdate
            end;
        _ -> []
    end,
    io:format(">>>>>>>>>>>>>>>>>>>>>~n"),
    lager:info("SendUpdates:~n~p~n", [SendUpdates]),
    io:format(">>>>>>>>>>>>>>>>>>>>>~n"),
    case SendUpdates of
        [] -> ok;
        _ -> {ok, _CT} = querying_utils:write_keys(SendUpdates)
    end,
    {ok, ObjUpdate}.

read_index(primary, TableName, TxId) ->
    %% TODO add the capability to read the primary index from memory/cache?
    %io:format(">> read_index(primary):~n", []),
    IndexName = generate_pindex_key(TableName),
    ObjKeys = querying_utils:build_keys(IndexName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_keys(ObjKeys, TxId),
    %io:format("ObjKeys: ~p~n", [ObjKeys]),
    %io:format("IdxObj: ~p~n", [IdxObj]),
    IdxObj;
read_index(secondary, {TableName, IndexName}, TxId) ->
    %% TODO add the capability to read the primary index from memory/cache?
    %% the secondary index is identified by the notation "#2i_IndexName", where
    %% IndexName = "table_name.index_name"
    %io:format(">> read_index(secondary):~n", []),
    FullIndexName = generate_sindex_key(TableName, IndexName),
    ObjKeys = querying_utils:build_keys(FullIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_keys(ObjKeys, TxId),
    %io:format("IdxObj: ~p~n", [IdxObj]),
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

%% Given a list of index updates on the form {TableName, IndexName, {EntryKey, EntryValue}},
%% build the database updates given that it may be necessary to delete old entries and
%% insert the new ones.
build_index_updates([], _TxId) -> [];
build_index_updates(Updates, TxId) when is_list(Updates) ->
    io:format(">> build_index_updates:~n", []),
    %Map = to_map(Updates),
    io:format("Updates: ~p~n", [Updates]),

    lists:foldl(fun(Update, AccList) ->
        ?INDEX_UPDATE(TableName, IndexName, {_Value, Type}, {PkValue, Op}) = Update,
        DBIndexName = generate_sindex_key(TableName, IndexName),
        %EntryValue = sets:to_list(Value),

        [IndexKey] = querying_utils:build_keys(DBIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
        [IndexObj] = querying_utils:read_keys(IndexKey, TxId),

        io:format("IndexKey: ~p~n", [IndexKey]),
        io:format("IndexObj: ~p~n", [IndexObj]),
        %io:format("EntryValue: ~p~n", [EntryValue]),
        io:format("{PkValue, Op}: ~p~n", [{PkValue, Op}]),
        io:format("Inserting new entry...~n", []),

        %IdxUpdate = lists:map(fun({CRDT, Pk, Op}) ->
        %    querying_utils:create_crdt_update(IndexKey, ?MAP_OPERATION, {CRDT, Pk, Op})
        %end, EntryValue),
        IdxUpdate = case is_list(Op) of
                        true ->
                            lists:map(fun(Op2) ->
                                querying_utils:create_crdt_update(IndexKey, ?MAP_OPERATION, {Type, PkValue, Op2})
                            end, Op);
                        false -> [querying_utils:create_crdt_update(IndexKey, ?MAP_OPERATION, {Type, PkValue, Op})]
                    end,

        %UpdEntry = {EntryKey, EntryValue},
        %io:format("UpdEntry: ~p~n", [UpdEntry]),
        %PrepareUpd = prepare_update(IndexKey, add_all, UpdEntry),
        %IdxUpdate = [querying_utils:create_crdt_update(IndexKey, ?MAP_OPERATION, UpdEntry)],
        io:format("FinalUpdate: ~p~n", [IdxUpdate]),
        lists:append([AccList, IdxUpdate])
    end, [], Updates);
build_index_updates(Update, TxId) when ?is_index_upd(Update) ->
    build_index_updates([Update], TxId).

apply_updates(Update, TxId) when ?is_index_upd(Update) ->
    apply_updates([Update], TxId);
apply_updates([Update | Tail], TxId) when ?is_index_upd(Update) ->
    DatabaseUpdates = build_index_updates([Update], TxId),
    ?INDEX_UPDATE(TableName, IndexName, _, _) = Update,
    IndexKey = generate_sindex_key(TableName, IndexName),
    ok = querying_utils:write_keys(IndexKey, DatabaseUpdates, TxId),
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

%% ====================================================================
%% Internal functions
%% ====================================================================

generate_pindex_key(TableName) ->
    IndexName = lists:concat([?PINDEX_PREFIX, TableName]),
    querying_utils:to_atom(IndexName).

generate_sindex_key(TableName, IndexName) ->
    FullIndexName = lists:concat([?SINDEX_PREFIX, TableName, '.', IndexName]),
    %io:format("FullIndexName: ~p~n", [FullIndexName]),
    querying_utils:to_atom(FullIndexName).

%% Each update is on the form: [{ {key, crdt_type, bucket}, operation, values}]
%% For the OrSet, 'operation' can be one of these: add, add_all, remove, remove_all
%% For the OrSet, 'values' are the values to be inserted in/removed from the set
%% TODO deprecated
%create_database_update(ObjectKey, add, Value) ->
%    {ObjectKey, add, Value};
%create_database_update(ObjectKey, add_all, Values) when is_list(Values) ->
%    {ObjectKey, add_all, Values};
%create_database_update(ObjectKey, remove, Value) ->
%    {ObjectKey, remove, Value};
%create_database_update(ObjectKey, remove_all, Values) when is_list(Values) ->
%    {ObjectKey, remove_all, Values}.

%% Entry update generator for the GMap CRDT
%% TODO deprecated
%prepare_update(IndexKey, add, {IndexedColVal, Value}) ->
%    prepare_update(IndexKey, add_all, {IndexedColVal, [Value]});
%prepare_update(_IndexKey, add_all, {IndexedColVal, Values}) when is_list(Values) ->
%    {{IndexedColVal, ?SINDEX_ENTRY_DT}, {add_all, Values}};
%prepare_update(IndexKey, remove, {IndexedColVal, Value}) ->
%    prepare_update(IndexKey, remove_all, {IndexedColVal, [Value]});
%prepare_update(_IndexKey, remove_all, {IndexedColVal, Values}) when is_list(Values) ->
%    {{IndexedColVal, ?SINDEX_ENTRY_DT}, {remove_all, Values}}.

update_type({?TABLE_METADATA_KEY, ?TABLE_METADATA_DT, ?AQL_METADATA_BUCKET}) -> ?TABLE_UPD_TYPE;
update_type({_Key, ?TABLE_METADATA_DT, ?AQL_METADATA_BUCKET}) -> ?METADATA_UPD_TYPE;
update_type({_Key, ?TABLE_DT, _Bucket}) -> ?RECORD_UPD_TYPE;
update_type(_) -> ?OTHER_UPD_TYPE.

%%to_map(Updates) when is_list(Updates) ->
%%    lists:foldl(fun(Update, Acc) ->
%%        %io:format("Current Acc: ~p~n", [Acc]),
%%        ?INDEX_UPDATE(TableName, IndexName, {_Val, CRDT}, {PK, Op}) = Update,
%%        DBIndexName = generate_sindex_key(TableName, IndexName),
%%        try maps:get(DBIndexName, Acc) of
%%            CurrSet ->
%%                %io:format("Does exist: ~p~n", [MapKey]),
%%                maps:put(DBIndexName, sets:add_element({CRDT, PK, Op}, CurrSet), Acc)
%%        catch
%%            error:Error ->
%%                case Error of
%%                    {badkey, _} ->
%%                        %io:format("Does not exist: ~p~n", [MapKey]),
%%                        maps:put(DBIndexName, sets:add_element({CRDT, PK, Op}, sets:new()), Acc);
%%                    _Other -> Acc
%%                end;
%%            Other -> io:format("An error occurred: ~p~n", [Other])
%%        end
%%    end, maps:new(), Updates).

retrieve_index(ObjUpdate) ->
    io:format(">> retrieve_new_index", []),
    io:format("ObjUpdate: ~p~n", [ObjUpdate]),
    ?OBJECT_UPDATE(_Key, _Type, _Bucket, _UpdOp, [Assign]) = ObjUpdate,
    {{_TableName, _CRDT}, {_CRDTOp, NewTableMeta}} = Assign,
    %NTableName = table_utils:table(NewTableMeta),
    NIdx = table_utils:indexes(NewTableMeta),
    {NewTableMeta, NIdx}.
%%    ReadTableMeta = table_utils:table_metadata(NTableName, ignore),
%%    io:format("ReadTableMeta: ~p", [ReadTableMeta]),
%%    case ReadTableMeta of
%%        [] -> io:format("Table metadata is empty~n", []), {undefined, []};
%%        _Else ->
%%            RIdx = table_utils:indexes(ReadTableMeta),
%%            lager:info("NIdx: ~p~n", [NIdx]),
%%            lager:info("RIdx: ~p~n", [RIdx]),
%%            NewIndex = lists:subtract(NIdx, RIdx),
%%            lager:info("NewIndex: ~p~n", [NewIndex]),
%%            {ReadTableMeta, NewIndex}
%%    end.

is_element(IndexedVal, Pk, IndexObj) ->
    case orddict:find(IndexedVal, IndexObj) of
        {ok, Pks} -> ordsets:is_element(Pk, Pks);
        error -> false
    end.

fill_index(ObjUpdate) ->
    case retrieve_index(ObjUpdate) of
        {_, []} ->
            io:format("No index was created...~n", []),
            [];
        {Table, Indexes} when is_list(Indexes) ->
            lists:foldl(fun(Index, Acc) ->
                io:format("A new index was created...~n", []),
                ?INDEX(IndexName, TableName, [IndexedColumn]) = Index, %% TODO support more than one column
                [PrimaryKey] = table_utils:primary_key_name(Table),
                io:format("PrimaryKey: ~p~n", [PrimaryKey]),
                PIndexObject = read_index(primary, TableName, ignore),
                SIndexObject = read_index(secondary, {TableName, IndexName}, ignore),
                io:format("PIndexObject: ~p~n", [PIndexObject]),
                Records = table_utils:record_data(PIndexObject, TableName, ignore),
                %Filtered = query_optimizer:get_partial_object(Records, lists:append(PrimaryKey, Cols)),
                io:format("Records: ~p~n", [Records]),
                IdxUpds = lists:map(fun(Record) ->
                    PkValue = querying_utils:to_atom(table_utils:lookup_value(PrimaryKey, Record)),
                    ?ATTRIBUTE(_ColName, Type, Value) = table_utils:get_column(IndexedColumn, Record),
                    case is_element(Value, PkValue, SIndexObject) of
                        true -> [];
                        false ->
                            Op = table_utils:crdt_to_op(Type, Value), %% generate an op according to Type
                            %io:format("Op: ~p~n", [Op]),
                            ?INDEX_UPDATE(TableName, IndexName, {Value, Type}, {PkValue, Op})
                    end
                end, Records),
                lists:flatten(Acc, IdxUpds)
            end,[], Indexes)
    end.
