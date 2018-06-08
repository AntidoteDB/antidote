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

-define(INDEX_UPDATE(TableName, IndexName, EntryKey, EntryValue), {TableName, IndexName, EntryKey, EntryValue}).
-define(MAP_OPERATION, update).

%% API
-export([index_name/1,
         table_name/1,
         attributes/1,
         create_index_hooks/2,
         index_update_hook/1,
         empty_hook/1,
         transaction_hook/1,
         rwtransaction_hook/1,
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

% TODO clean the code
create_index_hooks(Updates, _TxId) when is_list(Updates) ->
    lists:foldl(fun(ObjUpdate, UpdAcc) ->
        ?OBJECT_UPDATE(Key, Type, Bucket, _Op, _Param) = ObjUpdate,
        case update_type({Key, Type, Bucket}) of
            ?TABLE_UPD_TYPE ->
                %[{{TableName, _CRDT}, {_CRDTOp, _Meta}}] = Param,
                %io:format("Is a table update, where table = ~p~n", [TableName]),
                %antidote_hooks:register_post_hook(Bucket, ?MODULE, index_update_hook),
                %antidote_hooks:register_pre_hook(Bucket, ?MODULE, index_update_hook),

                %antidote_hooks:register_post_hook(Bucket, ?MODULE, empty_hook),
                antidote_hooks:register_pre_hook(Bucket, ?MODULE, empty_hook),
                %antidote_hooks:register_post_hook(Bucket, ?MODULE, transaction_hook),
                %antidote_hooks:register_post_hook(Bucket, ?MODULE, rwtransaction_hook),
                lists:append(UpdAcc, []);
                %lists:append(UpdAcc, generate_index_updates(Key, Type, Bucket, {Op, Param}, TxId));
            ?RECORD_UPD_TYPE ->
                %antidote_hooks:register_post_hook(Bucket, ?MODULE, index_update_hook),
                %antidote_hooks:register_pre_hook(Bucket, ?MODULE, index_update_hook),

                %antidote_hooks:register_post_hook(Bucket, ?MODULE, empty_hook),
                antidote_hooks:register_pre_hook(Bucket, ?MODULE, empty_hook),
                %antidote_hooks:register_post_hook(Bucket, ?MODULE, transaction_hook),
                %antidote_hooks:register_post_hook(Bucket, ?MODULE, rwtransaction_hook),

                %lists:append(UpdAcc, []);
                %lists:append(UpdAcc, generate_index_updates(Key, Type, Bucket, {Op, Param}, TxId));
                lists:append(UpdAcc, fill_pindex(Key, Bucket));
            _ -> lists:append(UpdAcc, [])
        end
    end, [], Updates).

% Uncomment to use with following 3 hooks
fill_pindex(Key, Bucket) ->
    TableName = querying_utils:to_atom(Bucket),
    PIdxName = generate_pindex_key(TableName),
    [PIdxKey] = querying_utils:build_keys(PIdxName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    PIdxUpdate = {PIdxKey, add, Key},
    [PIdxUpdate].

empty_hook({{Key, Bucket}, Type, Param}) ->
    {ok, {{Key, Bucket}, Type, Param}}.

transaction_hook({{Key, Bucket}, Type, Param}) ->
    {UpdateOp, Updates} = Param,
    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates),

    {ok, TxId} = cure:start_transaction(ignore, [], false),
    {ok, _} = cure:commit_transaction(TxId),

    {ok, ObjUpdate}.

rwtransaction_hook({{Key, Bucket}, Type, Param}) ->
    {UpdateOp, Updates} = Param,
    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates),

    {ok, TxId} = cure:start_transaction(ignore, [], false),

    %% write a key
    [ObjKey] = querying_utils:build_keys(key1, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    Upd = querying_utils:create_crdt_update(ObjKey, ?MAP_OPERATION, {antidote_crdt_register_lww, pk1, {assign, val1}}),
    {ok, _} = querying_utils:write_keys(Upd, TxId),

    %% read a key
    [ObjKey] = querying_utils:build_keys(key2, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [_Obj] = querying_utils:read_keys(value, ObjKey, TxId),

    {ok, _} = cure:commit_transaction(TxId),

    {ok, ObjUpdate}.


index_update_hook(Update) when is_tuple(Update) ->
    {ok, TxId} = querying_utils:start_transaction(),
    index_update(Update, TxId),
    {ok, _} = querying_utils:commit_transaction(TxId).

generate_index_updates(Key, Type, Bucket, Param, TxId) ->
    {UpdateOp, Updates} = Param,
    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates),

    case update_type({Key, Type, Bucket}) of
        ?TABLE_UPD_TYPE ->
            % Is a table update
            SIdxUpdates = fill_index(ObjUpdate, TxId),
            Upds = build_index_updates(SIdxUpdates, TxId),

            [{{TableName, _}, _}] = Updates,

            %% Remove table metadata entry from cache -- mark as 'dirty'
            ok = metadata_caching:remove_key(TableName),

            Upds;
        ?RECORD_UPD_TYPE ->
            % Is a record update
            Table = table_utils:table_metadata(Bucket, TxId),
            TableName = table_utils:table(Table),
            case Table of
                undefined -> [];
                _Else ->
                    % A table exists
                    PIdxName = generate_pindex_key(TableName),
                    [PIdxKey] = querying_utils:build_keys(PIdxName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),

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

                    lists:append([PIdxUpdate], build_index_updates(SIdxUpdates, TxId))
            end;
        _ -> []
    end.

%% Check if the object updates trigger index updates.
%% If so, generate updates for the respective indexes.
index_update({{Key, Bucket}, Type, Param}, TxId) ->
    {UpdateOp, Updates} = Param,
    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates), % TODO we assume the Op is always update
    SendUpdates = generate_index_updates(Key, Type, Bucket, Param, TxId),

    case SendUpdates of
        [] -> ok;
        _ -> {ok, _CT} = querying_utils:write_keys(SendUpdates)
    end,
    {ok, ObjUpdate}.

read_index(primary, TableName, TxId) ->
    IndexName = generate_pindex_key(TableName),
    ObjKeys = querying_utils:build_keys(IndexName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_keys(value, ObjKeys, TxId),
    IdxObj;
read_index(secondary, {TableName, IndexName}, TxId) ->
    %% The secondary index is identified by the notation "#2i_IndexName", where
    %% IndexName = "table_name.index_name"

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

%% Given a list of index updates on the form {TableName, IndexName, {EntryKey, EntryValue}},
%% build the database updates given that it may be necessary to delete old entries and
%% insert the new ones.
build_index_updates([], _TxId) -> [];
build_index_updates(Updates, _TxId) when is_list(Updates) ->
    lists:foldl(fun(Update, AccList) ->
        ?INDEX_UPDATE(TableName, IndexName, {_Value, Type}, {PkValue, Op}) = Update,
        DBIndexName = generate_sindex_key(TableName, IndexName),

        [IndexKey] = querying_utils:build_keys(DBIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),

        IdxUpdate = case is_list(Op) of
                        true ->
                            lists:map(fun(Op2) ->
                                querying_utils:create_crdt_update(IndexKey, ?MAP_OPERATION, {Type, PkValue, Op2})
                            end, Op);
                        false -> [querying_utils:create_crdt_update(IndexKey, ?MAP_OPERATION, {Type, PkValue, Op})]
                    end,

        lists:append([AccList, IdxUpdate])
    end, [], Updates);
build_index_updates(Update, TxId) when ?is_index_upd(Update) ->
    build_index_updates([Update], TxId).

apply_updates(Update, TxId) when ?is_index_upd(Update) ->
    apply_updates([Update], TxId);
apply_updates([Update | Tail], TxId) when ?is_index_upd(Update) ->
    DatabaseUpdates = build_index_updates([Update], TxId),
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

retrieve_index(ObjUpdate) ->
    ?OBJECT_UPDATE(_Key, _Type, _Bucket, _UpdOp, [Assign]) = ObjUpdate,
    {{_TableName, _CRDT}, {_CRDTOp, NewTableMeta}} = Assign,
    NIdx = table_utils:indexes(NewTableMeta),
    {NewTableMeta, NIdx}.

is_element(IndexedVal, Pk, IndexObj) ->
    case orddict:find(IndexedVal, IndexObj) of
        {ok, Pks} -> ordsets:is_element(Pk, Pks);
        error -> false
    end.

fill_index(ObjUpdate, TxId) ->
    case retrieve_index(ObjUpdate) of
        {_, []} ->
            % No index was created
            [];
        {Table, Indexes} when is_list(Indexes) ->
            lists:foldl(fun(Index, Acc) ->
                % A new index was created

                ?INDEX(IndexName, TableName, [IndexedColumn]) = Index, %% TODO support more than one column
                [PrimaryKey] = table_utils:primary_key_name(Table),
                PIndexObject = read_index(primary, TableName, TxId),
                SIndexObject = read_index(secondary, {TableName, IndexName}, TxId),

                Records = table_utils:record_data(PIndexObject, TableName, TxId),

                IdxUpds = lists:map(fun(Record) ->
                    PkValue = querying_utils:to_atom(table_utils:lookup_value(PrimaryKey, Record)),
                    ?ATTRIBUTE(_ColName, Type, Value) = table_utils:get_column(IndexedColumn, Record),
                    case is_element(Value, PkValue, SIndexObject) of
                        true -> [];
                        false ->
                            Op = table_utils:crdt_to_op(Type, Value), %% generate an op according to Type
                            ?INDEX_UPDATE(TableName, IndexName, {Value, Type}, {PkValue, Op})
                    end
                end, Records),
                lists:flatten(Acc, IdxUpds)
            end,[], Indexes)
    end.
