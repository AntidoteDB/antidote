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
         index_update_hook/2,
%%         empty_hook/1,
%%         transaction_hook/1,
%%         rwtransaction_hook/1,
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

create_index_hooks(Updates, _TxId) when is_list(Updates) ->
    lists:foldl(fun(ObjUpdate, UpdAcc) ->
        ?OBJECT_UPDATE(Key, Type, Bucket, _Op, _Param) = ObjUpdate,
        case update_type({Key, Type, Bucket}) of
            ?TABLE_UPD_TYPE ->
                case antidote_hooks:has_hook(pre_commit, Bucket) of
                    true -> ok;
                    false -> antidote_hooks:register_pre_hook(Bucket, ?MODULE, index_update_hook)
                end,

                lists:append(UpdAcc, []);
            ?RECORD_UPD_TYPE ->
                case antidote_hooks:has_hook(pre_commit, Bucket) of
                    true -> ok;
                    false -> antidote_hooks:register_pre_hook(Bucket, ?MODULE, index_update_hook)
                end,

                lists:append(UpdAcc, []);
            _ ->
                lists:append(UpdAcc, [])
        end
    end, [], Updates).

% Uncomment to use with following 3 hooks
%%fill_pindex(Key, Bucket) ->
%%    TableName = querying_utils:to_atom(Bucket),
%%    PIdxName = generate_pindex_key(TableName),
%%    [PIdxKey] = querying_utils:build_keys(PIdxName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
%%    PIdxUpdate = {PIdxKey, add, Key},
%%    [PIdxUpdate].
%%
%%empty_hook({{Key, Bucket}, Type, Param}) ->
%%    {ok, {{Key, Bucket}, Type, Param}}.
%%
%%transaction_hook({{Key, Bucket}, Type, Param}) ->
%%    {UpdateOp, Updates} = Param,
%%    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates),
%%
%%    {ok, TxId} = cure:start_transaction(ignore, [], false),
%%    {ok, _} = cure:commit_transaction(TxId),
%%
%%    {ok, ObjUpdate}.
%%
%%rwtransaction_hook({{Key, Bucket}, Type, Param}) ->
%%    {UpdateOp, Updates} = Param,
%%    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates),
%%
%%    {ok, TxId} = cure:start_transaction(ignore, [], false),
%%
%%    %% write a key
%%    [ObjKey] = querying_utils:build_keys(key1, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
%%    Upd = querying_utils:create_crdt_update(ObjKey, ?MAP_OPERATION, {antidote_crdt_register_lww, pk1, {assign, val1}}),
%%    {ok, _} = querying_utils:write_keys(Upd, TxId),
%%
%%    %% read a key
%%    [ObjKey] = querying_utils:build_keys(key2, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
%%    [_Obj] = querying_utils:read_keys(value, ObjKey, TxId),
%%
%%    {ok, _} = cure:commit_transaction(TxId),
%%
%%    {ok, ObjUpdate}.

%% The 'Transaction' object passed here is a tuple on the form
%% {TxId, ReadSet} that represents a transaction id and a transaction
%% read set, respectively.
index_update_hook(Update, Transaction) when is_tuple(Update) ->
    {{Key, Bucket}, Type, Param} = Update,
    case generate_index_updates(Key, Type, Bucket, Param, Transaction) of
        [] ->
            {ok, Update};
        Updates ->
            SendUpds = lists:append([Update], Updates),
            {ok, SendUpds}
    end.

index_update_hook(Update) when is_tuple(Update) ->
    {ok, TxId} = querying_utils:start_transaction(),
    {ok, Updates} = index_update_hook(Update, TxId),
    {ok, _} = querying_utils:commit_transaction(TxId),

    {ok, Updates}.

generate_index_updates(Key, Type, Bucket, Param, Transaction) ->
    {UpdateOp, Updates} = Param,
    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates),

    case update_type({Key, Type, Bucket}) of
        ?TABLE_UPD_TYPE ->
            % Is a table update
            %lager:info("Is a table update: ~p", [ObjUpdate]),

            SIdxUpdates = fill_index(ObjUpdate, Transaction),
            Upds = build_index_updates(SIdxUpdates, Transaction),

            [{{TableName, _}, _}] = Updates,

            %% Remove table metadata entry from cache -- mark as 'dirty'
            ok = metadata_caching:remove_key(TableName),

            Upds;
        ?RECORD_UPD_TYPE ->
            % Is a record update
            %lager:info("Is a record update: ~p", [ObjUpdate]),

            Table = table_utils:table_metadata(Bucket, Transaction),
            case Table of
                undefined -> [];
                _Else ->
                    % A table exists
                    TableName = table_utils:table(Table),

                    PIdxName = generate_pindex_key(TableName),
                    [PIdxKey] = querying_utils:build_keys(PIdxName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),

                    {K, T, B} = PIdxKey,

                    %PIdxUpdate = {{K, B}, T, {add, Key}},
                    PIdxUpdate = {{K, B}, T, {add, {Key, Type, Bucket}}},
                    Indexes = table_utils:indexes(Table),
                    SIdxUpdates = lists:foldl(fun(Operation, IdxUpdates2) ->
                        {{Col, CRDT}, {_CRDTOper, Val} = Op} = Operation,
                        case lookup_index(Col, Indexes) of
                            [] -> IdxUpdates2;
                            Idxs ->
                                AuxUpdates = lists:map(fun(Idx) ->
                                    ?INDEX_UPDATE(TableName, index_name(Idx), {Val, CRDT}, {{Key, Type, Bucket}, Op})
                                end, Idxs),
                                lists:append(IdxUpdates2, AuxUpdates)
                        end
                    end, [], Updates),

                    %lager:info("SIdxUpdates: ~p", [SIdxUpdates]),

                    lists:append([PIdxUpdate], build_index_updates(SIdxUpdates, Transaction))
            end;
        _ -> []
    end.

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

%% Given a list of index updates on the form {TableName, IndexName, {EntryKey, EntryValue}},
%% build the database updates given that it may be necessary to delete old entries and
%% insert the new ones.
build_index_updates([], _TxId) -> [];
build_index_updates(Updates, _TxId) when is_list(Updates) ->
    %lager:info("List of updates: ~p", [Updates]),

    lists:foldl(fun(Update, AccList) ->
        ?INDEX_UPDATE(TableName, IndexName, {_Value, Type}, {PkValue, Op}) = Update,

        DBIndexName = generate_sindex_key(TableName, IndexName),
        [IndexKey] = querying_utils:build_keys(DBIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),

        IdxUpdate = case is_list(Op) of
                        true ->
                            lists:map(fun(Op2) ->
                                {{K, T, B}, UpdateOp, Upd} = querying_utils:create_crdt_update(IndexKey, ?MAP_OPERATION, {Type, PkValue, Op2}),
                                {{K, B}, T, {UpdateOp, Upd}}
                            end, Op);
                        false ->
                            {{K, T, B}, UpdateOp, Upd} = querying_utils:create_crdt_update(IndexKey, ?MAP_OPERATION, {Type, PkValue, Op}),
                            [{{K, B}, T, {UpdateOp, Upd}}]
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
    querying_utils:to_atom(FullIndexName).

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
        {ok, Pks} ->
            querying_utils:first_occurrence(fun({K, _T, _B}) -> K == Pk end, Pks) =/= undefined;
            %ordsets:is_element(Pk, Pks);
        error -> false
    end.

fill_index(ObjUpdate, Transaction) ->
    case retrieve_index(ObjUpdate) of
        {_, []} ->
            % No index was created
            %lager:info("No index was created"),
            [];
        {Table, Indexes} when is_list(Indexes) ->
            lists:foldl(fun(Index, Acc) ->
                % A new index was created
                %lager:info("A new index was created"),

                ?INDEX(IndexName, TableName, [IndexedColumn]) = Index, %% TODO support more than one column
                [PrimaryKey] = table_utils:primary_key_name(Table),
                PIndexObject = read_index(primary, TableName, Transaction),
                SIndexObject = read_index(secondary, {TableName, IndexName}, Transaction),

                %Records = table_utils:record_data(PIndexObject, TableName, Transaction), TODO old

                IdxUpds = lists:map(fun(ObjKey) ->
                    case record_utils:record_data(ObjKey, Transaction) of
                        [] -> [];
                        [Record] ->
                            PkValue = querying_utils:to_atom(record_utils:lookup_value(PrimaryKey, Record)),
                            ?ATTRIBUTE(_ColName, Type, Value) = record_utils:get_column(IndexedColumn, Record),
                            case is_element(Value, PkValue, SIndexObject) of
                                true -> [];
                                false ->
                                    Op = crdt_utils:to_insert_op(Type, Value), %% generate an op according to Type
                                    %?INDEX_UPDATE(TableName, IndexName, {Value, Type}, {PkValue, Op}) TODO old
                                    ?INDEX_UPDATE(TableName, IndexName, {Value, Type}, {ObjKey, Op})
                            end
                    end
                end, PIndexObject),
                lists:append(Acc, lists:flatten(IdxUpds))
            end, [], Indexes)
    end.
