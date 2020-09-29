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

%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc An Antidote module to manage triggers that modify indexes.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(index_triggers).

-include("querying.hrl").

-define(MAP_UPD(ColName, ColType, Operation, Value), {{ColName, ColType}, {Operation, Value}}).
-define(INDEX_UPD(TableName, IndexName, EntryKey, EntryValue), {TableName, IndexName, EntryKey, EntryValue}).
-define(INDEX_OPERATION, update).
-define(PINDEX_ENTRY_DT, antidote_crdt_register_lww).

-define(FIELD_BOBJ_DT, antidote_crdt_register_lww).

%% API
-export([create_index_hooks/2,
    index_update_hook/2,
    index_update_hook/1,
    create_sindex_updates/2]).

create_index_hooks(Updates, _TxId) when is_list(Updates) ->
    lists:foldl(fun(ObjUpdate, UpdAcc) ->
        ?OBJECT_UPDATE(Key, Type, Bucket, _Op, _Param) = ObjUpdate,
        case update_type({Key, Type, Bucket}) of
            ?TABLE_UPD_TYPE ->
                ok = case antidote_hooks:has_hook(pre_commit, Bucket) of
                    true -> ok;
                    false -> antidote_hooks:register_pre_hook(Bucket, ?MODULE, index_update_hook)
                end,

                lists:append(UpdAcc, []);
            ?RECORD_UPD_TYPE ->
                ok = case antidote_hooks:has_hook(pre_commit, Bucket) of
                    true -> ok;
                    false -> antidote_hooks:register_pre_hook(Bucket, ?MODULE, index_update_hook)
                end,

                lists:append(UpdAcc, []);
            _ ->
                lists:append(UpdAcc, [])
        end
    end, [], Updates).

% Uncomment to use with the following 3 hooks
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

%% The 'Transaction' object passed here is a tuple of the form
%% {TxId, WriteSet} that includes a transaction id and a
%% transaction write set, respectively.
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

%% ====================================================================
%% Internal functions
%% ====================================================================

update_type({?TABLE_METADATA_KEY, ?TABLE_METADATA_DT, ?AQL_METADATA_BUCKET}) ->
    ?TABLE_UPD_TYPE;
update_type({_Key, ?TABLE_METADATA_DT, ?AQL_METADATA_BUCKET}) ->
    ?METADATA_UPD_TYPE;
update_type({_Key, ?TABLE_DT, _Bucket}) ->
    ?RECORD_UPD_TYPE;
update_type(_) ->
    ?OTHER_UPD_TYPE.

generate_index_updates(Key, Type, Bucket, Param, Transaction) ->
    {UpdateOp, Updates} = Param,
    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates),
    ObjBoundKey = {Key, Type, Bucket},

    case update_type({Key, Type, Bucket}) of
        ?TABLE_UPD_TYPE ->
            % This update is a table update
            [{{TableName, _}, _}] = Updates,

            Table = table_utils:table_metadata(TableName, Transaction),
            SIdxUpdates = fill_index(ObjUpdate, Table, Transaction),
            Upds = create_sindex_updates(SIdxUpdates, Transaction),

            %% Remove table metadata entry from cache -- mark as 'dirty'
            ok = metadata_caching:remove_key(TableName),

            Upds;
        ?RECORD_UPD_TYPE ->
            % This update is a record update
            TName = filter_table_name(Bucket),
            Table = table_utils:table_metadata(TName, Transaction),
            case Table of
                [] -> [];
                _Else ->
                    % A table exists
                    TableName = table_utils:name(Table),

                    {ok, PIdxName} = index_manager:generate_index_key(primary, TableName),
                    [PIdxKey] = querying_utils:build_keys(PIdxName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),

                    PIdxUpdate = create_pindex_update(ObjBoundKey, Updates, Table, PIdxKey, Transaction),
                    PrepareIdxUpdates = build_index_updates(Updates, ObjBoundKey, Table, []),
                    SIdxUpdates = create_sindex_updates(PrepareIdxUpdates, Transaction),

                    case PIdxUpdate of
                        none ->
                            SIdxUpdates;
                        _ ->
                            lists:append([PIdxUpdate], SIdxUpdates)
                    end
            end;
        _ -> []
    end.

fill_index(ObjUpdate, Table, Transaction) ->
    case retrieve_index(ObjUpdate, Table) of
        [] ->
            % No index was created in the update
            [];
        Indexes when is_list(Indexes) ->
            lists:foldl(fun(Index, Acc) ->
                % A new index was created in the update
                ?INDEX(IndexName, TableName, [IndexedColumn]) = Index, %% TODO support more than one column
                {ok, PIndexObject} = index_manager:read_index(primary, TableName, Transaction),

                IdxUpds = lists:map(fun({RawKey, BoundKey}) ->
                    case record_utils:record_data(BoundKey, Transaction) of
                        [] -> [];
                        [Record] ->
                            ?ATTRIBUTE(_ColName, Type, Value) = record_utils:get_column(IndexedColumn, Record),
                            AtomKey = querying_utils:to_atom(RawKey),
                            BObjOp = crdt_utils:to_insert_op(?CRDT_VARCHAR, BoundKey),
                            IndexValOp = crdt_utils:to_insert_op(Type, Value),
                            IndexValOps =
                                case is_list(IndexValOp) of
                                    true -> lists:map(fun(IdxOp) -> {index_val, Type, IdxOp} end, IndexValOp);
                                    false -> [{index_val, Type, IndexValOp}]
                                end,
                            Op = lists:append([{bound_obj, ?FIELD_BOBJ_DT, BObjOp}], IndexValOps),
                            ?INDEX_UPD(TableName, IndexName, {Value, Type}, {AtomKey, Op})
                    end
                end, PIndexObject),
                lists:append(Acc, lists:flatten(IdxUpds))
            end, [], Indexes)
    end.

build_index_updates([Update | Updates], ObjBoundKey, Table, Acc) ->
    {Key, _Type, _Bucket} = ObjBoundKey,
    Indexes = table_utils:indexes(Table),
    TName = table_utils:name(Table),
    {{Col, CRDT}, {_CRDTOper, Val} = IndexValOp} = Update,
    NewAcc =
        case index_manager:lookup_index(Col, Indexes) of
            {ok, []} ->
                Acc;
            {ok, Idxs} ->
                AuxUpdates = lists:map(fun(Idx) ->
                    BObjOp = crdt_utils:to_insert_op(?CRDT_VARCHAR, ObjBoundKey),
                    EntryOp = [{bound_obj, ?FIELD_BOBJ_DT, BObjOp}, {index_val, CRDT, IndexValOp}],

                    ?INDEX_UPD(TName, index_manager:index_name(Idx), {Val, CRDT}, {Key, EntryOp})
                end, Idxs),
                lists:append(Acc, AuxUpdates)
        end,
    build_index_updates(Updates, ObjBoundKey, Table, NewAcc);
build_index_updates([], _ObjBoundKey, _Table, Acc) ->
    Acc.

%% Given a list of index updates, build the database updates given
%% that it may be necessary to delete old entries and
%% insert the new ones.
create_sindex_updates([], _TxId) -> [];
create_sindex_updates(Updates, _TxId) when is_list(Updates) ->
    lists:foldl(fun(Update, AccList) ->
        ?INDEX_UPD(TableName, IndexName, {_Value, _Type}, {PkValue, Op}) = Update,

        {ok, DBIndexName} = index_manager:generate_index_key(secondary, TableName, IndexName),
        [IndexKey] = querying_utils:build_keys(DBIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),

        IdxUpdate =
            case is_list(Op) of
                true ->
                    lists:map(fun(Op2) ->
                        {{K, T, B}, UpdateOp, Upd} = crdt_utils:create_crdt_update(IndexKey, ?INDEX_OPERATION, {PkValue, Op2}),
                        {{K, B}, T, {UpdateOp, Upd}}
                    end, Op);
                false ->
                    {{K, T, B}, UpdateOp, Upd} = crdt_utils:create_crdt_update(IndexKey, ?INDEX_OPERATION, {PkValue, Op}),
                    [{{K, B}, T, {UpdateOp, Upd}}]
            end,

        lists:append([AccList, IdxUpdate])
    end, [], Updates);
create_sindex_updates(Update, TxId) when ?is_index_upd(Update) ->
    create_sindex_updates([Update], TxId).

retrieve_index(ObjUpdate, Table) ->
    ?OBJECT_UPDATE(_Key, _Type, _Bucket, _UpdOp, [Assign]) = ObjUpdate,
    ?MAP_UPD(_TableName, _CRDT, _CRDTOp, NewTableMeta) = Assign,

    CurrentIdx =
        case Table of
            [] -> [];
            _ -> table_utils:indexes(Table)
        end,
    NIdx = table_utils:indexes(NewTableMeta),
    NewIndexes = lists:filter(fun(?INDEX(IndexName, _, _)) ->
        not lists:keymember(IndexName, 1, CurrentIdx)
    end, NIdx),
    NewIndexes.

create_pindex_update(ObjBoundKey, Updates, Table, PIndexKey, Transaction) ->
    case proplists:get_value({?STATE_COL, ?STATE_COL_DT}, Updates) of
        {_, i} -> %% We only update the indexes on row insertion
            TableCols = table_utils:columns(Table),
            [PKName] = table_utils:primary_key_name(Table),
            {PKName, PKType, _Constraint} = maps:get(PKName, TableCols),
            PKCRDT = crdt_utils:type_to_crdt(PKType, ignore),

            ConvPKey = case proplists:get_value({PKName, PKCRDT}, Updates) of
                           {_Op, Value} ->
                               Value;
                           undefined ->
                               [Record] = record_utils:record_data(ObjBoundKey, Transaction),
                               record_utils:lookup_value({PKName, PKCRDT}, Record)
                       end,

            PIdxOp = crdt_utils:to_insert_op(?CRDT_VARCHAR, ObjBoundKey),

            {{IdxKey, IdxType, IdxBucket}, UpdateType, IndexOp} =
                crdt_utils:create_crdt_update(PIndexKey, ?INDEX_OPERATION, {ConvPKey, PIdxOp}),
            {{IdxKey, IdxBucket}, IdxType, {UpdateType, IndexOp}};
        _ ->
            none
    end.

filter_table_name(Bucket) when is_atom(Bucket) ->
    BucketStr = atom_to_list(Bucket),
    BucketName = filter_table_name(BucketStr),
    list_to_atom(BucketName);
filter_table_name(Bucket) when is_binary(Bucket) ->
    BucketStr = binary_to_list(Bucket),
    BucketName = filter_table_name(BucketStr),
    list_to_binary(BucketName);
filter_table_name(Bucket) when is_integer(Bucket) ->
    BucketStr = integer_to_list(Bucket),
    BucketName = filter_table_name(BucketStr),
    list_to_integer(BucketName);
filter_table_name(Bucket) when is_list(Bucket) ->
    AtIdx = string:chr(Bucket, $@),
    case AtIdx of
        0 -> Bucket;
        _ -> string:sub_string(Bucket, AtIdx + 1)
    end.
