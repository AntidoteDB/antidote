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
%%% @doc An Antidote module that includes some utility functions for
%%%      table management.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(table_utils).
-include("querying.hrl").

%% API
-export([table/1,
         policy/1,
         columns/1,
         foreign_keys/1,
         indexes/1,
         column_names/1,
         primary_key_name/1,
         all_column_names/1,
         tables_metadata/1,
         table_metadata/2,
         is_primary_key/2,
         is_column/2,
         is_foreign_key/2,
         shadow_column_state/4,
         record_data/3,
         get_column/2,
         lookup_value/2,
         crdt_to_op/2]).

table(?TABLE(TName, _Policy, _Cols, _SCols, _Idx)) -> TName.

policy(?TABLE(_TName, Policy, _Cols, _SCols, _Idx)) -> Policy.

columns(?TABLE(_TName, _Policy, Cols, _SCols, _Idx)) -> Cols.

foreign_keys(?TABLE(_TName, _Policy, _Cols, SCols, _Idx)) -> SCols.

indexes(?TABLE(_TName, _Policy, _Cols, _SCols, Idx)) -> Idx.

column_names(Table) ->
    Columns = columns(Table),
    maps:get(?COLUMNS, Columns).

primary_key_name(Table) ->
    Columns = columns(Table),
    maps:get(?PK_COLUMN, Columns).

all_column_names(Table) ->
    TCols = column_names(Table),
    SCols = lists:map(fun(?FK(FKName, _FKType, _RefTable, _RefCol)) -> FKName end, foreign_keys(Table)),
    lists:append([['#st'], TCols, SCols]).

%% Tables metadata is always read from the database;
%% Only individual table metadata is stored on cache.
tables_metadata(TxId) ->
    ObjKey = querying_utils:build_keys(?TABLE_METADATA_KEY, ?TABLE_DT, ?AQL_METADATA_BUCKET),
    [Meta] = querying_utils:read_keys(value, ObjKey, TxId),
    Meta.

table_metadata(TableName, TxId) ->
    %io:format(">> table_metadata:~n", []),
    %lager:info(">> table_metadata:~n", []),
    case metadata_caching:get_key(TableName) of
        {error, _} ->
            %io:format("Not in cache; reading from database...~n", []),
            Metadata = tables_metadata(TxId),
            TableNameAtom = querying_utils:to_atom(TableName),
            MetadataKey = {TableNameAtom, ?TABLE_NAME_DT},
            case proplists:get_value(MetadataKey, Metadata) of
                undefined -> [];
                TableMeta ->
                    %lager:info("Not in cache; reading from database: ~p~n", [TableMeta]),
                    ok = metadata_caching:insert_key(TableName, TableMeta),
                    TableMeta
            end;
        TableMetaObj ->
            %io:format("In cache; retrieving object~n", []),
            %lager:info("In cache; retrieving object: ~p~n", [TableMetaObj]),
            TableMetaObj %% table metadata is a 'value' type object
    end.

is_primary_key(ColumnName, ?TABLE(_TName, _Policy, Cols, _FKeys, _Idx)) when is_map(Cols) ->
    ColList = maps:get(?PK_COLUMN, Cols),
    lists:member(ColumnName, ColList).

is_column(ColumnName, ?TABLE(_TName, _Policy, Cols, _FKeys, _Idx)) when is_map(Cols) ->
    ColList = maps:get(?COLUMNS, Cols),
    lists:member(ColumnName, ColList).

is_foreign_key(ColumnName, ?TABLE(_TName, _Policy, _Cols, FKeys, _Idx)) ->
    Aux = lists:dropwhile(fun(?FK(FkName, _FkType, _RefTable, _RefCol)) ->
        ColumnName /= FkName end, FKeys),
    case Aux of
        [] -> false;
        [_Col | _] -> true
    end.

% RecordData represents a single record, i.e. a list of tuples on the form:
% {{col_name, datatype}, value}
shadow_column_state(TableName, ShadowCol, RecordData, TxId) ->
    %io:format(">> shadow_column_state:~n", []),
    ?FK(FkName, FkType, _RefTable, _RefCol) = ShadowCol,
    %%io:format("ShadowCol: ~p~n", [ShadowCol]),
    ColName = {column_name(FkName), type_to_crdt(FkType, undefined)},
    %%io:format("ColName: ~p~n", [ColName]),
    RefColValue = lookup_value(ColName, RecordData),
    %%io:format("RefColValue: ~p~n", [RefColValue]),
    StateObjKey = querying_utils:build_keys({TableName, FkName}, ?SHADOW_COL_DT, ?AQL_METADATA_BUCKET),
    %io:format("StateObjKey: ~p~n", [StateObjKey]),
    [ShColData] = querying_utils:read_keys(value, StateObjKey, TxId),
    %io:format("ShColData: ~p~n", [ShColData]),
    RefColName = {RefColValue, ?SHADOW_COL_ENTRY_DT},
    State = lookup_value(RefColName, ShColData),
    %io:format("State: ~p~n", [State]),
    State.

record_data(PKeys, TableName, TxId) when is_list(PKeys) ->
    PKeyAtoms = lists:map(fun(PKey) -> querying_utils:to_atom(PKey) end, PKeys),
    ObjKeys = querying_utils:build_keys(PKeyAtoms, ?TABLE_DT, TableName),
    case querying_utils:read_keys(value, ObjKeys, TxId) of
        [[]] -> [];
        ObjValues -> ObjValues
    end;
record_data(PKey, TableName, TxId) ->
    record_data([PKey], TableName, TxId).

get_column(_ColumnName, []) -> undefined;
get_column({ColumnName, CRDT}, Record) ->
    case proplists:lookup({ColumnName, CRDT}, Record) of
        none -> undefined;
        Entry -> Entry
    end;
get_column(ColumnName, Record) ->
    Aux = lists:dropwhile(fun(?ATTRIBUTE(Column, _Type, _Value)) ->
        Column /= ColumnName end, Record),
    case Aux of
        [] -> undefined;
        [Col | _] -> Col
    end.

lookup_value(_ColumnName, []) -> [];
lookup_value({ColumnName, CRDT}, Record) ->
    proplists:get_value({ColumnName, CRDT}, Record);
lookup_value(ColumnName, Record) ->
    case get_column(ColumnName, Record) of
        ?ATTRIBUTE(_Column, _Type, Value) -> Value;
        undefined -> undefined
    end.

crdt_to_op(?CRDT_INTEGER, Value) -> {assign, Value};
crdt_to_op(?CRDT_VARCHAR, Value) -> {assign, Value};
crdt_to_op(?CRDT_BOOLEAN, Value) ->
    case Value of
        true -> {enable, {}};
        false -> {disable, {}}
    end;
crdt_to_op(?CRDT_BCOUNTER_INT, Value) when is_tuple(Value) ->
    {Inc, Dec} = Value,
    IncList = orddict:to_list(Inc),
    DecList = orddict:to_list(Dec),
    SumInc = lists:sum([Val || {_Ids, Val} <- IncList]),
    SumDec = lists:sum([Val || {_Ids, Val} <- DecList]),
    IncUpdate = increment_counter(SumInc),
    DecUpdate = decrement_counter(SumDec),
    lists:flatten([IncUpdate, DecUpdate]);
crdt_to_op(_, _) -> {error, invalid_crdt}.

%% ====================================================================
%% Internal functions
%% ====================================================================

column_name([{_TableName, ColName}]) -> ColName;
column_name(FkName) -> FkName.

type_to_crdt(?AQL_INTEGER, _) -> ?CRDT_INTEGER;
type_to_crdt(?AQL_BOOLEAN, _) -> ?CRDT_BOOLEAN;
type_to_crdt(?AQL_COUNTER_INT, {_, _}) -> ?CRDT_BCOUNTER_INT;
type_to_crdt(?AQL_COUNTER_INT, _) -> ?CRDT_COUNTER_INT;
type_to_crdt(?AQL_VARCHAR, _) -> ?CRDT_VARCHAR.

increment_counter(0) -> [];
increment_counter(Value) when is_integer(Value) ->
    bcounter_op(increment, Value).

decrement_counter(0) -> [];
decrement_counter(Value) when is_integer(Value) ->
    bcounter_op(decrement, Value).

bcounter_op(Op, Value) ->
    {Op, {Value, term}}.
