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
         shadow_column_state/4,
         record_data/3]).

table(?TABLE(TName, _Policy, _Cols, _SCols, _Idx)) -> TName.

policy(?TABLE(_TName, Policy, _Cols, _SCols, _Idx)) -> Policy.

columns(?TABLE(_TName, _Policy, Cols, _SCols, _Idx)) -> Cols.

foreign_keys(?TABLE(_TName, _Policy, _Cols, SCols, _Idx)) -> SCols.

indexes(?TABLE(_TName, _Policy, _Cols, _SCols, Idx)) -> Idx.

column_names(Table) ->
    Columns = columns(Table),
    maps:get(?COLUMNS, Columns).

%shadow_column_state(TableName, ShadowCols, RecordData, TxId) when is_list(ShadowCols) ->
%    TNameAtom = querying_commons:to_atom(TableName),
%    shadow_column_state(TNameAtom, ShadowCols, RecordData, TxId, []);
%shadow_column_state(TableName, ShadowCol, RecordData, TxId) ->
%    shadow_column_state(TableName, [ShadowCol], RecordData, TxId).

% RecordData representes a single record, i.e. a list of tuples on the form:
% {{col_name, datatype}, value}
shadow_column_state(TableName, ShadowCol, RecordData, TxId) ->
    io:format(">> shadow_column_state:~n", []),
    ?FK(FkName, FkType, _RefTable, _RefCol) = ShadowCol,
    %%io:format("ShadowCol: ~p~n", [ShadowCol]),
    ColName = {column_name(FkName), type_to_crdt(FkType, undefined)},
    %%io:format("ColName: ~p~n", [ColName]),
    RefColValue = lookup_column(ColName, RecordData),
    %%io:format("RefColValue: ~p~n", [RefColValue]),
    StateObjKey = querying_commons:build_keys({TableName, FkName}, ?SHADOW_COL_DT, ?AQL_METADATA_BUCKET),
    %%io:format("StateObjKey: ~p~n", [StateObjKey]),
    [ShColData] = querying_commons:read_keys(StateObjKey, TxId),
    %io:format("ShColData: ~p~n", [ShColData]),
    RefColName = {RefColValue, ?SHADOW_COL_ENTRY_DT},
    State = lookup_column(RefColName, ShColData),
    io:format("State: ~p~n", [State]),
    State.
    %shadow_column_state(TableName, Tail, RecordData, TxId, lists:append(Acc, [State]));
%shadow_column_state(_TableName, [], _RecordData, _TxId, Acc) ->
%    Acc.

record_data(TableName, PKey, TxId) ->
    PKeyAtom = querying_commons:to_atom(PKey),
    ObjKey = querying_commons:build_keys(PKeyAtom, ?TABLE_DT, TableName),
    querying_commons:read_keys(ObjKey, TxId).

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

lookup_column(_ColumnName, []) -> [];
lookup_column(ColumnName, Record) ->
    proplists:get_value(ColumnName, Record).
%lookup_column(ColumnName, Data) when is_list(Data) ->
%    lookup_column(ColumnName, Data, []).
%lookup_column(ColumnName, [Record | Tail], Acc) ->
%    NewAcc = lists:append(Acc, [proplists:get_value(ColumnName, Record)]),
%    lookup_column(ColumnName, Tail, NewAcc);
%lookup_column(_ColumnName, [], Acc) ->
%    Acc.
