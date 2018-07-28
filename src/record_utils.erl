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
%%% @doc An Antidote module that contains operations that concern
%%%      the (AQL's representation of) records.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(record_utils).
-include("querying.hrl").

%% API
-export([record_data/2,
    record_data/3,
    get_column/2,
    lookup_value/2,
    delete_record/2,
    satisfies_function/4,
    satisfies_predicate/3]).

record_data(Keys, TxId) when is_list(Keys) ->
    case querying_utils:read_keys(value, Keys, TxId) of
        [[]] -> [];
        ObjValues -> ObjValues
    end;
record_data(Key, TxId) ->
    record_data([Key], TxId).

record_data(PKeys, TableName, TxId)
    when is_list(PKeys) andalso is_atom(TableName) ->
    PKeyAtoms = [querying_utils:to_atom(PKey) || PKey <- PKeys],
    ObjKeys = querying_utils:build_keys(PKeyAtoms, ?TABLE_DT, TableName),
    case querying_utils:read_keys(value, ObjKeys, TxId) of
        [[]] -> [];
        ObjValues -> ObjValues
    end;
record_data(PKeys, Table, TxId)
    when is_list(PKeys) ->
    PKeyAtoms = [querying_utils:to_atom(PKey) || PKey <- PKeys],
    ZippedKeys = lists:zip(PKeyAtoms, PKeys),
    ObjKeys = querying_utils:build_keys_from_table(ZippedKeys, Table, TxId),
    case querying_utils:read_keys(value, ObjKeys, TxId) of
        [[]] -> [];
        ObjValues -> ObjValues
    end;
record_data(PKey, Table, TxId) ->
    record_data([PKey], Table, TxId).

get_column(_ColumnName, []) -> undefined;
get_column({ColumnName, CRDT}, Record) ->
    case proplists:lookup({ColumnName, CRDT}, Record) of
        none -> undefined;
        Entry -> Entry
    end;
get_column(ColumnName, Record) ->
    querying_utils:first_occurrence(
        fun(?ATTRIBUTE(Column, _Type, _Value)) ->
            Column == ColumnName
        end, Record).

lookup_value(_ColumnName, []) -> [];
lookup_value({ColumnName, CRDT}, Record) ->
    proplists:get_value({ColumnName, CRDT}, Record);
lookup_value(ColumnName, Record) ->
    case get_column(ColumnName, Record) of
        ?ATTRIBUTE(_Column, _Type, Value) -> Value;
        undefined -> undefined
    end.

delete_record(ObjKey, TxId) ->
    StateKey = {?STATE_COL, ?STATE_COL_DT},
    StateOp = crdt_utils:to_insert_op(?CRDT_VARCHAR, 'd'),
    MapOp = {StateKey, StateOp},
    Update = crdt_utils:create_crdt_update(ObjKey, update, MapOp),
    ok = querying_utils:write_keys(Update, TxId),
    false.

satisfies_function(_, _, [], _) -> false;
satisfies_function({Func, Predicate}, Table, Record, TxId) ->
    ?FUNCTION(FuncName, Args) = Func,
    TableName = table_utils:name(Table),
    AllCols = table_utils:all_column_names(Table),

    ReplaceArgs = builtin_functions:replace_args({FuncName, Args}, TableName, AllCols, Record),
    AppendTable = case FuncName of
                      assert_visibility ->
                          lists:append([ReplaceArgs, [Table]]);
                      _ ->
                          ReplaceArgs
                  end,
    Result = builtin_functions:exec({FuncName, AppendTable}, TxId),
    Predicate(Result).

satisfies_predicate(_, _, []) -> false;
satisfies_predicate(Column, Predicate, Record) when is_list(Record) ->
    Find = querying_utils:first_occurrence(
        fun(?ATTRIBUTE(ColName, CRDT, Val)) ->
            ConvVal = crdt_utils:convert_value(CRDT, Val),
            Column == ColName andalso Predicate(ConvVal)
        end, Record),

    Find =/= undefined.
