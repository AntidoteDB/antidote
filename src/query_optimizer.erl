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
%%% @doc An Antidote module to filter database objects according to
%%%      the Antidote Query Language (AQL) schema.
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(query_optimizer).

-include("querying.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([query_filter/2,
         get_partial_object/3,
         get_partial_object/5]).

%% TODO support more than one table per filter, for supporting join queries
query_filter(Filter, TxId) when is_list(Filter) ->
    io:format(">> query_filter:~n", []),

    TableMetadata = read_metadata(TxId),
    TableName = table(Filter),
    Table = lookup_table(TableName, TableMetadata),

    ProjectionCols = projection(Filter),
    Conditions = conditions(Filter),

    io:format("Metadata: ~p~n", [TableMetadata]),
    io:format("Table: ~p~n", [Table]),

    FilteredResult =
        case Conditions of
            [] ->
                Index = indexing:read_index(primary, TableName, TxId),
                read_records(Index, TableName, TxId);
            _Else ->
                apply_filter(Conditions, Table, TxId)
        end,
    io:format("FilteredResult: ~p~n", [FilteredResult]),
    ResultToList = case is_list(FilteredResult) of
                       false -> sets:to_list(FilteredResult);
                       true -> FilteredResult
                   end,
    io:format("ProjectionCols: ~p~n", [ProjectionCols]),
    case ProjectionCols of
        ?WILDCARD ->
            {ok, apply_projection(table_utils:column_names(Table), ResultToList)};
        _Proj ->
            {ok, apply_projection(ProjectionCols, ResultToList)}
    end.

get_partial_object(Key, Type, Bucket, Filter, TxId) ->
    ObjectKey = querying_commons:build_keys(Key, Type, Bucket),
    [Object] = querying_commons:read_keys(ObjectKey, TxId),
    {ok, apply_projection(Filter, Object)}.
get_partial_object(ObjectKey, Filter, TxId) when is_tuple(ObjectKey) ->
    [Object] = querying_commons:read_keys(ObjectKey, TxId),
    {ok, apply_projection(Filter, Object)}.

table(Filter) ->
    {_, [TableName]} = lists:keyfind(tables, 1, Filter),
    TableName.
projection(Filter) ->
    {_, Columns} = lists:keyfind(projection, 1, Filter),
    Columns.
conditions(Filter) ->
    {_, Conditions} = lists:keyfind(conditions, 1, Filter),
    Conditions.

%% ====================================================================
%% Internal functions
%% ====================================================================
read_metadata(TxId) ->
    ObjKey = querying_commons:build_keys(?AQL_METADATA_KEY, ?TABLE_DT, ?AQL_METADATA_BUCKET),
    [Meta] = querying_commons:read_keys(ObjKey, TxId),
    Meta.

lookup_table(TableName, Metadata) ->
    TableNameAtom = querying_commons:to_atom(TableName),
    MetadataKey = {TableNameAtom, ?TABLE_NAME_DT},
    proplists:get_value(MetadataKey, Metadata).

apply_filter(Conditions, Table, TxId) when is_list(Conditions) ->
    %%io:format(">> apply_filter:~n", []),
    io:format("Conditions: ~p~n", [Conditions]),
    lists:foldl(fun(Conjunction, FinalRes) ->
        PartialResult = iterate_conditions(Conjunction, Table, TxId, []),
        ResultSet = sets:from_list(PartialResult),
        %%io:format("ResultSet: ~p~n", [ResultSet]),
        sets:union(FinalRes, ResultSet)
    end, sets:new(), Conditions).

iterate_conditions([Cond | Tail], Table, TxId, _Acc) when is_list(Cond) ->
    io:format("(is_list) Current Cond: ~p~n", [Cond]),
    FiltObjects = apply_filter(Cond, Table, TxId),
    ResultToList = case is_list(FiltObjects) of
                       false -> sets:to_list(FiltObjects);
                       true -> FiltObjects
                   end,
    %%io:format("RecordObjs: ~p~n", [ResultToList]),
    iterate_conditions(Tail, Table, TxId, ResultToList);
iterate_conditions([Cond | Tail], Table, TxId, Acc) ->
    io:format("Current Cond: ~p~n", [Cond]),
    RecordObjs = case Acc of
                     [] -> retrieve_and_filter(Cond, Table, TxId);
                     _Else -> filter_objects(Cond, Acc)
                 end,
    %%io:format("RecordObjs: ~p~n", [RecordObjs]),
    iterate_conditions(Tail, Table, TxId, RecordObjs);
iterate_conditions([], _Table, _TxId, Acc) ->
    Acc.

retrieve_and_filter(Condition, Table, TxId) ->
    io:format(">> retrieve_and_filter:~n", []),
    ?CONDITION(Column, Comparison, Value) = Condition,
    {Op, _} = Comparison,
    TableName = table_utils:table(Table),
    io:format("Condition: ~p~n", [Condition]),
    io:format("TableName: ~p~n", [TableName]),
    %% TODO need to validate the column (i.e. the column is part of the table)
    ObjData = case is_primary_key(Column, Table) of
                   true ->
                       io:format("Primary Key? Yes~n", []),
                       ReadKeys = case Op of
                           equality -> Value;
                           _Op ->
                               Index = indexing:read_index(primary, TableName, TxId),
                               filter_keys(comp_to_predicate(Op, Value), Index)
                       end,
                       read_records(ReadKeys, TableName, TxId);
                   false ->
                       io:format("Primary Key? No~n", []),
                       SIndexes = table_utils:indexes(Table),
                       SIndex = find_index_by_attribute(Column, SIndexes),
                       case SIndex of
                           false ->
                               %% full table scan
                               Index = indexing:read_index(primary, TableName, TxId),
                               Records = read_records(Index, TableName, TxId),
                               filter_objects(Condition, Records);
                           {IdxName, TName, _Cols} ->
                               %% read the index object denoted by '#2i_tablename.index'
                               SIndexObject = indexing:read_index(secondary, lists:concat([TName, '.', IdxName]), TxId),
                               IndexedKeys = indexing:get_indexed_values(SIndexObject),
                               FilteredIdxKeys = filter_keys(comp_to_predicate(Op, Value), IndexedKeys),
                               PKs = indexing:get_primary_keys(FilteredIdxKeys, SIndexObject),
                               read_records(PKs, TName, TxId)
                       end
               end,
    %io:format("ReadKeys: ~p~n", [ReadKeys]),
    %ObjKeys = querying_commons:build_keys(ReadKeys, ?TABLE_DT, TableName),
    %ObjData = case querying_commons:read_keys(ObjKeys, TxId) of
    %              [[]] -> [];
    %              ObjValues -> ObjValues
    %          end,
    io:format("ObjData: ~p~n", [ObjData]),
    %% TODO read shadow columns as well and add them and their resp. values to the object
    get_shadow_columns(Table, ObjData, TxId).
    %lists:append(ObjData, ShCols).

filter_objects(Condition, Objects) ->
    ?CONDITION(Column, Comparison, Value) = Condition,
    {Op, _} = Comparison,
    Predicate = comp_to_predicate(Op, Value),
    filter_objects(Column, Predicate, Objects, []).
filter_objects(Column, Predicate, [Object | Objs], Acc) when is_list(Object) ->
    Find = lists:filter(fun(Attr) ->
        ?ATTRIBUTE(ColName, _CRDT, Val) = Attr,
        Column == ColName andalso Predicate(Val)
    end, Object),
    case Find of
        [] -> filter_objects(Column, Predicate, Objs, Acc);
        _Else -> filter_objects(Column, Predicate, Objs, lists:append(Acc, [Object]))
    end;
filter_objects(_Column, _Predicate, [], Acc) ->
    Acc.

filter_keys(_Predicate, []) -> [];
filter_keys(Predicate, Keys) when is_list(Keys) ->
    lists:filter(Predicate, Keys).

comp_to_predicate(Comparator, Value) ->
    case Comparator of
        equality -> fun(Elem) -> Elem == Value end;
        notequality -> fun(Elem) -> Elem /= Value end;
        greater -> fun(Elem) -> Elem > Value end;
        greatereq -> fun(Elem) -> Elem >= Value end;
        lesser -> fun(Elem) -> Elem < Value end;
        lessereq -> fun(Elem) -> Elem =< Value end
    end.

is_primary_key(ColumnName, ?TABLE(_TName, _Policy, Cols, _FKeys, _Idx)) when is_map(Cols) ->
    ColList = maps:get(?PK_COLUMN, Cols),
    lists:member(ColumnName, ColList).

%% TODO support this search to comprise indexes with multiple attributes
find_index_by_attribute(_Attribute, []) -> false;
find_index_by_attribute(Attribute, IndexList) when is_list(IndexList) ->
    lists:foldl(fun(Elem, Acc) ->
        ?INDEX(_IdxName, _TName, Cols) = Elem,
        case lists:member(Attribute, Cols) of
            true -> lists:append(Acc, [Elem]);
            false -> Acc
        end
     end, [], IndexList);
find_index_by_attribute(_Attribute, _Idx) -> false.

get_shadow_columns(_Table, [], _TxId) -> [];
get_shadow_columns(Table, RecordsData, TxId) ->
    %%io:format(">> get_shadow_columns:~n", []),
    TableName = table_utils:table(Table),
    ForeignKeys = table_utils:foreign_keys(Table),

    NewRecordsData = lists:foldl(fun(ForeignKey, Acc) ->
        ?FK(FkName, _FkType, _RefTableName, _RefColName) = ForeignKey,
        lists:map(fun(RecordData) ->
            FkState = table_utils:shadow_column_state(TableName, ForeignKey, RecordData, TxId),
            NewEntry = ?ATTRIBUTE(FkName, ?SHADOW_COL_ENTRY_DT, FkState),
            lists:append(RecordData, [NewEntry])
            end, Acc)
        end, RecordsData, ForeignKeys),
    %io:format("NewRecordsData: ~p~n", [NewRecordsData]),
    NewRecordsData.


    %FkStates = table_utils:shadow_column_state(TableName, ForeignKeys, RecordData, TxId),
    %io:format("FkStates: ~p~n", [FkStates]),
    %FkCRDTs = lists:map(fun(?FK(FkName, _FkType, _RefTableName, _RefColName)) ->
    %    {FkName, ignore} %% TODO SHADOW_COL_DT or SHADOW_COL_ENTRY_DT?
    %    end
    %,ForeignKeys),
    %io:format("FkCRDTs: ~p~n", [FkCRDTs]),
    %lists:zip(FkCRDTs, FkStates).

apply_projection([], Objects) ->
    Objects;
apply_projection(Projection, Objects) when is_list(Objects) ->
    apply_projection(Projection, Objects, []);
apply_projection(Projection, Object) ->
    apply_projection(Projection, [Object]).

apply_projection(Projection, [Object | Objs], Acc) ->
    %%io:format(">> apply_projection:~n", []),
    FilteredObj = lists:filter(fun(Attr) ->
        ?ATTRIBUTE(ColName, _CRDT, _Val) = Attr,
        lists:member(ColName, Projection)
    end, Object),
    apply_projection(Projection, Objs, lists:append(Acc, [FilteredObj]));
apply_projection(_Projection, [], Acc) ->
    Acc.

read_records(PKeys, TableName, TxId) when is_list(PKeys) ->
    ObjKeys = querying_commons:build_keys(PKeys, ?TABLE_DT, TableName),
    case querying_commons:read_keys(ObjKeys, TxId) of
        [[]] -> [];
        ObjValues -> ObjValues
    end;
read_records(PKey, TableName, TxId) ->
    read_records([PKey], TableName, TxId).

%%====================================================================
%% Eunit tests
%%====================================================================

-ifdef(TEST).

comparison_convert_test() ->
    ValueList = [0, 1, 2, 3, 4],
    Op1 = equality,
    Op2 = notequality,
    Op3 = greater,
    Val = 2,
    Res1 = filter_keys(comp_to_predicate(Op1, Val), ValueList),
    Res2 = filter_keys(comp_to_predicate(Op2, Val), ValueList),
    Res3 = filter_keys(comp_to_predicate(Op3, Val), ValueList),
    ?assertEqual(Res1, [2]),
    ?assertEqual(Res2, [0, 1, 3, 4]),
    ?assertEqual(Res3, [3, 4]).

projection_test() ->
    Objects = [
        [{{'Col1', crdt}, "A1"}, {{'Col2', crdt}, "B1"}, {{'Col3', crdt}, "C1"}],
        [{{'Col1', crdt}, "A2"}, {{'Col2', crdt}, "B2"}, {{'Col3', crdt}, "C2"}],
        [{{'Col1', crdt}, "A3"}, {{'Col2', crdt}, "B3"}, {{'Col3', crdt}, "C3"}]
    ],
    ExpectedResult1 = [
        [{{'Col1', crdt}, "A1"}, {{'Col2', crdt}, "B1"}],
        [{{'Col1', crdt}, "A2"}, {{'Col2', crdt}, "B2"}],
        [{{'Col1', crdt}, "A3"}, {{'Col2', crdt}, "B3"}]
    ],
    Result = apply_projection(['Col1', 'Col2'], Objects),
    ?assertEqual(Result, ExpectedResult1).

filter_test() ->
    Objects = [
        [{{'Col1', crdt}, "A1"}, {{'Col2', crdt}, "Sam"}, {{'Col3', crdt}, 2016}],
        [{{'Col1', crdt}, "A2"}, {{'Col2', crdt}, "Jon"}, {{'Col3', crdt}, 2008}],
        [{{'Col1', crdt}, "A3"}, {{'Col2', crdt}, "Rob"}, {{'Col3', crdt}, 2012}]
    ],
    Condition1 = {'Col3', {greater, ignore}, 2008},
    ExpectedResult1 = [
        [{{'Col1', crdt}, "A1"}, {{'Col2', crdt}, "Sam"}, {{'Col3', crdt}, 2016}],
        [{{'Col1', crdt}, "A3"}, {{'Col2', crdt}, "Rob"}, {{'Col3', crdt}, 2012}]
    ],
    Result = filter_objects(Condition1, Objects),
    ?assertEqual(Result, ExpectedResult1).

-endif.
