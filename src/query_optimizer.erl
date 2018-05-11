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

-define(FUNCTION(Name, Params), {func, Name, Params}).

%% API
-export([query_filter/2,
         get_partial_object/2,
         get_partial_object/3,
         get_partial_object/5]).

%% TODO support more than one table per filter, for supporting join queries
query_filter(Filter, TxId) when is_list(Filter) ->
    %io:format(">> query_filter:~n", []),

    TableName = table(Filter),
    Table = table_utils:table_metadata(TableName, TxId),

    ProjectionCols = projection(Filter),
    Conditions = conditions(Filter),

    %io:format("Table: ~p~n", [Table]),

    FilteredResult =
        case Conditions of
            [] ->
                Index = indexing:read_index(primary, TableName, TxId),
                read_records(Index, TableName, TxId);
            _Else ->
                apply_filter(Conditions, Table, TxId)
        end,
    ResultToList = case is_list(FilteredResult) of
                       false -> sets:to_list(FilteredResult);
                       true -> FilteredResult
                   end,
    %io:format("ResultToList: ~p~n", [ResultToList]),
    %io:format("ProjectionCols: ~p~n", [ProjectionCols]),
    case ProjectionCols of
        ?WILDCARD ->
            {ok, apply_projection(table_utils:column_names(Table), ResultToList)};
        _Proj ->
            {ok, apply_projection(ProjectionCols, ResultToList)}
    end.

get_partial_object(Key, Type, Bucket, Filter, TxId) ->
    ObjectKey = querying_utils:build_keys(Key, Type, Bucket),
    [Object] = querying_utils:read_keys(ObjectKey, TxId),
    {ok, apply_projection(Filter, Object)}.
get_partial_object(ObjectKey, Filter, TxId) when is_tuple(ObjectKey) ->
    [Object] = querying_utils:read_keys(ObjectKey, TxId),
    {ok, apply_projection(Filter, Object)}.
get_partial_object(Object, Filter) when is_list(Object) ->
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
apply_filter(Conditions, Table, TxId) ->
    %io:format(">> apply_filter:~n", []),
    %io:format("Conditions: ~p~n", [Conditions]),
    case is_disjunction(Conditions) of
        true ->
            lists:foldl(fun(Conjunction, FinalRes) ->
                %PartialResult = iterate_conditions(Conjunction, Table, TxId, []),
                {RemainConds, PartialRes} = read_subqueries(Conjunction, Table, TxId, [], nil),
                PartialResult = read_remaining(RemainConds, Table, PartialRes, TxId),

                ResultSet = sets:from_list(PartialResult),
                %%io:format("ResultSet: ~p~n", [ResultSet]),
                sets:union(FinalRes, ResultSet)
            end, sets:new(), Conditions);
        false -> throw("The current condition is not valid")
    end.

read_subqueries([{sub, Conds} | Tail], Table, TxId, RemainConds, PartialRes) ->
    %io:format(">> process_subqueries: is sub query~n", []),
    Result = apply_filter(Conds, Table, TxId),
    ResultSet = case is_list(Result) of
                       true -> sets:from_list(Result);
                       false -> Result
                end,
    %io:format("PartialRes: ~p~n", [PartialRes]),
    %io:format("ResultSet: ~p~n", [ResultSet]),
    Intersection = case PartialRes of
                       nil -> ResultSet;
                       _Else -> sets:intersection(PartialRes, ResultSet)
                   end,
    %io:format("Intersection: ~p~n", [Intersection]),
    read_subqueries(Tail, Table, TxId, RemainConds, Intersection);
read_subqueries([Cond | Tail], Table, TxId, RemainConds, PartialRes) ->
    read_subqueries(Tail, Table, TxId, lists:append(RemainConds, [Cond]), PartialRes);
read_subqueries([], _Table, _TxId, RemainConds, nil) ->
    {RemainConds, nil};
read_subqueries([], Table, TxId, RemainConds, PartialRes) ->
    AddFks = get_shadow_columns(Table, sets:to_list(PartialRes), TxId),
    {RemainConds, AddFks}.

read_remaining(_Conditions, _Table, [], _TxId) -> [];
read_remaining(Conditions, Table, CurrentData, TxId) ->
    %io:format(">> process_remaining:~n"),
    %io:format("Conditions: ~p~n", [Conditions]),
    %io:format("Table: ~p~n", [Table]),
    %io:format("CurrentData: ~p~n", [CurrentData]),
    TableName = table_utils:table(Table),
    RangeQueries = range_queries:get_range_query(Conditions),
    %io:format("RangeQueries: ~p~n", [RangeQueries]),
    case RangeQueries of
        nil -> [];
        _Else ->
            case CurrentData of
                nil ->
                    {Remain, Indexes} = dict:fold(fun(Column, _Range, {RemainAcc, IdxAcc}) ->
                        %?CONDITION(Column, _Comp, _Val) = Col,
                        case is_func(Column) of
                            true -> {lists:append(RemainAcc, [Column]), IdxAcc};
                            false ->
                                case table_utils:is_primary_key(Column, Table) of
                                    true -> {RemainAcc, lists:append(IdxAcc, [{primary, TableName}])};
                                    false ->
                                        SIndexes = table_utils:indexes(Table),
                                        case find_index_by_attribute(Column, SIndexes) of
                                            [] -> {lists:append(RemainAcc, [Column]), IdxAcc};
                                            [SIndex] -> {RemainAcc, lists:append(IdxAcc, [{secondary, SIndex}])}
                                        end
                                end
                        end
                    end, {[], []}, RangeQueries),
                    %io:format("Remain: ~p~n", [Remain]),
                    %io:format("Indexes: ~p~n", [Indexes]),
                    LeastKeys = lists:foldl(fun(Index, Curr) ->
                        ReadKeys = case Index of
                                       {primary, TableName} ->
                                           IdxData = indexing:read_index(primary, TableName, TxId),
                                           [PKCol] = table_utils:primary_key_name(Table),
                                           %io:format("IdxData: ~p~n", [IdxData]),
                                           %io:format("PKCol: ~p~n", [PKCol]),
                                           GetRange = range_queries:lookup_range(PKCol, RangeQueries),
                                           %io:format("GetRange: ~p~n", [GetRange]),
                                           FilterFun = read_pk_predicate(GetRange),
                                           ordsets:from_list(filter_keys(FilterFun, IdxData));
                                       {secondary, {Name, TName, [Col]}} -> %% TODO support more columns
                                           GetRange = range_queries:lookup_range(Col, RangeQueries),
                                           %io:format("GetRange: ~p~n", [GetRange]),
                                           IdxData = case range_type(GetRange) of
                                                         equality ->
                                                             {{_, Val}, {_, Val}} = GetRange,
                                                             Res = indexing:read_index_function(secondary, {TName, Name}, {get, Val}, TxId),
                                                             [Res];
                                                         notequality ->
                                                             {{_, Val}, {_, Val}} = GetRange,
                                                             Aux = indexing:read_index(secondary, {TName, Name}, TxId),
                                                             lists:filter(fun({IdxVal, Set}) -> IdxVal /= Set end, Aux);
                                                         range ->
                                                             indexing:read_index_function(secondary,
                                                                 {TName, Name}, {range, range_queries:to_condition(GetRange)}, TxId)
                                                     end,
                                           %io:format("IdxData: ~p~n", [IdxData]),
                                           %IdxData = indexing:read_index_function(secondary, {TName, Name}, {range, GetRange}, TxId),
                                           lists:foldl(fun({_IdxCol, PKs}, Set) ->
                                               %% there's an assumption that the accumulator will never have repeated keys
                                               ordsets:union(Set, PKs)
                                           end, ordsets:new(), IdxData)
                                   end,
                        case Curr of
                            nil -> ReadKeys;
                            Curr -> ordsets:intersection(Curr, ReadKeys)
                        end
                    end, nil, Indexes),
                    %io:format("LeastKeys: ~p~n", [LeastKeys]),
                    case LeastKeys of
                        nil ->
                            %% TODO this is a must change: iterate the range queries and filter
                            iterate_conditions(Conditions, Table, TxId, []);
                        LeastKeys ->
                            KeyList = ordsets:to_list(LeastKeys),
                            Objects = read_records(KeyList, TableName, TxId),
                            %io:format("Objects: ~p~n", [Objects]),
                            ObjsData = lists:foldl(fun(RemainCol, AccObjs) ->
                                GetRange = range_queries:lookup_range(RemainCol, RangeQueries),
                                FilterFun = read_predicate(GetRange),
                                filter_objects({RemainCol, FilterFun}, Table, AccObjs, TxId)
                            end, Objects, Remain),
                            get_shadow_columns(Table, ObjsData, TxId)
                    end;
                CurrentData -> %% TODO
                    %io:format("CurrentData: ~p~n", [CurrentData]),
                    ObjsData = lists:foldl(fun(Condition, PartRes) ->
                        filter_objects(Condition, Table, PartRes, TxId)
                    end, CurrentData, Conditions),
                    ObjsData
                    %get_shadow_columns(Table, ObjsData, TxId)
            end
    end.


iterate_conditions([{sub, Conds} = _Cond | Tail], Table, TxId, Acc) ->
    %io:format(">> iterate_conditions:~n", []),
    %io:format("(is_subquery) Current Cond: ~p~n", [Cond]),
    iterate_conditions([Conds | Tail], Table, TxId, Acc);
iterate_conditions([Cond | Tail], Table, TxId, Acc) ->
    %io:format(">> iterate_conditions:~n", []),
    case is_disjunction(Cond) of
        true ->
            %io:format("(is_disjunction) Current Cond: ~p~n", [Cond]),
            FiltObjects = apply_filter(Cond, Table, TxId),
            ResultToList = case is_list(FiltObjects) of
                               false -> sets:to_list(FiltObjects);
                               true -> FiltObjects
                           end,
            %%io:format("RecordObjs: ~p~n", [ResultToList]),
            iterate_conditions(Tail, Table, TxId, ResultToList);
        false ->
            %io:format("Current Cond: ~p~n", [Cond]),
            RecordObjs = case Acc of
                             [] -> retrieve_and_filter(Cond, Table, TxId);
                             _Else -> filter_objects(Cond, Table, Acc, TxId)
                         end,
            %%io:format("RecordObjs: ~p~n", [RecordObjs]),
            iterate_conditions(Tail, Table, TxId, RecordObjs)
    end;
iterate_conditions([], _Table, _TxId, Acc) ->
    Acc.

retrieve_and_filter(Condition, Table, TxId) ->
    %io:format(">> retrieve_and_filter:~n", []),

    ?CONDITION(Column, _Comparison, _Value) = Condition,
    %TableName = table_utils:table(Table),
    %io:format("Condition: ~p~n", [Condition]),
    %io:format("TableName: ~p~n", [TableName]),
    ObjData = case is_func(Column) of
                  true -> function_filtering(Condition, Table, TxId);
                  false -> column_filtering(Condition, Table, TxId)
    end,
    %io:format("ObjData: ~p~n", [ObjData]),
    %ObjData.
    %% read shadow columns as well and add them and their resp. values to the object
    get_shadow_columns(Table, ObjData, TxId).

%% Restriction: only functions are allowed to access foreign key states
function_filtering({Func, Predicate}, Table, Records, TxId) ->
    ?FUNCTION(FuncName, Args) = Func,
    TableName = table_utils:table(Table),
    case builtin_functions:is_function({FuncName, Args}) of
        true ->
            AllCols = table_utils:all_column_names(Table),
            %% TODO This assumes that a function can only reference one column. Support more in the future.
            {_, [{ColPos, ConditionCol}]} = lists:foldl(fun(Arg, Acc) ->
                {Pos, Cols} = Acc,
                case lists:member(Arg, AllCols) of
                    true -> {Pos + 1, lists:append(Cols, [{Pos, Arg}])};
                    false -> {Pos + 1, Cols}
                end
            end, {0, []}, Args),

            lists:foldl(fun(Record, Acc) ->
                ColValue = case table_utils:is_foreign_key(ConditionCol, Table) of
                               true ->
                                   EntryKey = {ConditionCol, ?SHADOW_COL_ENTRY_DT},
                                   case table_utils:get_column(EntryKey, Record) of
                                       undefined ->
                                           ShCol = shadow_column_spec(ConditionCol, Table),
                                           table_utils:shadow_column_state(TableName, ShCol, Record, TxId);
                                       ?ATTRIBUTE(_C, _T, V) -> V
                                   end;
                               false -> ?ATTRIBUTE(_C, _T, V) = table_utils:get_column(ConditionCol, Record), V
                           end,

                %io:format("ColValue: ~p~n", [ColValue]),
                ReplaceArgs = querying_utils:replace(ColPos, ColValue, Args),
                Result = builtin_functions:exec({FuncName, ReplaceArgs}),
                %Pred = comp_to_predicate(Op, querying_utils:to_atom(Value)),
                %io:format("Result: ~p~n", [Result]),
                case Predicate(Result) of
                    true -> %io:format("Predicate(Result) = true~n", []),
                        lists:append(Acc, [Record]);
                    false -> %io:format("Predicate(Result) = false~n", []),
                        Acc
                end
            end, [], Records);
        false -> throw(lists:concat(["Invalid function: ", Func]))
    end;
function_filtering(Condition, Table, Records, TxId) ->
    ?CONDITION(Func, {Op, _}, Value) = Condition,
    Predicate = comp_to_predicate(Op, querying_utils:to_atom(Value)),
    function_filtering({Func, Predicate}, Table, Records, TxId).

function_filtering(Condition, Table, TxId) ->
    TableName = table_utils:table(Table),
    ReadKeys = indexing:read_index(primary, TableName, TxId),
    Records = read_records(ReadKeys, TableName, TxId),
    function_filtering(Condition, Table, Records, TxId).

column_filtering(Condition, Table, TxId) ->
    %% TODO need to validate the column (i.e. the column is part of the table)
    ?CONDITION(_Column, Comparison, Value) = Condition,
    {Op, _} = Comparison,
    TableName = table_utils:table(Table),
    ValueAtom = querying_utils:to_atom(Value),
    ReadKeys = compute_readkeys(Condition, Table, TxId),
    case ReadKeys of
        Pks when is_list(Pks) ->
            FilteredPks = filter_keys(comp_to_predicate(Op, ValueAtom), Pks),
            read_records(FilteredPks, TableName, TxId);
        {index, Index} ->
            IndexedKeys = indexing:get_indexed_values(Index),
            FilteredIdxKeys = filter_keys(comp_to_predicate(Op, Value), IndexedKeys),
            PKs = indexing:get_primary_keys(FilteredIdxKeys, Index),
            read_records(PKs, TableName, TxId);
        {full, Keys} ->
            Records = read_records(Keys, TableName, TxId),
            filter_objects(Condition, Table, Records, TxId)
    end.

%% Given a condition, read all the necessary keys for computing and filtering the
%% final result. The necessary keys may be retrieved in the form of a primary key
%% or in the form of indexes.
compute_readkeys(?CONDITION(Column, Op, Value), Table, TxId) when is_atom(Column) ->
    TableName = table_utils:table(Table),
    case table_utils:is_primary_key(Column, Table) of
        true ->
            case Op of
                {equality, _} -> [querying_utils:to_atom(Value)];
                _Op -> indexing:read_index(primary, TableName, TxId)
            end;
        false ->
            SIndexes = table_utils:indexes(Table),
            SIndex = find_index_by_attribute(Column, SIndexes),
            case SIndex of
                [] ->
                    %% full table scan
                    {full, indexing:read_index(primary, TableName, TxId)};
                [{IdxName, TName, _Cols}] -> %% TODO support more than one index
                    %% read the index object denoted by '#2i_tablename.index'
                    {index, indexing:read_index(secondary, {TName, IdxName}, TxId)}
            end
    end.

filter_objects({Column, Predicate}, Table, Objects, TxId) ->
    case is_func(Column) of
        true -> function_filtering({Column, Predicate}, Table, Objects, TxId);
        false -> filter_objects(Column, Predicate, Objects, TxId, [])
    end;
filter_objects(Condition, Table, Objects, TxId) ->
    %io:format(">> filter_objects:~n", []),
    %io:format("Objects: ~p~n", [Objects]),
    ?CONDITION(Column, Comparison, Value) = Condition,
    {Op, _} = Comparison,
    Predicate = comp_to_predicate(Op, Value),
    filter_objects({Column, Predicate}, Table, Objects, TxId).
    %filter_objects(Column, Predicate, Objects, []).

filter_objects(Column, Predicate, [Object | Objs], TxId, Acc) when is_list(Object) ->
    Find = lists:filter(fun(Attr) ->
        ?ATTRIBUTE(ColName, _CRDT, Val) = Attr,
        Column == ColName andalso Predicate(Val)
    end, Object),
    %io:format("Find: ~p~n", [Find]),
    case Find of
        [] -> filter_objects(Column, Predicate, Objs, TxId, Acc);
        _Else -> filter_objects(Column, Predicate, Objs, TxId, lists:append(Acc, [Object]))
    end;
filter_objects(_Column, _Predicate, [], _TxId, Acc) ->
    Acc.

filter_keys(_Predicate, []) -> [];
filter_keys(Predicate, Keys) when is_list(Keys) ->
    %io:format(">> filter_keys:~n", []),
    %io:format("Predicate: ~p~n", [Predicate]),
    %io:format("Keys: ~p~n", [Keys]),
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


%% TODO support this search to comprise indexes with multiple attributes
find_index_by_attribute(_Attribute, []) -> [];
find_index_by_attribute(Attribute, IndexList) when is_list(IndexList) ->
    lists:foldl(fun(Elem, Acc) ->
        ?INDEX(_IdxName, _TName, Cols) = Elem,
        case lists:member(Attribute, Cols) of
            true -> lists:append(Acc, [Elem]);
            false -> Acc
        end
     end, [], IndexList);
find_index_by_attribute(_Attribute, _Idx) -> [].

get_shadow_columns(_Table, [], _TxId) -> [];
get_shadow_columns(Table, RecordsData, TxId) ->
    %io:format(">> get_shadow_columns:~n", []),
    TableName = table_utils:table(Table),
    ForeignKeys = table_utils:foreign_keys(Table),

    NewRecordsData = lists:foldl(fun(ForeignKey, Acc) ->
        ?FK(FkName, _FkType, _RefTableName, _RefColName) = ForeignKey,
        lists:map(fun(RecordData) ->
            case table_utils:get_column({FkName, ?SHADOW_COL_ENTRY_DT}, RecordData) of
                undefined ->
                    FkState = table_utils:shadow_column_state(TableName, ForeignKey, RecordData, TxId),
                    NewEntry = ?ATTRIBUTE(FkName, ?SHADOW_COL_ENTRY_DT, FkState),
                    lists:append(RecordData, [NewEntry]);
                _Else ->
                    RecordData
            end
        end, Acc)
    end, RecordsData, ForeignKeys),
    %io:format("NewRecordsData: ~p~n", [NewRecordsData]),
    NewRecordsData.

shadow_column_spec(FkName, Table) ->
    ForeignKeys = table_utils:foreign_keys(Table),
    Search = lists:dropwhile(fun(?FK(Name, _Type, _RefTableName, _RefColName)) -> FkName /= Name end, ForeignKeys),
    case Search of
        [] -> none;
        [Result | _] -> Result
    end.

apply_projection([], Objects) ->
    Objects;
apply_projection(Projection, Objects) when is_list(Objects) ->
    apply_projection(Projection, Objects, []);
apply_projection(Projection, Object) ->
    apply_projection(Projection, [Object]).

apply_projection(Projection, [Object | Objs], Acc) ->
    %io:format(">> apply_projection:~n", []),
    FilteredObj = lists:filter(fun(Attr) ->
        ?ATTRIBUTE(ColName, _CRDT, _Val) = Attr,
        lists:member(ColName, Projection)
    end, Object),
    %io:format("FilteredObj: ~p~n", [FilteredObj]),
    apply_projection(Projection, Objs, lists:append(Acc, [FilteredObj]));
apply_projection(_Projection, [], Acc) ->
    Acc.

read_records(PKey, TableName, TxId) ->
    table_utils:record_data(PKey, TableName, TxId).

is_disjunction(Query) ->
    querying_utils:is_list_of_lists(Query).

is_func(?FUNCTION(_Name, _Params)) -> true;
is_func(_) -> false.

range_type({{greatereq, _Val}, {lessereq, _Val}}) -> equality;
range_type({{greater, _Val}, {lesser, _Val}}) -> notequality;
range_type(_) -> range.

read_predicate(Range) ->
    case range_queries:to_predicate(Range) of
        {{_, LPred}, infinity} ->
            fun(V) -> LPred(V) end;
        {infinity, {_, RPred}} ->
            fun(V) -> RPred(V) end;
        {{_, LPred}, {_, RPred}} ->
            fun(V) -> LPred(V) andalso RPred(V) end;
        Pred when is_function(Pred) -> Pred
    end.

read_pk_predicate(Range) ->
    {{LB, Val1}, {RB, Val2}} = Range,
    NewRange = {{LB, querying_utils:to_atom(Val1)}, {RB, querying_utils:to_atom(Val2)}},
    read_predicate(NewRange).

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

%%filter_test() ->
%%    Objects = [
%%        [{{'Col1', crdt}, "A1"}, {{'Col2', crdt}, "Sam"}, {{'Col3', crdt}, 2016}],
%%        [{{'Col1', crdt}, "A2"}, {{'Col2', crdt}, "Jon"}, {{'Col3', crdt}, 2008}],
%%        [{{'Col1', crdt}, "A3"}, {{'Col2', crdt}, "Rob"}, {{'Col3', crdt}, 2012}]
%%    ],
%%    Condition1 = {'Col3', {greater, ignore}, 2008},
%%    ExpectedResult1 = [
%%        [{{'Col1', crdt}, "A1"}, {{'Col2', crdt}, "Sam"}, {{'Col3', crdt}, 2016}],
%%        [{{'Col1', crdt}, "A3"}, {{'Col2', crdt}, "Rob"}, {{'Col3', crdt}, 2012}]
%%    ],
%%    Result = filter_objects(Condition1, Objects),
%%    ?assertEqual(Result, ExpectedResult1).

-endif.
