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
%%% @doc An Antidote module that manages range queries.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(range_queries).

-include("querying.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(RANGE(LowerBound, LowerVal, UpperBound, UpperVal), {{LowerBound, LowerVal}, {UpperBound, UpperVal}}).
-define(RANGE_INFINITY, {{open, infinity}, {open, infinity}}).
-define(RANGE_INEQ(Val), {{open, Val}, {open, Val}}).
-define(RANGE_EQ(Val), {{close, Val}, {close, Val}}).
-define(EQUALITY, {greatereq, lessereq}).
-define(NOTEQUALITY, {greater, lesser}).

%% API
-export([get_range_query/1, lookup_range/2, to_predicate/1, to_condition/1]).

get_range_query(Conditions) ->
    GroupedConds = lists:foldl(fun(Condition, MapAcc) ->
        ?CONDITION(Column, {Comparison, _}, Value) = Condition,
        case dict:is_key(Column, MapAcc) of
            true -> dict:update(Column, fun(CondList) -> lists:append(CondList, [{Comparison, Value}]) end, MapAcc);
            false -> dict:store(Column, [{Comparison, Value}], MapAcc)
        end
    end, dict:new(), Conditions),

    {RangeQueries, Status} = dict:fold(fun(Col, CList, {DictAcc, CurrStatus}) ->
        case CurrStatus of
            nil -> {DictAcc, CurrStatus};
            ok ->
                Range = get_range(CList, {?RANGE_INFINITY, []}),
                case Range of
                    nil -> {DictAcc, nil};
                    Range ->
                        %io:format(">> get_range_query:~n", []),
                        %io:format("Range: ~p~n", [Range]),
                        NewRange = to_readable_bound(Range),
                        {dict:store(Col, NewRange, DictAcc), CurrStatus}
                end
        end
    end, {dict:new(), ok}, GroupedConds),
    case Status of
        ok -> RangeQueries;
        nil -> nil
    end.

lookup_range(Key, Ranges) ->
    case dict:find(Key, Ranges) of
        {ok, Range} -> Range;
        error -> []
    end.

to_predicate({?RANGE(nil, infinity, nil, infinity), Excluded}) ->
    {ignore, fun(V) -> not lists:member(V, Excluded) end};
to_predicate({?RANGE(greatereq, Val, lessereq, Val), Excluded}) ->
    Equality = fun(V) -> V == Val end,
    Inequality = fun(V) -> not lists:member(V, Excluded) end,
    {Equality, Inequality};
to_predicate({?RANGE(LBound, LVal, UBound, UVal), Excluded}) ->
    LowerComp = to_pred(LBound, LVal),
    UpperComp = to_pred(UBound, UVal),
    Inequality = fun(V) -> not lists:member(V, Excluded) end,
    {{LowerComp, UpperComp}, Inequality}.

to_condition({?RANGE(LBound, LVal, UBound, UVal), _Excluded}) ->
    {to_pred(LBound, LVal), to_pred(UBound, UVal)}.

%% ====================================================================
%% Internal functions
%% ====================================================================
get_range([{Comparator, Value} | Tail], Range) ->
    NewRange = update_range(Comparator, Value, Range),
    get_range(Tail, NewRange);
get_range([], Range) -> Range.

update_range(_Comp, _Val, nil) -> nil;
update_range(Comparator, Value, {Range, Excluded}) -> %%?RANGE(LBound, LVal, RBound, RVal) = Range) ->
    %% Range = {<range>, <excluded>}
    %% , where <range> is a range and <excluded> are values from inequalities
    ToRange = to_range(Comparator, Value),
    case intersects(ToRange, {Range, Excluded}) of
        true ->
            CompToBound = check_bound(Comparator),
            ?RANGE(LBound, LVal, UBound, UVal) = Range,
            case bound_type(Comparator) of
                lower when LVal == infinity orelse Value > LVal ->
                    {?RANGE(CompToBound, Value, UBound, UVal), Excluded};
                upper when UVal == infinity orelse Value < UVal ->
                    {?RANGE(LBound, LVal, CompToBound, Value), Excluded};
                equal ->
                    {?RANGE(CompToBound, Value, CompToBound, Value), Excluded};
                notequal ->
                    ?RANGE_INEQ(Val) = ToRange,
                    case lists:member(Val, Excluded) of
                        true -> {Range, Excluded};
                        false -> {Range, lists:append(Excluded, [Val])}
                    end;
                _ ->
                    {Range, Excluded}
            end;
        false ->
            nil
    end.

bound_type(greater) -> lower;
bound_type(greatereq) -> lower;
bound_type(lesser) -> upper;
bound_type(lessereq) -> upper;
bound_type(equality) -> equal;
bound_type(notequality) -> notequal.

check_bound(greater) -> open;
check_bound(greatereq) -> close;
check_bound(lesser) -> open;
check_bound(lessereq) -> close;
check_bound(equality) -> close;
check_bound(notequality) -> open.

to_cond(lower, open) -> greater;
to_cond(lower, close) -> greatereq;
to_cond(upper, open) -> lesser;
to_cond(upper, close) -> lessereq.

to_range(greater, Val) -> ?RANGE(open, Val, open, infinity);
to_range(greatereq, Val) -> ?RANGE(close, Val, open, infinity);
to_range(lesser, Val) -> ?RANGE(open, infinity, open, Val);
to_range(lessereq, Val) -> ?RANGE(open, infinity, close, Val);
to_range(equality, Val) -> ?RANGE_EQ(Val);
to_range(notequality, Val) -> ?RANGE_INEQ(Val).

to_pred(_, infinity) -> infinity;
to_pred({BType, Bound}, Val) ->
    Cond = to_cond(BType, Bound),
    to_pred(Cond, Val);
to_pred(CondType, Val) ->
    {CondType, func(CondType, Val)}.

to_readable_bound({Range, Excluded}) ->
    ?RANGE(LB, LV, UB, UV) = Range,
    {Cond1, Val1} = to_readable_bound({lower, LB}, LV),
    {Cond2, Val2} = to_readable_bound({upper, UB}, UV),

    {?RANGE(Cond1, Val1, Cond2, Val2) , Excluded}.

to_readable_bound(_, infinity) -> {nil, infinity};
to_readable_bound({BType, Bound}, Val) ->
    {to_cond(BType, Bound), Val}.

intersects(?RANGE_EQ(Val) = Range1, {Range2, Excluded}) ->
    %io:format(">> intersects 1:~n", []),
    %io:format("~p~n", [Range1]),
    %io:format("~p~n", [Range2]),
    %io:format("~p~n", [Expected]),
    not lists:member(Val, Excluded) and intersects_ranges(Range1, Range2);
intersects(Range1, {Range2, _Excluded}) ->
    %io:format(">> intersects 2:~n", []),
    %io:format("~p~n", [Range1]),
    %io:format("~p~n", [Range2]),
    intersects_ranges(Range1, Range2).

intersects_ranges(_, ?RANGE_INFINITY) -> true;
intersects_ranges(?RANGE_INFINITY, _) -> true;
intersects_ranges(?RANGE_INEQ(Val), ?RANGE(Bound, Val, Bound, Val)) -> false;
intersects_ranges(?RANGE_INEQ(_Val), _) -> true;
intersects_ranges(Range1, Range2) ->
    ?RANGE(LBound1, LVal1, UBound1, UVal1) = Range1,
    ?RANGE(LBound2, LVal2, UBound2, UVal2) = Range2,
    Comp1 = case det_bound_pred(UBound1, LBound2) of
                open -> lesser;
                close -> lessereq
            end,
    Comp2 = case det_bound_pred(LBound1, UBound2) of
                open -> greater;
                close -> greatereq
            end,
    not (compare(Comp1, UVal1, LVal2) orelse compare(Comp2, LVal1, UVal2)).

compare(_Op, _Val, infinity) -> false;
compare(_Op, infinity, _Val) -> false;
compare(Op, Val1, Val2) ->
    Fun = func(Op, Val2),
    Fun(Val1).

func(greater, Val) -> fun(V) -> V > Val end;
func(greatereq, Val) -> fun(V) -> V >= Val end;
func(lesser, Val) -> fun(V) -> V < Val end;
func(lessereq, Val) -> fun(V) -> V =< Val end;
func(equality, Val) -> fun(V) -> V == Val end;
func(notequality, Val) -> fun(V) -> V /= Val end.

det_bound_pred(close, close) -> open;
det_bound_pred(_, _) -> close.

%%====================================================================
%% Eunit tests
%%====================================================================

-ifdef(TEST).

intersection_test() ->
    Range1 = ?RANGE(open, 5, open, 10),
    Range2 = ?RANGE_EQ(6),
    Range3 = ?RANGE_INFINITY,
    Range4 = ?RANGE(open, infinity, open, 15),
    Range5 = ?RANGE(open, 11, open, infinity),
    Range6 = ?RANGE_INEQ(4),
    Range7 = ?RANGE_EQ(5),

    ?assertEqual(true, intersects(Range1, {Range3, []})),
    ?assertEqual(true, intersects(Range2, {Range1, []})),
    ?assertEqual(true, intersects(Range4, {Range1, []})),
    ?assertEqual(false, intersects(Range1, {Range5, []})),
    ?assertEqual(true, intersects(Range6, {Range1, []})),
    ?assertEqual(true, intersects(Range4, {Range3, [4]})),
    ?assertEqual(true, intersects(Range5, {Range3, [4]})),
    ?assertEqual(false, intersects(Range7, {Range1, []})).

range_test() ->
    Conditions = [
        {'Col1', {greater, ignore}, 2008},
        {'Col2', {lesser, ignore}, 1000},
        {'Col1', {lesser, ignore}, 2016},
        {'Col2', {greater, ignore}, 500}
    ],
    Conditions2 = lists:append(Conditions, [{'Col1', {greatereq, ignore}, 2008}]),
    Conditions3 = lists:append(Conditions, [{'Col1', {equality, ignore}, 2010}]),
    Conditions4 = lists:append(Conditions, [{'Col2', {equality, ignore}, 2000}]),
    Conditions5 = [{'Col1', {greater, ignore}, 2008}],

    Expected = [{'Col1', {?RANGE(greater, 2008, lesser, 2016), []}}, {'Col2', {?RANGE(greater, 500, lesser, 1000), []}}],
    Expected2 = [{'Col1', {?RANGE(greatereq, 2010, lessereq, 2010), []}}, {'Col2', {?RANGE(greater, 500, lesser, 1000), []}}],
    Expected3 = [{'Col1', {?RANGE(greater, 2008, nil, infinity), []}}],

    ?assertEqual(Expected, dict:to_list(get_range_query(Conditions))),
    ?assertEqual(Expected, dict:to_list(get_range_query(Conditions2))),
    ?assertEqual(Expected2, dict:to_list(get_range_query(Conditions3))),
    ?assertEqual(nil, get_range_query(Conditions4)),
    ?assertEqual(Expected3, dict:to_list(get_range_query(Conditions5))).

notequal_test() ->
    Conditions1 = [
        {'Col1', {lessereq, ignore}, 2008},
        {'Col1', {notequality, ignore}, 2005}
    ],
    Conditions2 = [
        {'Col1', {notequality, ignore}, 2008},
        {'Col1', {notequality, ignore}, 2005}
    ],
    Conditions3 = [
        {'Col1', {equality, ignore}, 2005},
        {'Col1', {notequality, ignore}, 2005}
    ],

    Expected1 = [{'Col1', {?RANGE(nil, infinity, lessereq, 2008), [2005]}}],
    Expected2 = [{'Col1', {?RANGE(nil, infinity, nil, infinity), [2008, 2005]}}],
    Expected3 = nil,

    Result1 = get_range_query(Conditions1),
    Result2 = get_range_query(Conditions2),
    Result3 = get_range_query(Conditions3),
    ?assertEqual(Expected1, dict:to_list(Result1)),
    ?assertEqual(Expected2, dict:to_list(Result2)),
    ?assertEqual(Expected3, Result3).

-endif.
