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

-define(RANGE(Lower, Upper), {Lower, Upper}).
-define(INFINITY, {open, infinity}).
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
    %GroupedConds = iterate_conditions(Conditions, []),
    {RangeQueries, Status} = dict:fold(fun(Col, CList, {DictAcc, CurrStatus}) ->
        case CurrStatus of
            nil -> {DictAcc, CurrStatus};
            ok ->
                Range = get_range(CList, {?INFINITY, ?INFINITY}),
                case Range of
                    nil -> {DictAcc, nil};
                    ?RANGE({LB, LV}, {RB, RV}) ->
                        NewRange = ?RANGE(to_readable_bound({lower, LB}, LV), to_readable_bound({upper, RB}, RV)),
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

to_predicate(?RANGE({LBound, Val}, {RBound, Val})) ->
    case {LBound, RBound} of
        ?EQUALITY -> fun(V) -> V == Val end;
        ?NOTEQUALITY -> fun(V) -> V /= Val end
    end;
to_predicate(?RANGE({LBound, LVal}, {RBound, RVal})) ->
    LowerComp = to_pred(LBound, LVal),
    UpperComp = to_pred(RBound, RVal),
    {LowerComp, UpperComp}.

to_condition(?RANGE({LBound, LVal}, {RBound, RVal})) ->
    %LowerBound = to_cond(LBound, LVal),
    %UpperBound = to_cond(RBound, RVal),
    ?RANGE(to_pred(LBound, LVal), to_pred(RBound, RVal)).

%% ====================================================================
%% Internal functions
%% ====================================================================
get_range([{Comparator, Value} | Tail], Range) ->
    NewRange = update_range(Comparator, Value, Range),
    get_range(Tail, NewRange);
get_range([], Range) -> Range.

update_range(_Comp, _Val, nil) -> nil;
update_range(Comparator, Value, ?RANGE({_, LVal} = Lower, {_, RVal} = Upper) = Range) ->
    ToRange = to_range(Comparator, Value),
    case intersects(ToRange, Range) of
        true ->
            CompToBound = check_bound(Comparator),
            case bound_type(Comparator) of
                lower when LVal == infinity orelse Value > LVal ->
                    ?RANGE({CompToBound, Value}, Upper);
                upper when RVal == infinity orelse Value < RVal ->
                    ?RANGE(Lower, {CompToBound, Value});
                both ->
                    ?RANGE({CompToBound, Value}, {CompToBound, Value});
                _ ->
                    ?RANGE(Lower, Upper)
            end;
        false ->
            nil
    end.

bound_type(greater) -> lower;
bound_type(greatereq) -> lower;
bound_type(lesser) -> upper;
bound_type(lessereq) -> upper;
bound_type(_) -> both.

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

to_range(greater, Val) -> ?RANGE({open, Val}, ?INFINITY);
to_range(greatereq, Val) -> ?RANGE({close, Val}, ?INFINITY);
to_range(lesser, Val) -> ?RANGE(?INFINITY, {open, Val});
to_range(lessereq, Val) -> ?RANGE(?INFINITY, {close, Val});
to_range(equality, Val) -> ?RANGE({close, Val}, {close, Val});
to_range(notequality, Val) -> ?RANGE({open, Val}, {open, Val}).

to_pred(_, infinity) -> infinity;
to_pred({BType, Bound}, Val) ->
    Cond = to_cond(BType, Bound),
    to_pred(Cond, Val);
to_pred(CondType, Val) ->
    {CondType, func(CondType, Val)}.

to_readable_bound(_, infinity) -> {nil, infinity};
to_readable_bound({BType, Bound}, Val) ->
    {to_cond(BType, Bound), Val}.

intersects(_, ?RANGE(?INFINITY, ?INFINITY)) -> true;
intersects(?RANGE({open, Val}, {open, Val}), Range) ->
    not intersects(?RANGE({close, Val}, {close, Val}), Range);
intersects(Range1, Range2) ->
    ?RANGE({LBound1, LVal1}, {RBound1, RVal1}) = Range1,
    ?RANGE({LBound2, LVal2}, {RBound2, RVal2}) = Range2,
    Comp1 = case det_bound_pred(RBound1, LBound2) of
                open -> lesser;
                close -> lessereq
            end,
    Comp2 = case det_bound_pred(LBound1, RBound2) of
                open -> greater;
                close -> greatereq
            end,
    not (compare(Comp1, RVal1, LVal2) orelse compare(Comp2, LVal1, RVal2)).

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
    Range1 = ?RANGE({open, 5}, {open, 10}),
    Range2 = ?RANGE({close, 6}, {close, 6}),
    Range3 = ?RANGE(?INFINITY, ?INFINITY),
    Range4 = ?RANGE(?INFINITY, {open, 15}),
    Range5 = ?RANGE({open, 11}, ?INFINITY),
    Range6 = ?RANGE({open, 4}, {open, 4}),
    ?assertEqual(true, intersects(Range1, Range3)),
    ?assertEqual(true, intersects(Range2, Range1)),
    ?assertEqual(true, intersects(Range4, Range1)),
    ?assertEqual(false, intersects(Range1, Range5)),
    ?assertEqual(true, intersects(Range6, Range1)),
    ?assertEqual(false, intersects(Range6, Range4)).

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

    Expected = [{'Col1', ?RANGE({greater, 2008}, {lesser, 2016})}, {'Col2', ?RANGE({greater, 500}, {lesser, 1000})}],
    Expected2 = [{'Col1', ?RANGE({greatereq, 2010}, {lessereq, 2010})}, {'Col2', ?RANGE({greater, 500}, {lesser, 1000})}],
    Expected3 = [{'Col1', ?RANGE({greater, 2008}, {nil, infinity})}],

    ?assertEqual(Expected, dict:to_list(get_range_query(Conditions))),
    ?assertEqual(Expected, dict:to_list(get_range_query(Conditions2))),
    ?assertEqual(Expected2, dict:to_list(get_range_query(Conditions3))),
    ?assertEqual(nil, get_range_query(Conditions4)),
    ?assertEqual(Expected3, dict:to_list(get_range_query(Conditions5))).

-endif.
