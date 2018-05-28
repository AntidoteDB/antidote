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
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% @author pedrolopes
%% @doc module antidote_crdt_index_go - A grow-only index
%%
%% An operation-based CRDT, very similar to antidote_crdt_gmap.
%% It keeps two maps, the index map and the indirection map:
%%  - the index map stores index entries, each one maps an indexed
%%    value and a set of primary keys;
%%  - the indirection map stores the inverted bindings between primary
%%    keys and indexed values and has the same behaviour as the gmap.
%%
%% This CRDT does not support entry deletions.
%% ------------------------------------------------------------------

-module(antidote_crdt_index_go).
-behaviour(antidote_crdt).

-define(LOWER_BOUND_PRED, [greater, greatereq]).
-define(UPPER_BOUND_PRED, [lesser, lessereq]).
-define(WRONG_PRED(Preds), io_lib:format("Some of the predicates don't respect a range query: ~p", [Preds])).

%% API
-export([new/0,
         new/1,
         value/1,
         value/2,
         downstream/2,
         update/2,
         equal/2,
         to_binary/1, from_binary/1,
         is_operation/1,
         require_state_downstream/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type gindex() :: {gindex_type(), indexmap(), indirectionmap()}.
-type gindex_type() :: atom().
-type gb_tree_node() :: nil | {_, _, _, _}.
-type indexmap() :: {non_neg_integer(), gb_tree_node()}.
-type indirectionmap() :: dict:dict({Key::term(), Type::atom()}, NestedState::term()).

-type pred_type() :: greater | greatereq | lesser | lessereq.
-type pred_arg() :: number().
-type predicate() :: {pred_type(), pred_arg()} | infinity.

-type gindex_query() :: {range, {predicate(), predicate()}} |
                        {get, term()} |
                        {lookup, term()}.

-type gindex_op() :: {update, nested_op()} | {update, [nested_op()]}.
-type nested_op() :: {{Key::term(), Type::atom()}, Op::term()}.
-type gindex_effect() :: {update, nested_downstream()} | {update, [nested_downstream()]}.
-type nested_downstream() :: {{Key::term(), Type::atom()}, Op::term()}.

-type invalid_type() :: {error, wrong_type}.
-type key_not_found() :: {error, key_not_found}.
-type wrong_predicate() :: erlang:throw(string()).
-type value_output() :: [{term(), term()}] | {term(), term()} |
                        invalid_type() |
                        key_not_found() |
                        wrong_predicate().

-spec new() -> gindex().
new() ->
    {undefined, gb_trees:empty(), dict:new()}. %% TODO benchmark the possible choices for the index: dict, orddict, gb_trees

-spec new(term()) -> gindex().
new(Type) ->
    case antidote_crdt:is_type(Type) of
        true -> {Type, gb_trees:empty(), dict:new()};
        false -> new()
    end.

-spec value(gindex()) -> value_output().
value({_Type, Index, _Indirection}) ->
    gb_trees:to_list(Index).

-spec value(gindex_query(), gindex()) -> value_output().
value({range, {LowerPred, UpperPred}}, {_Type, Index, _Indirection}) ->
    case validate_pred(lower, LowerPred) andalso validate_pred(upper, UpperPred) of
        true ->
            %io:format("gindex: ~p~n", [Index]),
            Iterator = case LowerPred of
                           infinity -> gb_trees:iterator(Index);
                           _ -> gb_trees:iterator_from(lookup_lower_bound(LowerPred, Index), Index)
                       end,
            iterate_and_filter({UpperPred, [key]}, gb_trees:next(Iterator), []);
            %Iterator = gb_trees:iterator(Index),
            %iterate_and_filter2({LowerPred, [key]}, {UpperPred, [key]}, gb_trees:next(Iterator), []);
        false ->
            throw(lists:flatten(?WRONG_PRED({LowerPred, UpperPred})))
    end;
value({get, Key}, {_Type, Index, _Indirection}) ->
    case gb_trees:lookup(Key, Index) of
        {value, Value} -> {Key, Value};
        none -> {error, key_not_found}
    end;
value({lookup, Key}, {Type, _Index, Indirection} = GIndex) ->
    case dict:find(Key, Indirection) of
        {ok, CRDTValue} ->
            Value = Type:value(CRDTValue),
            value({get, Value}, GIndex);
        error -> {error, key_not_found}
    end.

-spec downstream(gindex_op(), gindex()) -> {ok, gindex_effect()} | invalid_type().
downstream({update, {Type, Key, Op}}, {_Type, _Index, Indirection} = GIndex) ->
    case index_type(GIndex, Type) of
        Type ->
            CurrentValue = case dict:is_key(Key, Indirection) of
                               true -> dict:fetch(Key, Indirection);
                               false -> Type:new()
                           end,
            {ok, DownstreamOp} = Type:downstream(Op, CurrentValue),
            {ok, {update, {Type, Key, DownstreamOp}}};
        _Else -> {error, wrong_type}
    end;
downstream({update, Ops}, GIndex) when is_list(Ops) ->
    {ok, {update, lists:map(fun(Op) -> {ok, DSOp} = downstream({update, Op}, GIndex), DSOp end, Ops)}}.

-spec update(gindex_effect(), gindex()) -> {ok, gindex()}.
update({update, {Type, Key, Op}}, {_Type, Index, Indirection}) ->
    {OldValue, NewValue} = case dict:find(Key, Indirection) of
        {ok, Value} ->
            {ok, ValueUpdated} = Type:update(Op, Value),
            {Value, ValueUpdated};
        error ->
            NewCRDT = Type:new(),
            {ok, NewValueUpdated} = Type:update(Op, NewCRDT),
            {undefined, NewValueUpdated}
    end,

    NewIndirection = dict:store(Key, NewValue, Indirection),

    NewIndex = update_index(get_value(Type, OldValue), get_value(Type, NewValue), Key, Index),
    {ok, {Type, NewIndex, NewIndirection}};
update({update, Ops}, Map) ->
    apply_ops(Ops, Map).

-spec equal(gindex(), gindex()) -> boolean().
equal({Type1, Index1, Indirection1}, {Type2, Index2, Indirection2}) ->
    Type1 =:= Type2 andalso
    Index1 =:= Index2 andalso
    dict:size(Indirection1) =:= dict:size(Indirection2) andalso
    rec_equals(Type1, Indirection1, Indirection2).

-define(TAG, 101).
-define(V1_VERS, 1).

-spec to_binary(gindex()) -> binary().
to_binary(GIndex) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(GIndex))/binary>>.

-spec from_binary(binary()) -> {ok, gindex()}.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    {ok, binary_to_term(Bin)}.

-spec is_operation(term()) -> boolean().
is_operation(Operation) ->
    case Operation of
        {update, {Type, _Key, Op}} ->
            antidote_crdt:is_type(Type)
                andalso Type:is_operation(Op);
        {update, Ops} when is_list(Ops) ->
            distinct([Key || {Key, _} <- Ops])
                andalso lists:all(fun(Op) -> is_operation({update, Op}) end, Ops);
        _ ->
            false
    end.

-spec require_state_downstream(term()) -> boolean().
require_state_downstream(_Op) ->
    true.

%% ===================================================================
%% Internal functions
%% ===================================================================
index_type({Type, _Index, _Indirection}, Default) ->
    case Type of
        undefined -> Default;
        Default -> Type;
        _Else -> undefined
    end;
index_type({_Index, Indirection}, Default) ->
    Keys = dict:fetch_keys(Indirection),
    Types = lists:foldl(
        fun({_Key, Type}, SetAcc) -> sets:add_element(Type, SetAcc) end,
        sets:new(), Keys),
    case sets:to_list(Types) of
        [Type] -> Type;
        [] -> Default;
        _Else -> undefined
    end.

update_index(OldEntryKey, NewEntryKey, EntryValue, Index) ->
    Removed = case gb_trees:lookup(OldEntryKey, Index) of
        {value, Set} ->
            case ordsets:is_element(EntryValue, Set) of
                true -> gb_trees:update(OldEntryKey, ordsets:del_element(EntryValue, Set), Index);
                false -> full_search(EntryValue, Index)
            end;
        none -> Index
    end,

    case gb_trees:lookup(NewEntryKey, Removed) of
         {value, Set2} -> gb_trees:update(NewEntryKey, ordsets:add_element(EntryValue, Set2), Removed);
         none -> gb_trees:insert(NewEntryKey, ordsets:add_element(EntryValue, ordsets:new()), Removed)
    end.

get_value(_Type, undefined) -> undefined;
get_value(Type, CRDTValue) ->
    Value = Type:value(CRDTValue),
    calc_value(Type, Value).

%% A special case for a bounded counter, where the value of an index entry
%% supported by this CRDT corresponds to the difference between the sum of
%% increments and the sum of decrements.
calc_value(antidote_crdt_counter_b, {Inc, Dec}) ->
    IncList = orddict:to_list(Inc),
    DecList = orddict:to_list(Dec),
    SumInc = sum_values(IncList),
    SumDec = sum_values(DecList),
    SumInc - SumDec;
calc_value(_, Value) -> Value.

sum_values(List) when is_list(List) -> lists:sum([Value || {_Ids, Value} <- List]).

apply_ops([], GIndex) ->
    {ok, GIndex};
apply_ops([Op | Rest], GIndex) ->
    {ok, GIndex2} = update(Op, GIndex),
    apply_ops(Rest, GIndex2).

rec_equals(Type, Indirection1, Indirection2) ->
    IndList1 = dict:to_list(Indirection1),
    IndList2 = dict:to_list(Indirection2),
    Remaining = lists:dropwhile(fun({Key, Value}) ->
        case proplists:lookup(Key, IndList2) of
            none -> false;
            {Key, Value2} ->
                Type:equal(Value, Value2)
        end
    end, IndList1),
    length(Remaining) =:= 0.

distinct([]) -> true;
distinct([X | Xs]) ->
    not lists:member(X, Xs) andalso distinct(Xs).

lookup_lower_bound(_LowerPred, {0, _Tree}) -> nil;
lookup_lower_bound(LowerPred, {Size, Tree}) when Size > 0 ->
    lookup_lower_bound(LowerPred, Tree, nil).
lookup_lower_bound(_LowerPred, nil, Final) ->
    Final;
lookup_lower_bound(LowerPred, {Key, _Value, Left, Right}, Final) ->
    case apply_pred(LowerPred, Key) of
        true -> lookup_lower_bound(LowerPred, Left, Key);
        false -> lookup_lower_bound(LowerPred, Right, Final)
    end.

iterate_and_filter(_Predicate, none, Acc) ->
    Acc;
iterate_and_filter({infinity, _} = Predicate, {Key, Value, Iter}, Acc) ->
    iterate_and_filter(Predicate, gb_trees:next(Iter), lists:append(Acc, [{Key, Value}]));
iterate_and_filter({Bound, Params} = Predicate, {Key, Value, Iter}, Acc) ->
    Result = case Params of
                 [key] -> apply_pred(Bound, Key);
                 [value, V] -> apply_pred(Bound, [Value, V])
             end,
    case Result of
        true -> iterate_and_filter(Predicate, gb_trees:next(Iter), lists:append(Acc, [{Key, Value}]));
        false -> iterate_and_filter(Predicate, gb_trees:next(Iter), Acc)
    end.

%%iterate_and_filter2(_Predicate1, _Predicate2, none, Acc) ->
%%    Acc;
%%iterate_and_filter2({infinity, _} = Predicate1, Predicate2, {Key, Value, Iter}, Acc) ->
%%    iterate_and_filter2(Predicate1, Predicate2, gb_trees:next(Iter), lists:append(Acc, [{Key, Value}]));
%%iterate_and_filter2({Bound1, Params1} = Predicate1, {Bound2, Params2} = Predicate2, {Key, Value, Iter}, Acc) ->
%%    Result = case {Params1, Params2} of
%%                 {[key], [key]} -> apply_pred(Bound1, Key) andalso apply_pred(Bound2, Key);
%%                 {[value, V1], [value, V2]} -> apply_pred(Bound1, [Value, V1]) andalso apply_pred(Bound2, [Value, V2])
%%             end,
%%    case Result of
%%        true -> iterate_and_filter2(Predicate1, Predicate2, gb_trees:next(Iter), lists:append(Acc, [{Key, Value}]));
%%        false -> iterate_and_filter2(Predicate1, Predicate2, gb_trees:next(Iter), Acc)
%%    end.

full_search(EntryValue, Index) ->
    Iterator = gb_trees:iterator(Index),
    FilterFun = fun([Set, V]) -> ordsets:is_element(V, Set) end,
    case iterate_and_filter({FilterFun, [value, EntryValue]}, gb_trees:next(Iterator), []) of
        [] -> Index;
        Entries ->
            lists:foldl(fun({Key, Value}, AccIndex) ->
                gb_trees:update(Key, ordsets:del_element(EntryValue, Value), AccIndex)
            end, Index, Entries)
    end.

validate_pred(_BoundType, infinity) -> true;
validate_pred(lower, {Type, _Val}) ->
    lists:member(Type, ?LOWER_BOUND_PRED);
validate_pred(upper, {Type, _Val}) ->
    lists:member(Type, ?UPPER_BOUND_PRED).

apply_pred(infinity, _Param) -> true;
apply_pred({Type, Val}, Param) ->
    Func = to_predicate(Type, Val),
    Func(Param);
apply_pred(Func, Param) when is_function(Func) ->
    Func(Param).

to_predicate(greater, Val) -> fun(V) -> V > Val end;
to_predicate(greatereq, Val) -> fun(V) -> V >= Val end;
to_predicate(lesser, Val) -> fun(V) -> V < Val end;
to_predicate(lessereq, Val) -> fun(V) -> V =< Val end;
to_predicate(equality, Val) -> fun(V) -> V == Val end;
to_predicate(notequality, Val) -> fun(V) -> V /= Val end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%tree_insertions(Number, TreeAcc) ->
%    gb_trees:enter(Number, Number, TreeAcc).

new_test() ->
    ?assertEqual({undefined, gb_trees:empty(), dict:new()}, new()),
    ?assertEqual({undefined, gb_trees:empty(), dict:new()}, new(dummytype)),
    ?assertEqual({antidote_crdt_register_lww, gb_trees:empty(), dict:new()}, new(antidote_crdt_register_lww)).

update_test() ->
    Index1 = new(antidote_crdt_register_lww),
    {ok, DownstreamOp} = downstream({update, {antidote_crdt_register_lww, key1, {assign, "col"}}}, Index1),
    ?assertMatch({update, {antidote_crdt_register_lww, key1, {_TS, "col"}}}, DownstreamOp),
    {ok, Index2} = update(DownstreamOp, Index1),
    Set = ordsets:add_element(key1, ordsets:new()),
    ?assertEqual([{"col", Set}], value(Index2)).

update2_test() ->
    Index1 = new(),
    {ok, DownstreamOp} = downstream({update, {antidote_crdt_set_aw, key1, {add, <<"elem">>}}}, Index1),
    {ok, Index2} = update(DownstreamOp, Index1),

    {ok, DownstreamOp2} = downstream({update, {antidote_crdt_counter_pn, key1, {increment, 5}}}, Index1),
    {ok, Index3} = update(DownstreamOp2, Index1),
    Set = ordsets:add_element(key1, ordsets:new()),

    ?assertEqual([{[<<"elem">>], Set}], value(Index2)),
    ?assertEqual([{5, Set}], value(Index3)).

update3_test() ->
    Index1 = new(),
    {ok, DownstreamOp} = downstream({update, {antidote_crdt_register_lww, key1, {assign, "col"}}}, Index1),
    {ok, Index2} = update(DownstreamOp, Index1),
    Response = downstream({update, {antidote_crdt_counter_pn, key2, {assign, 2}}}, Index2),
    ?assertEqual({error, wrong_type}, Response).

equal_test() ->
    Index1 = new(),
    {ok, DownstreamOp1} = downstream({update, {antidote_crdt_register_lww, key1, {assign, "col1"}}}, Index1),
    {ok, DownstreamOp2} = downstream({update, {antidote_crdt_register_lww, key1, {assign, "col2"}}}, Index1),
    {ok, DownstreamOp3} = downstream({update, {antidote_crdt_counter_pn, key1, {increment, 1}}}, Index1),
    {ok, Index2} = update(DownstreamOp1, Index1),
    {ok, Index3} = update(DownstreamOp2, Index1),
    {ok, Index4} = update(DownstreamOp3, Index1),
    ?assertEqual(true, equal(Index1, Index1)),
    ?assertEqual(true, equal(Index2, Index2)),
    ?assertEqual(false, equal(Index1, Index2)),
    ?assertEqual(false, equal(Index2, Index3)),
    ?assertEqual(false, equal(Index2, Index4)).

%%bound_search_test() ->
%%    Pred = {greatereq, 3},
%%    Tree1 = gb_trees:empty(),
%%    Tree2 = gb_trees:enter(1, 1, Tree1),
%%    Tree3 = lists:foldl(fun tree_insertions/2, Tree1, lists:seq(1, 10)),
%%    Tree4 = lists:foldl(fun tree_insertions/2, Tree1, lists:seq(2, 20, 2)),
%%    Tree5 = lists:foldl(fun tree_insertions/2, Tree1, lists:reverse(lists:seq(1, 5))),
%%    Tree6 = lists:foldl(fun tree_insertions/2, Tree1, [1, 2]),
%%
%%    ?assertEqual(nil, lookup_lower_bound(Pred, Tree1)),
%%    ?assertEqual(nil, lookup_lower_bound(Pred, Tree2)),
%%    ?assertEqual(3, lookup_lower_bound(Pred, Tree3)),
%%    ?assertEqual(4, lookup_lower_bound(Pred, Tree4)),
%%    ?assertEqual(3, lookup_lower_bound(Pred, Tree5)),
%%    ?assertEqual(nil, lookup_lower_bound(Pred, Tree6)).

range_test() ->
    Index1 = new(),
    Updates = [
        {antidote_crdt_register_lww, "col1", {assign, 1}}, {antidote_crdt_register_lww, "col2", {assign, 2}},
        {antidote_crdt_register_lww, "col3", {assign, 3}}, {antidote_crdt_register_lww, "col4", {assign, 4}},
        {antidote_crdt_register_lww, "col5", {assign, 5}}, {antidote_crdt_register_lww, "col6", {assign, 6}}
    ],
    {ok, DownstreamOp1} = downstream({update, Updates}, Index1),
    {ok, Index2} = update(DownstreamOp1, Index1),
    LowerPred1 = {greatereq, 3},
    UpperPred1 = {lesser, 6},
    ?assertEqual([], value({range, {LowerPred1, UpperPred1}}, Index1)),
    ?assertEqual([{3, ["col3"]}, {4, ["col4"]}, {5, ["col5"]}], value({range, {LowerPred1, UpperPred1}}, Index2)).

-endif.
