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
%% @doc module antidote_crdt_gindex - A grow-only index
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

-module(antidote_crdt_gindex).
-behaviour(antidote_crdt).

%% API
-export([new/0,
         new/1,
         value/1,
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
-type indexmap() :: orddict:orddict(Key::term(), NestedState::term()).
-type indirectionmap() :: dict:dict({Key::term(), Type::atom()}, NestedState::term()).

-type gindex_query() :: {range, gindex()} | gindex().

-type gindex_op() :: {update, nested_op()} | {update, [nested_op()]}.
-type nested_op() :: {{Key::term(), Type::atom()}, Op::term()}.
-type gindex_effect() :: {update, nested_downstream()} | {update, [nested_downstream()]}.
-type nested_downstream() :: {{Key::term(), Type::atom()}, Op::term()}.

-type invalid_type() :: {error, wrong_type}.

-spec new() -> gindex().
new() ->
    {undefined, orddict:new(), dict:new()}.

-spec new(term()) -> gindex().
new(Type) ->
    case antidote_crdt:is_type(Type) of
        true -> {Type, orddict:new(), dict:new()};
        false -> new()
    end.

-spec value(gindex_query()) -> indexmap().
value({range, LowerPred, UpperPred, {_Type, Index, _Indirection}}) ->
    orddict:filter(fun(Key, _Value) ->
        LowerPred(Key) andalso UpperPred(Key)
    end, Index);
value({get, Key, {_Type, Index, _Indirection}}) ->
    case orddict:find(Key, Index) of
        {ok, Value} -> orddict:store(Key, Value, orddict:new());
        error -> {error, key_not_found}
    end;
value({lookup, Key, {Type, _Index, Indirection} = GIndex}) ->
    case dict:find(Key, Indirection) of
        {ok, Value} ->
            CRDTValue = Type:value(Value),
            value({get, CRDTValue, GIndex});
        error -> {error, key_not_found}
    end;
value({_Type, Index, _Indirection}) ->
    Index.

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
downstream({update, Ops}, CurrentMap) when is_list(Ops) ->
    {ok, {update, lists:map(fun(Op) -> {ok, DSOp} = downstream({update, Op}, CurrentMap), DSOp end, Ops)}}.

-spec update(gindex_effect(), gindex()) -> {ok, gindex()}.
update({update, {Type, Key, Op}}, {_Type, Index, Indirection}) ->
    NewIndirection = case dict:is_key(Key, Indirection) of
                         true ->
                             dict:update(Key, fun(V) -> {ok, Value} = Type:update(Op, V), Value end, Indirection);
                         false ->
                             {ok, NewValueUpdated} = Type:update(Op, Type:new()),
                             dict:store(Key, NewValueUpdated, Indirection)
    end,
    NewIndex = update_index(get_value(Type, Key, NewIndirection), Key, Index),
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

update_index(EntryKey, EntryValue, Index) ->
    RemoveOld = orddict:fold(fun(Key, Value, Acc) ->
        case ordsets:is_element(EntryValue, Value) of
            true -> orddict:store(Key, ordsets:del_element(EntryValue, Value), Acc);
            false -> orddict:store(Key, Value, Acc)
        end
    end, orddict:new(), Index),
    orddict:update(EntryKey, fun(Set) ->
        ordsets:add_element(EntryValue, Set)
    end, ordsets:add_element(EntryValue, ordsets:new()), RemoveOld).

get_value(Type, Key, Indirection) ->
    CRDTValue = dict:fetch(Key, Indirection),
    Value = Type:value(CRDTValue),
    calc_value(Type, Value).

calc_value(antidote_crdt_bcounter, {Inc, Dec}) ->
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

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
new_test() ->
    ?assertEqual({undefined, orddict:new(), dict:new()}, new()),
    ?assertEqual({undefined, orddict:new(), dict:new()}, new(dummytype)),
    ?assertEqual({antidote_crdt_lwwreg, orddict:new(), dict:new()}, new(antidote_crdt_lwwreg)).

update_test() ->
    Index1 = new(antidote_crdt_lwwreg),
    {ok, DownstreamOp} = downstream({update, {antidote_crdt_lwwreg, key1, {assign, "col"}}}, Index1),
    ?assertMatch({update, {antidote_crdt_lwwreg, key1, {_TS, "col"}}}, DownstreamOp),
    {ok, Index2} = update(DownstreamOp, Index1),
    Set = ordsets:add_element(key1, ordsets:new()),
    ?assertEqual([{"col", Set}], value(Index2)).

update2_test() ->
    Index1 = new(),
    {ok, DownstreamOp} = downstream({update, {antidote_crdt_orset, key1, {add, <<"elem">>}}}, Index1),
    {ok, Index2} = update(DownstreamOp, Index1),

    {ok, DownstreamOp2} = downstream({update, {antidote_crdt_integer, key1, {increment, 5}}}, Index1),
    {ok, Index3} = update(DownstreamOp2, Index1),
    Set = ordsets:add_element(key1, ordsets:new()),

    ?assertEqual([{[<<"elem">>], Set}], value(Index2)),
    ?assertEqual([{5, Set}], value(Index3)).

update3_test() ->
    Index1 = new(),
    {ok, DownstreamOp} = downstream({update, {antidote_crdt_lwwreg, key1, {assign, "col"}}}, Index1),
    {ok, Index2} = update(DownstreamOp, Index1),
    Response = downstream({update, {antidote_crdt_integer, key2, {assign, 2}}}, Index2),
    ?assertEqual({error, wrong_type}, Response).

equal_test() ->
    Index1 = new(),
    {ok, DownstreamOp1} = downstream({update, {antidote_crdt_lwwreg, key1, {assign, "col1"}}}, Index1),
    {ok, DownstreamOp2} = downstream({update, {antidote_crdt_lwwreg, key1, {assign, "col2"}}}, Index1),
    {ok, DownstreamOp3} = downstream({update, {antidote_crdt_integer, key1, {increment, 1}}}, Index1),
    {ok, Index2} = update(DownstreamOp1, Index1),
    {ok, Index3} = update(DownstreamOp2, Index1),
    {ok, Index4} = update(DownstreamOp3, Index1),
    ?assertEqual(true, equal(Index1, Index1)),
    ?assertEqual(true, equal(Index2, Index2)),
    ?assertEqual(false, equal(Index1, Index2)),
    ?assertEqual(false, equal(Index2, Index3)),
    ?assertEqual(false, equal(Index2, Index4)).

-endif.
