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
%%% @doc An Antidote module that supports some utility functions for
%%%      CRDTs.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(crdt_utils).
-include("querying.hrl").

-define(CRDT_INDEX, antidote_crdt_index).
-define(CRDT_INDEX_P, antidote_crdt_index_p).
-define(CRDT_MAP, antidote_crdt_map_go).
-define(CRDT_SET, antidote_crdt_set_aw).

-define(INVALID_OP_MSG(Operation, CRDT), io_lib:format("The operation ~p is not part of the ~p specification", [Operation, CRDT])).

%% API
-export([to_insert_op/2,
    type_to_crdt/2,
    create_crdt_update/3,
    convert_value/2]).

to_insert_op(?CRDT_VARCHAR, Value) -> {assign, Value};
to_insert_op(?CRDT_BOOLEAN, Value) ->
    case Value of
        true -> {enable, {}};
        false -> {disable, {}}
    end;
to_insert_op(?CRDT_BCOUNTER_INT, Value) when is_tuple(Value) ->
    {Inc, Dec} = Value,
    IncList = orddict:to_list(Inc),
    DecList = orddict:to_list(Dec),
    SumInc = sum_values(IncList),
    SumDec = sum_values(DecList),
    IncUpdate = increment_bcounter(SumInc),
    DecUpdate = decrement_bcounter(SumDec),
    lists:flatten([IncUpdate, DecUpdate]);
to_insert_op(?CRDT_COUNTER_INT, Value) ->
    increment_counter(Value);
to_insert_op(_, _) -> {error, invalid_crdt}.

type_to_crdt(?AQL_INTEGER, _) -> ?CRDT_INTEGER;
type_to_crdt(?AQL_BOOLEAN, _) -> ?CRDT_BOOLEAN;
type_to_crdt(?AQL_COUNTER_INT, {_, _}) -> ?CRDT_BCOUNTER_INT;
type_to_crdt(?AQL_COUNTER_INT, _) -> ?CRDT_COUNTER_INT;
type_to_crdt(?AQL_VARCHAR, _) -> ?CRDT_VARCHAR.

create_crdt_update({_Key, ?CRDT_MAP, _Bucket} = ObjKey, UpdateOp, Value) ->
    Update = map_update(Value),
    {ObjKey, UpdateOp, Update};
create_crdt_update({_Key, ?CRDT_INDEX, _Bucket} = ObjKey, UpdateOp, Value) ->
    Update = index_update(UpdateOp, Value),
    {ObjKey, UpdateOp, Update};
create_crdt_update({_Key, ?CRDT_INDEX_P, _Bucket} = ObjKey, UpdateOp, Value) ->
    Update = index_p_update(UpdateOp, Value),
    {ObjKey, UpdateOp, Update};
create_crdt_update(ObjKey, UpdateOp, Value) ->
    set_update(ObjKey, UpdateOp, Value).

convert_value(?CRDT_BCOUNTER_INT, {Inc, Dec}) ->
    IncList = orddict:to_list(Inc),
    DecList = orddict:to_list(Dec),
    SumInc = sum_values(IncList),
    SumDec = sum_values(DecList),
    SumInc - SumDec;
convert_value(_, Value) ->
    Value.

%% ====================================================================
%% Internal functions
%% ====================================================================

increment_counter(0) -> [];
increment_counter(Value) when is_integer(Value) ->
    {increment, Value}.

%decrement_counter(0) -> [];
%decrement_counter(Value) when is_integer(Value) ->
%    {decrement, Value}.

increment_bcounter(0) -> [];
increment_bcounter(Value) when is_integer(Value) ->
    bcounter_op(increment, Value).

decrement_bcounter(0) -> [];
decrement_bcounter(Value) when is_integer(Value) ->
    bcounter_op(decrement, Value).

bcounter_op(Op, Value) ->
    {Op, {Value, term}}.

sum_values(List) when is_list(List) ->
    lists:sum([Value || {_Ids, Value} <- List]).

map_update({{Key, CRDT}, {Op, Value} = Operation}) ->
    case CRDT:is_operation(Operation) of
        true -> [{{Key, CRDT}, {Op, Value}}];
        false -> throw(lists:flatten(?INVALID_OP_MSG(Operation, CRDT)))
    end;
map_update(Values) when is_list(Values) ->
    lists:foldl(fun(Update, Acc) ->
        lists:append(Acc, map_update(Update))
                end, [], Values).

index_update(UpdateOp, {_CRDT, _Key, Operations} = Update) when is_list(Operations) ->
    case ?CRDT_INDEX:is_operation({UpdateOp, Update}) of
        true -> [Update];
        false -> throw(lists:flatten(?INVALID_OP_MSG(Update, ?CRDT_INDEX_P)))
    end;
index_update(UpdateOp, Values) when is_list(Values) ->
    lists:foldl(fun(Update, Acc) ->
        lists:append(Acc, index_update(UpdateOp, Update))
                end, [], Values).

index_p_update(UpdateOp, {_Key, {_Op, _Value}} = Operation) ->
    case ?CRDT_INDEX_P:is_operation({UpdateOp, Operation}) of
        true -> [Operation];
        false -> throw(lists:flatten(?INVALID_OP_MSG(Operation, ?CRDT_INDEX_P)))
    end;
index_p_update(UpdateOp, {Key, Operations}) when is_list(Operations) ->
    lists:foldl(fun(Op, Acc) ->
        lists:append(Acc, index_p_update(UpdateOp, {Key, Op}))
                end, [], Operations);
index_p_update(UpdateOp, Values) when is_list(Values) ->
    lists:foldl(fun(Update, Acc) ->
        lists:append(Acc, index_p_update(UpdateOp, Update))
                end, [], Values).

set_update({_Key, ?CRDT_SET, _Bucket} = ObjKey, UpdateOp, Value) ->
    case ?CRDT_SET:is_operation(UpdateOp) of
        true -> {ObjKey, UpdateOp, Value};
        false -> throw(lists:flatten(?INVALID_OP_MSG(UpdateOp, ?CRDT_SET)))
    end.
