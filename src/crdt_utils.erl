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
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(crdt_utils).
-include("querying.hrl").

%% API
-export([to_insert_op/2,
         type_to_crdt/2,
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
