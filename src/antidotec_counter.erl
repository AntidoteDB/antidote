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
-module(antidotec_counter).

-include_lib("antidote_pb_codec/include/antidote_pb.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-behaviour(antidotec_datatype).

-export([new/0, new/1,
         value/1,
         to_ops/2,
         is_type/1,
         dirty_value/1,
         type/0
        ]).

-export([increment/1,
         increment/2,
         decrement/1,
         decrement/2
        ]).

-record(counter, {
          value :: integer(),
          increment :: integer()
         }).

-export_type([antidotec_counter/0]).
-opaque antidotec_counter() :: #counter{}.

-spec new() -> antidotec_counter().
new() ->
    #counter{value = 0, increment = 0}.

-spec new(integer()) -> antidotec_counter().
new(Value) ->
    #counter{value = Value, increment = 0}.

-spec value(antidotec_counter()) -> integer().
value(#counter{value = Value}) ->
    Value.

-spec dirty_value(antidotec_counter()) -> integer().
dirty_value(#counter{value = Value, increment = Increment}) ->
    Value + Increment.

%% @doc Increments the counter with 1 unit.
-spec increment(antidotec_counter()) -> antidotec_counter().
increment(Counter) ->
    increment(1, Counter).

%% @doc Increments the counter with Amount units.
-spec increment(integer(), antidotec_counter()) -> antidotec_counter().
increment(Amount, #counter{increment = Increment} = Counter)
  when is_integer(Amount) ->
    Counter#counter{increment = Increment + Amount}.

%% @doc Decrements the counter by 1.
-spec decrement(antidotec_counter()) -> antidotec_counter().
decrement(Counter) ->
    increment(-1, Counter).

%% @doc Decrements the counter by the passed amount.
-spec decrement(integer(), antidotec_counter()) -> antidotec_counter().
decrement(Amount, #counter{increment = Value} = Counter)
  when is_integer(Amount) ->
    Counter#counter{increment = Value - Amount}.

-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, counter).

-spec type() -> counter.
type() ->
     counter.

to_ops(_BoundObject, #counter{increment = 0}) -> [];

to_ops(BoundObject, #counter{increment = Amount}) when Amount < 0 ->
    [{BoundObject, decrement, -Amount}];

to_ops(BoundObject, #counter{increment = Amount}) ->
    [{BoundObject, increment, Amount}].


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(COMPARE_VALUES(X, Y), ?assertEqual(X, dirty_value(Y))).

incr_op_test() ->
    New = new(),
    ?COMPARE_VALUES(0, New),
    ?COMPARE_VALUES(1, increment(New)),
    ?COMPARE_VALUES(7, increment(7, New)),
    ?COMPARE_VALUES(-3, increment(-3, New)),

    ?COMPARE_VALUES(-1, decrement(New)),
    ?COMPARE_VALUES(-5, decrement(5, New)).
-endif.