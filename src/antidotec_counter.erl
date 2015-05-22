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

-include_lib("riak_pb/include/antidote_pb.hrl").

-behaviour(antidotec_datatype).

-export([new/1,
         new/2,
         message_for_get/1,
         value/1,
         to_ops/1,
         is_type/1,
         type/0,
         dirty_value/1
        ]).

-export([increment/1,
         increment/2,
         decrement/1,
         decrement/2
        ]).

-record(counter, {
          key :: term(),
          value :: integer(),
          increment :: integer()
         }).

-export_type([antidote_counter/0]).
-opaque antidote_counter() :: #counter{}.

-spec new(term()) -> antidote_counter().
new(Key) ->
    #counter{key=Key, value=0, increment=0}.

-spec new(term(), integer()) -> antidote_counter().
new(Key, Value) ->
    #counter{key=Key, value=Value, increment=0}.

-spec value(antidote_counter()) -> integer().
value(#counter{value=Value}) ->
    Value.

-spec dirty_value(antidote_counter()) -> integer().
dirty_value(#counter{value=Value, increment=Increment}) ->
    Value + Increment.

%% @doc Increments the counter with 1 unit.
-spec increment(antidote_counter()) -> antidote_counter().
increment(Counter) ->
    increment(1, Counter).

%% @doc Increments the counter with Amount units.
-spec increment(integer(), antidote_counter()) -> antidote_counter().
increment(Amount, #counter{increment=Value}=Counter) when is_integer(Amount) ->
    Counter#counter{increment=Value+Amount}.

%% @doc Decrements the counter by 1.
-spec decrement(antidote_counter()) -> antidote_counter().
decrement(Counter) ->
    increment(-1, Counter).

%% @doc Decrements the counter by the passed amount.
-spec decrement(integer(), antidote_counter()) -> antidote_counter().
decrement(Amount, #counter{increment=Value}=Counter) ->
    Counter#counter{increment=Value-Amount}.

-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, counter).

-spec type() -> riak_dt_pncounter.
type() -> riak_dt_pncounter.

to_ops(#counter{key=_Key, increment=0}) -> undefined;

to_ops(#counter{key=Key, increment=Amount}) when Amount < 0 ->
    [#fpbdecrementreq{key=Key, amount=-Amount}];

to_ops(#counter{key=Key, increment=Amount}) ->
    [#fpbincrementreq{key=Key, amount=Amount}].

message_for_get(Key) -> #fpbgetcounterreq{key=Key}.

