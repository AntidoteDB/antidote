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

%% antidote_crdt_big_counter: A convergent, replicated, operation based Big Counter

-module(antidote_crdt_big_counter).

-behaviour(antidote_crdt).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([ new/0,
          value/1,
          downstream/2,
          update/2,
          equal/2,
          to_binary/1,
          from_binary/1,
          is_operation/1,
          is_bottom/1,
          require_state_downstream/1
        ]).

-type uniqueToken() :: term().
-type state() :: [{integer(), uniqueToken()}].
-type op() ::
    {increment, integer()}
    | {decrement, integer()}
    | reset.
-type effect() ::
      {integer(), uniqueToken()}
      | {{reset, {}}, Overridden::[uniqueToken()]}.

%% @doc Create a new, empty big counter
-spec new() -> state().
new() ->
    [{0, unique()}].

%% @doc The value of this counter is equal to the sum of all the values
%% having tokens.
-spec value(state()) -> integer().
value(BigCounter) ->
    lists:sum([V || {V, _} <- BigCounter]).


-spec downstream(op(), state()) -> {ok, effect()}.
downstream(Op, BigCtr) ->
    Token = unique(),
    case Op of
        {increment, Value} when is_integer(Value) ->
            {ok, {Value, Token}};
        {decrement, Value} when is_integer(Value) ->
            {ok, {-Value, Token}};
        {reset, {}} ->
            Overridden = [Tok || {_, Tok} <- BigCtr],
            {ok, Overridden}
    end.

-spec unique() -> uniqueToken().
unique() ->
    crypto:strong_rand_bytes(20).


-spec update(effect(), state()) -> {ok, state()}.
update({Value, Token}, BigCtr) ->
    % insert new value
    {ok, insert_sorted({Value, Token}, BigCtr)};
update(Overridden, BigCtr) ->
  BigCtr2 = [{V, T} || {V, T} <- BigCtr, not lists:member(T, Overridden)],
  case BigCtr2 == [] of
    true ->
        {ok, new()};
    false ->
        {ok, BigCtr2}
  end.

% insert value into sorted list
insert_sorted(A, []) -> [A];
insert_sorted(A, [X|Xs]) when A < X -> [A, X|Xs];
insert_sorted(A, [X|Xs]) -> [X|insert_sorted(A, Xs)].


-spec equal(state(), state()) -> boolean().
equal(BigCtr1, BigCtr2) ->
    BigCtr1 == BigCtr2.

-define(TAG, 85).
-define(V1_VERS, 1).

-spec to_binary(state()) -> binary().
to_binary(BigCtr) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(BigCtr))/binary>>.

%% @doc Decode binary
-spec from_binary(binary()) -> {ok, state()} | {error, term()}.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    {ok, riak_dt:from_binary(Bin)}.

is_bottom(BigCtr) ->
  BigCtr == new().

%% @doc The following operation verifies
%%      that Operation is supported by this particular CRDT.
-spec is_operation(term()) -> boolean().
is_operation({increment, Value}) when is_integer(Value) -> true;
is_operation({decrement, Value}) when is_integer(Value)-> true;
is_operation({reset, {}}) -> true;
is_operation(is_bottom) -> true;
is_operation(_) -> false.

require_state_downstream(_) ->
     true.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).


-endif.
