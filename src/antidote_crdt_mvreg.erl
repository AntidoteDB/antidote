%% -------------------------------------------------------------------
%%
%% riak_dt_mvreg: A DVVSet based multi value register
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc
%% A Multi-Value Register CRDT.
%% Read Returns a sorted list of all concurrently added values
%%
%% This is implemented, by assigning a unique token to every assign operation.
%% The downstream effect carries the tokens of overridden values, so that
%% only assignments, which happened before are really overridden and
%% concurrent assignments are maintained
%%
%% @end

-module(antidote_crdt_mvreg).

-behaviour(antidote_crdt).

%% Callbacks
-export([ new/0,
          value/1,
          downstream/2,
          update/2,
          equal/2,
          to_binary/1,
          from_binary/1,
          is_operation/1,
          require_state_downstream/1
        ]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([mvreg/0, mvreg_op/0]).

%% TODO: make opaque
-type mvreg() :: [{term(), uniqueToken()}].
-type uniqueToken() :: term().
-type mvreg_effect() ::
    {Value::term(), uniqueToken(), Overridden::[uniqueToken()]}
  | {reset, Overridden::[uniqueToken()]}.


-type mvreg_op() :: {assign, term()}.


%% @doc Create a new, empty `mvreg()'
-spec new() -> mvreg().
new() ->
    [].



%% @doc The values of this `mvreg()'. Multiple values can be returned,
%% since there can be diverged value in this register.
-spec value(mvreg()) -> [term()].
value(MVReg) ->
    [V || {V,_} <- MVReg].


-spec downstream(mvreg_op(), mvreg()) -> {ok, mvreg_effect()}.
downstream({assign, Value}, MVReg) ->
    Token = unique(),
    Overridden = [Tok || {_, Tok} <- MVReg],
    {ok, {Value, Token, Overridden}};
downstream({reset, {}}, MVReg) ->
  Overridden = [Tok || {_, Tok} <- MVReg],
  {ok, {reset, Overridden}}.

-spec unique() -> uniqueToken().
unique() ->
    crypto:strong_rand_bytes(20).


-spec update(mvreg_effect(), mvreg()) -> {ok, mvreg()}.
update({Value, Token, Overridden}, MVreg) ->
    % remove overridden values
    MVreg2 = [{V,T} || {V,T} <- MVreg, not lists:member(T, Overridden)],
    % insert new value
    {ok, insertSorted({Value, Token}, MVreg2)};
update({reset, Overridden}, MVreg) ->
  MVreg2 = [{V,T} || {V,T} <- MVreg, not lists:member(T, Overridden)],
  {ok, MVreg2}.

% insert value into sorted list
insertSorted(A, []) -> [A];
insertSorted(A, [X|Xs]) when A < X -> [A,X|Xs];
insertSorted(A, [X|Xs]) -> [X|insertSorted(A,Xs)].


-spec equal(mvreg(), mvreg()) -> boolean().
equal(MVReg1, MVReg2) ->
    MVReg1 == MVReg2.

-define(TAG, 85).
-define(V1_VERS, 1).

-spec to_binary(mvreg()) -> binary().
to_binary(MVReg) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(MVReg))/binary>>.

%% @doc Decode binary `mvreg()'
-spec from_binary(binary()) -> {ok, mvreg()} | {error, term()}.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    {ok, riak_dt:from_binary(Bin)}.


%% @doc The following operation verifies
%%      that Operation is supported by this particular CRDT.
-spec is_operation(term()) -> boolean().
is_operation({assign, _}) -> true;
is_operation({reset, {}}) -> true;
is_operation(_) -> false.

require_state_downstream(_) ->
     true.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).


-endif.
