%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
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

-module(antidote_crdt_register_mv).

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
          require_state_downstream/1,
          is_bottom/1
        ]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type antidote_crdt_register_mv() :: [{term(), uniqueToken()}].
-type uniqueToken() :: term().
-type antidote_crdt_register_mv_effect() ::
    {Value::term(), uniqueToken(), Overridden::[uniqueToken()]}
  | {reset, Overridden::[uniqueToken()]}.


-type antidote_crdt_register_mv_op() :: {assign, term()}.


%% @doc Create a new, empty `antidote_crdt_register_mv()'
-spec new() -> antidote_crdt_register_mv().
new() ->
    [].



%% @doc The values of this `antidote_crdt_register_mv()'. Multiple values can be returned,
%% since there can be diverged value in this register.
-spec value(antidote_crdt_register_mv()) -> [term()].
value(MVReg) ->
    [V || {V, _} <- MVReg].


-spec downstream(antidote_crdt_register_mv_op(), antidote_crdt_register_mv()) -> {ok, antidote_crdt_register_mv_effect()}.
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


-spec update(antidote_crdt_register_mv_effect(), antidote_crdt_register_mv()) -> {ok, antidote_crdt_register_mv()}.
update({Value, Token, Overridden}, MVreg) ->
    % remove overridden values
    MVreg2 = [{V, T} || {V, T} <- MVreg, not lists:member(T, Overridden)],
    % insert new value
    {ok, insert_sorted({Value, Token}, MVreg2)};
update({reset, Overridden}, MVreg) ->
  MVreg2 = [{V, T} || {V, T} <- MVreg, not lists:member(T, Overridden)],
  {ok, MVreg2}.

% insert value into sorted list
insert_sorted(A, []) -> [A];
insert_sorted(A, [X|Xs]) when A < X -> [A, X|Xs];
insert_sorted(A, [X|Xs]) -> [X|insert_sorted(A, Xs)].


-spec equal(antidote_crdt_register_mv(), antidote_crdt_register_mv()) -> boolean().
equal(MVReg1, MVReg2) ->
    MVReg1 == MVReg2.

-define(TAG, 85).
-define(V1_VERS, 1).

-spec to_binary(antidote_crdt_register_mv()) -> binary().
to_binary(MVReg) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(MVReg))/binary>>.

%% @doc Decode binary `antidote_crdt_register_mv()'
-spec from_binary(binary()) -> {ok, antidote_crdt_register_mv()} | {error, term()}.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    {ok, antidote_crdt:from_binary(Bin)}.


%% @doc The following operation verifies
%%      that Operation is supported by this particular CRDT.
-spec is_operation(term()) -> boolean().
is_operation({assign, _}) -> true;
is_operation({reset, {}}) -> true;
is_operation(_) -> false.

require_state_downstream(_) ->
     true.

is_bottom(State) -> State == new().


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

upd(Update, State) ->
    {ok, Downstream} = downstream(Update, State),
    {ok, Res} = update(Downstream, State),
    Res.

reset_test() ->
    R1 = new(),
    ?assertEqual([], value(R1)),
    ?assertEqual(true, is_bottom(R1)),
    R2 = upd({assign, <<"a">>}, R1),
    ?assertEqual([<<"a">>], value(R2)),
    ?assertEqual(false, is_bottom(R2)),
    R3 = upd({reset, {}}, R2),
    ?assertEqual([], value(R3)),
    ?assertEqual(true, is_bottom(R3)).


-endif.
