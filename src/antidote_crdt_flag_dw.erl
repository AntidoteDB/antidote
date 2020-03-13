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
%% An operation-based Disable-wins Flag CRDT.

%% @end
-module(antidote_crdt_flag_dw).

%% Callbacks
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

-behaviour(antidote_crdt).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(TAG, 77).
-define(V1_VERS, 1).

-type flag_dw() :: {antidote_crdt_flag_helper:tokens(), antidote_crdt_flag_helper:tokens()}.

%% SeenTokens, NewEnableTokens, NewDisableTokens
-type downstream_op() :: {antidote_crdt_flag_helper:tokens(), antidote_crdt_flag_helper:tokens(), antidote_crdt_flag_helper:tokens()}.

-spec new() -> flag_dw().
new() ->
  {[], []}.

-spec value(flag_dw()) -> boolean().
value({EnableTokens, DisableTokens}) ->
  DisableTokens == [] andalso EnableTokens =/= [].

-spec downstream(antidote_crdt_flag_helper:op(), flag_dw()) -> {ok, downstream_op()}.
downstream({disable, {}}, {EnableTokens, DisableTokens}) ->
  {ok, {EnableTokens ++ DisableTokens, [], [antidote_crdt_flag_helper:unique()]}};
downstream({enable, {}}, {EnableTokens, DisableTokens}) ->
  {ok, {EnableTokens ++ DisableTokens, [antidote_crdt_flag_helper:unique()], []}};
downstream({reset, {}}, {EnableTokens, DisableTokens}) ->
  {ok, {EnableTokens ++ DisableTokens, [], []}}.

-spec update(downstream_op(), flag_dw()) -> {ok, flag_dw()}.
  update({SeenTokens, NewEnableTokens, NewDisableTokens}, {CurrentEnableTokens, CurrentDisableTokens}) ->
    FinalEnableTokens = (CurrentEnableTokens ++ NewEnableTokens) -- SeenTokens,
    FinalDisableTokens = (CurrentDisableTokens ++ NewDisableTokens) -- SeenTokens,
    {ok, {FinalEnableTokens, FinalDisableTokens}}.

-spec equal(flag_dw(), flag_dw()) -> boolean().
  equal(Flag1, Flag2) ->
    Flag1 == Flag2.

-spec to_binary(flag_dw()) -> antidote_crdt_flag_helper:binary_flag().
  to_binary(Flag) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Flag))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    {ok, binary_to_term(Bin)}.

is_operation(A) -> antidote_crdt_flag_helper:is_operation(A).

is_bottom(Flag) ->
  Flag == new().

require_state_downstream(A) -> antidote_crdt_flag_helper:require_state_downstream(A).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
