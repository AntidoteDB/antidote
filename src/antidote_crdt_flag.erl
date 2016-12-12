%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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
%% A wrapper for operation-based flags, enable wins flag and disable wins flag.

%% @end
-module(antidote_crdt_flag).


%% Callbacks
-export([ new/0,
          downstream/2,
          update/2,
          equal/2,
          to_binary/1,
          from_binary/1,
          is_operation/1,
          is_bottom/1,
          require_state_downstream/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([flag/0, binary_flag/0, op/0]).
-opaque flag() :: {tokens(), tokens()}.

-type binary_flag() :: binary(). %% A binary that from_binary/1 will operate on.

-type op() ::
      {enable, {}}
    | {disable, {}}
    | {reset, {}}.

%% CurrentTokens, EnableTokens, DisableTokens
-type downstream_op() :: {tokens(), tokens(), tokens()}.

-type token() :: term().
-type tokens() :: [token()].

-spec new() -> flag().
new() ->
  {[], []}.

-spec downstream(op(), flag()) -> {ok, downstream_op()}.
downstream({enable, {}}, {EnableTokens, DisableTokens}) ->
  {ok, {EnableTokens ++ DisableTokens, [unique()], []}};
downstream({disable, {}}, {EnableTokens, DisableTokens}) ->
  {ok, {EnableTokens ++ DisableTokens, [], [unique()]}};
downstream({reset, {}}, {EnableTokens, DisableTokens}) ->
  {ok, {EnableTokens ++ DisableTokens, [], []}}.

%% @doc generate a unique identifier (best-effort).
unique() ->
    crypto:strong_rand_bytes(20).

-spec update(downstream_op(), flag()) -> {ok, flag()}.
  update({SeenTokens, ToEnable, ToDisable}, {CurrentEnableTokens, CurrentDisableTokens}) ->
    EnableTokens = (CurrentEnableTokens ++ ToEnable) -- SeenTokens,
    DisableTokens = (CurrentDisableTokens ++ ToDisable) -- SeenTokens,
    {ok, {EnableTokens, DisableTokens}}.

-spec equal(flag(), flag()) -> boolean().
  equal(Flag1, Flag2) ->
    Flag1 == Flag2. % Everything inside is ordered, so this should work

-define(TAG, 77).
-define(V1_VERS, 1).

-spec to_binary(flag()) -> binary_flag().
to_binary(Flag) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Flag))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    {ok, binary_to_term(Bin)}.

is_operation({enable, {}}) -> true;
is_operation({disable, {}}) -> true;
is_operation({reset, {}}) -> true;
is_operation(_) -> false.

is_bottom(Flag) ->
  Flag == new().

require_state_downstream(_) -> true.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
