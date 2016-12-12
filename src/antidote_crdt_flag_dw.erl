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

-export_type([flag_dw/0]).
-opaque flag_dw() :: antidote_crdt_flag:flag().

-spec new() -> flag_dw().
new() ->
  antidote_crdt_flag:new().

-spec value(flag_dw()) -> boolean().
value({_, B}) ->
  B == [].

-spec downstream(antidote_crdt_flag:op(), flag_dw()) -> {ok, antidote_crdt_flag:downstream_op()}.
  downstream(A, B) ->
    antidote_crdt_flag:downstream(A, B).

-spec update(antidote_crdt_flag:downstream_op(), flag_dw()) -> {ok, flag_dw()}.
  update(A, B) ->
    antidote_crdt_flag:update(A, B).

-spec equal(flag_dw(), flag_dw()) -> boolean().
  equal(A, B) ->
    antidote_crdt_flag:equal(A, B).

-spec to_binary(flag_dw()) -> antidote_crdt_flag:binary_flag().
  to_binary(Flag) ->
    antidote_crdt_flag:to_binary(Flag).

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    {ok, binary_to_term(Bin)}.

is_operation(A) -> antidote_crdt_flag:is_operation(A).

is_bottom(A) -> antidote_crdt_flag:is_bottom(A).

require_state_downstream(A) -> antidote_crdt_flag:require_state_downstream(A).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
