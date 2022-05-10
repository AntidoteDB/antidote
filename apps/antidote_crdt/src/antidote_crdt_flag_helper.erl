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
%% A helper for operation-based flags, enable wins flag and disable wins flag.

%% @end
-module(antidote_crdt_flag_helper).


%% Callbacks
-export([ from_binary/1,
          is_operation/1,
          require_state_downstream/1,
          unique/0
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([tokens/0, binary_flag/0, op/0]).

-type binary_flag() :: binary(). %% A binary that from_binary/1 will operate on.

-type op() ::
      {enable, {}}
    | {disable, {}}
    | {reset, {}}.

-type token() :: term().
-type tokens() :: [token()].

%% @doc generate a unique identifier (best-effort).
unique() ->
    crypto:strong_rand_bytes(20).

-define(TAG, 77).
-define(V1_VERS, 1).

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    {ok, binary_to_term(Bin)}.

is_operation({enable, {}}) -> true;
is_operation({disable, {}}) -> true;
is_operation({reset, {}}) -> true;
is_operation(_) -> false.

require_state_downstream(_) -> true.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
