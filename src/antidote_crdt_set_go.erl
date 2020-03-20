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

%% @doc module antidote_crdt_set_go - An operation based grow-only set

-module(antidote_crdt_set_go).

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
          require_state_downstream/1
        ]).

-type antidote_crdt_set_go() :: ordsets:ordset(member()).
-type antidote_crdt_set_go_op() :: {add, member()}
                 | {add_all, [member()]}.

-type antidote_crdt_set_go_effect() :: antidote_crdt_set_go().
-type member() :: term().

new() ->
    ordsets:new().

value(Set) ->
    Set.

-spec downstream(antidote_crdt_set_go_op(), antidote_crdt_set_go()) -> {ok, antidote_crdt_set_go_effect()}.
downstream({add, Elem}, _State) ->
    {ok, ordsets:from_list([Elem])};
downstream({add_all, Elems}, _State) ->
    {ok, ordsets:from_list(Elems)}.

update(Effect, State) ->
    {ok, ordsets:union(State, Effect)}.

require_state_downstream(_Operation) -> false.

is_operation({add, _}) -> true;
is_operation({add_all, _}) -> true;
is_operation(_) -> false.

equal(CRDT1, CRDT2) ->
    CRDT1 == CRDT2.

to_binary(CRDT) ->
    erlang:term_to_binary(CRDT).

from_binary(Bin) ->
    {ok, erlang:binary_to_term(Bin)}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(test).

all_test() ->
    S0 = new(),
    {ok, Downstream} = downstream({add, a}, S0),
    {ok, S1} = update(Downstream, S0),
    ?assertEqual(1, antidote_crdt_set_go:stat(element_count, S1)).

-endif.
