%% -------------------------------------------------------------------
%%
%% crdt_set: A convergent, replicated, operation based observe remove set
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
%% An operation-based Remove-wins Set CRDT.
%%
%% @end
-module(antidote_crdt_set_rw).


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


-behaviour(antidote_crdt).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([set/0, binary_set/0, set_op/0]).
-opaque set() :: {ResetTokens::[token()], orddict:orddict(term(), [token()])}.

-type binary_set() :: binary(). %% A binary that from_binary/1 will operate on.

-type set_op() ::
      {add, member()}
    | {remove, member()}
    | {add_all, [member()]}
    | {remove_all, [member()]}
    | reset.

-type token() :: term().
-type effect() ::
      {add, [{member(), RemTokens::[token()]}], ResetTokens::[token()]}
    | {remove, [{member(), RemTokens::[token()]}], NewRemToken::token(), ResetTokens::[token()]}
    | {reset, [{member(), RemTokens::[token()]}], OldResetTokens::[token()], NewResetToken::token()}.

-type member() :: term().

-spec new() -> set().
new() ->
  {[], orddict:new()}.

-spec value(set()) -> [member()].
value({_,Dict}) ->
    [Val || {Val, []} <- orddict:to_list(Dict)].


-spec downstream(set_op(), set()) -> {ok, effect()}.
downstream({add, Elem}, State) ->
    downstream({add_all, [Elem]}, State);
downstream({add_all,Elems}, {ResetTokens, Dict}) ->
    {ok, {add, elemsWithRemovedTombstones(lists:usort(Elems), Dict, ResetTokens), ResetTokens}};
downstream({remove, Elem}, State) ->
    downstream({remove_all, [Elem]}, State);
downstream({remove_all, Elems}, {ResetTokens, Dict}) ->
    {ok, {remove, elemsWithRemovedTombstones(lists:usort(Elems), Dict, ResetTokens), unique(), ResetTokens}};
downstream(reset, {ResetTokens, Dict}) ->
  RemovedTombstones = elemsWithRemovedTombstones(lists:usort(orddict:fetch_keys(Dict)), Dict, ResetTokens),
  {ok, {reset, RemovedTombstones, ResetTokens, unique()}}.

%% @doc generate a unique identifier (best-effort).
unique() ->
    crypto:strong_rand_bytes(20).

elemsWithRemovedTombstones([], _Dict, _ResetTokens) -> [];
elemsWithRemovedTombstones(Elems, [], ResetTokens) -> [{E,ResetTokens} || E <- Elems];
elemsWithRemovedTombstones([E|ElemsRest]=Elems, [{K,Ts}|DictRest]=Dict, ResetTokens) ->
    if
        E == K ->
            [{E,Ts++ResetTokens}|elemsWithRemovedTombstones(ElemsRest, DictRest, ResetTokens)];
        E > K ->
            elemsWithRemovedTombstones(Elems, DictRest, ResetTokens);
        true ->
            [{E,ResetTokens}|elemsWithRemovedTombstones(ElemsRest, Dict, ResetTokens)]
    end.


-spec update(effect(), set()) -> {ok, set()}.
update({add, Elems, ObservedResetTokens}, {ResetTokens, Dict}) ->
  NewResetTokens = ResetTokens -- ObservedResetTokens,
  {ok, {ResetTokens, addElems(Elems, Dict, [], NewResetTokens)}};
update({remove,Elems,Token, ObservedResetTokens}, {ResetTokens, Dict}) ->
  NewResetTokens = ResetTokens -- ObservedResetTokens,
  {ok, {ResetTokens, addElems(Elems, Dict, [Token], NewResetTokens)}};
update({reset, Elems, OldResetTokens, NewResetToken}, {ResetTokens, Dict}) ->
  Dict2 = addElems(Elems, Dict, [], []),
  Dict3 = [{X, [NewResetToken|Tokens--OldResetTokens]} || {X,Tokens} <- Dict2],
  {ok, {[NewResetToken] ++ (ResetTokens -- OldResetTokens), Dict3}}.


addElems([], Dict, _, _) -> Dict;
addElems(Elems, [], NewTombs, NewEntryTombs) -> [{E, NewTombs++NewEntryTombs} || {E,_} <- Elems];
addElems([{E,RemovedTombstones}|ElemsRest]=Elems, [{K,Ts}|DictRest]=Dict, NewTombs, NewEntryTombs) ->
    if
        E == K ->
            [{E, NewTombs ++ (Ts -- RemovedTombstones)}|addElems(ElemsRest, DictRest, NewTombs, NewEntryTombs)];
        E > K ->
            [{K,Ts}|addElems(Elems, DictRest, NewTombs, NewEntryTombs)];
        true ->
            [{E, NewTombs++NewEntryTombs}|addElems(ElemsRest, Dict, NewTombs, NewEntryTombs)]
    end.


        -spec equal(set(), set()) -> boolean().
equal(ORDictA, ORDictB) ->
    ORDictA == ORDictB. % Everything inside is ordered, so this should work


-define(TAG, 77).
-define(V1_VERS, 1).

-spec to_binary(set()) -> binary_set().
to_binary(Set) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Set))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    {ok, binary_to_term(Bin)}.





is_operation({add, _Elem}) -> true;
is_operation({add_all, L}) when is_list(L) -> true;
is_operation({remove, _Elem}) ->
    true;
is_operation({remove_all, L}) when is_list(L) -> true;
is_operation(reset) -> true;
is_operation(_) -> false.

require_state_downstream(_) -> true.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

reset1_test() ->
  Set0 = new(),
  % DC1 reset
  {ok, ResetEffect} = downstream(reset, Set0),
  {ok, Set1a} = update(ResetEffect, Set0),
  % DC1 add
  {ok, Add1Effect} = downstream({add, a}, Set1a),
  {ok, Set1b} = update(Add1Effect, Set1a),
  % DC2 add
  {ok, Add2Effect} = downstream({add, a}, Set0),
  {ok, Set2a} = update(Add2Effect, Set0),
  % pull 2 from DC1 to DC2
  {ok, Set2b} = update(ResetEffect, Set2a),
  {ok, Set2c} = update(Add1Effect, Set2b),

  io:format("ResetEffect = ~p~n", [ResetEffect]),
  io:format("Add1Effect = ~p~n", [Add1Effect]),
  io:format("Add2Effect = ~p~n", [Add2Effect]),

  io:format("Set1a = ~p~n", [Set1a]),
  io:format("Set1b = ~p~n", [Set1b]),
  io:format("Set2a = ~p~n", [Set2a]),
  io:format("Set2b = ~p~n", [Set2b]),
  io:format("Set2c = ~p~n", [Set2c]),

  ?assertEqual([], value(Set1a)),
  ?assertEqual([a], value(Set1b)),
  ?assertEqual([a], value(Set2a)),
  ?assertEqual([], value(Set2b)),
  ?assertEqual([a], value(Set2c)).


reset2_test() ->
  Set0 = new(),
  % DC1 reset
  {ok, Reset1Effect} = downstream(reset, Set0),
  {ok, Set1a} = update(Reset1Effect, Set0),
  % DC1 --> Dc2
  {ok, Set2a} = update(Reset1Effect, Set0),
  % DC2 reset
  {ok, Reset2Effect} = downstream(reset, Set2a),
  {ok, Set2b} = update(Reset2Effect, Set2a),
  % DC2 add
  {ok, Add2Effect} = downstream({add, a}, Set2b),
  {ok, Set2c} = update(Add2Effect, Set2b),
  % DC3 add
  {ok, Add3Effect} = downstream({add, a}, Set0),
  {ok, Set3a} = update(Add3Effect, Set0),
  % DC1 --> DC3
  {ok, Set3b} = update(Reset1Effect, Set3a),
  % DC2 --> DC3
  {ok, Set3c} = update(Reset2Effect, Set3b),
  % DC2 --> DC3
  {ok, Set3d} = update(Add2Effect, Set3c),


  ?assertEqual([], value(Set1a)),
  ?assertEqual([a], value(Set2c)),
  ?assertEqual([a], value(Set3d)).


-endif.
