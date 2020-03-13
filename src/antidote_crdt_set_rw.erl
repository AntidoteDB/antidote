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
%% An operation-based Remove-wins Set CRDT.
%%
%% Semantics:
%% Add-operations only affect the remove-operations, which they have observed.
%% The concurrent remove-operations will win over an add-operation.
%% A reset operation has the same effect as removing all possible elements (not only the ones currently in the set).
%%
%% Implementation:
%% The implementation is not very efficient, in particular tombstones for removed elements are stored forever.
%%
%% The state is a pair of:
%%  a) the reset tokens, which store when the last resets were executed
%%  b) a mapping from elements to a set of tombstone-tokens
%%
%% An element is in the set, if its set of tombstone-tokens is empty.
%% An add-operation will remove the current tombstones of an element.
%%
%% In this implementation of the RWSet, we remove entries for elements having no tokens
%% (i.e. AddTokens == [] and RemoveTokens == [])

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
          is_bottom/1,
          require_state_downstream/1
        ]).


-behaviour(antidote_crdt).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type antidote_crdt_set_rw() :: orddict:orddict(term(), {tokens(), tokens()}).

-type binary_antidote_crdt_set_rw() :: binary(). %% A binary that from_binary/1 will operate on.

-type antidote_crdt_set_rw_op() ::
      {add, member()}
    | {remove, member()}
    | {add_all, [member()]}
    | {remove_all, [member()]}
    | {reset, {}}.

%% element, CurrentTokens, AddTokens, RemoveTokens
-type downstream_op() :: [{member(), tokens(), tokens(), tokens()}].

-type member() :: term().
-type token() :: term().
-type tokens() :: [token()].

-spec new() -> antidote_crdt_set_rw().
new() ->
  orddict:new().

-spec value(antidote_crdt_set_rw()) -> [member()].
value(RWSet) ->
  [Elem || {Elem, {_, []}} <- RWSet].

-spec downstream(antidote_crdt_set_rw_op(), antidote_crdt_set_rw()) -> {ok, downstream_op()}.
downstream({add, Elem}, RWSet) ->
  downstream({add_all, [Elem]}, RWSet);
downstream({add_all, Elems}, RWSet) ->
  CreateDownstream = fun(Elem, CurrentTokens) ->
      Token = unique(),
      {Elem, CurrentTokens, [Token], []}
  end,
  DownstreamOps = create_downstreams(CreateDownstream, lists:usort(Elems), RWSet, []),
  {ok, lists:reverse(DownstreamOps)};
downstream({remove, Elem}, RWSet) ->
    downstream({remove_all, [Elem]}, RWSet);
downstream({remove_all, Elems}, RWSet) ->
  CreateDownstream = fun(Elem, CurrentTokens) ->
      Token = unique(),
      {Elem, CurrentTokens, [], [Token]}
  end,
  DownstreamOps = create_downstreams(CreateDownstream, lists:usort(Elems), RWSet, []),
  {ok, lists:reverse(DownstreamOps)};
downstream({reset, {}}, RWSet) ->
  CreateDownstream = fun(Elem, CurrentTokens) ->
      {Elem, CurrentTokens, [], []}
  end,
  DownstreamOps = create_downstreams(CreateDownstream, lists:usort(orddict:fetch_keys(RWSet)), RWSet, []),
  {ok, lists:reverse(DownstreamOps)}.

%% @private generic downstream op creation for adds and removals
create_downstreams(_CreateDownstream, [], _RWSet, DownstreamOps) ->
  DownstreamOps;
create_downstreams(CreateDownstream, Elems, [], DownstreamOps) ->
  lists:foldl(
    fun(Elem, Ops) ->
      DownstreamOp = CreateDownstream(Elem, []),
      [DownstreamOp|Ops]
    end,
    DownstreamOps,
    Elems
  );
create_downstreams(CreateDownstream, [Elem1|ElemsRest]=Elems, [{Elem2, {AddTokens, RemoveTokens}}|RWSetRest]=RWSet, DownstreamOps) ->
  if
    Elem1 == Elem2 ->
      DownstreamOp = CreateDownstream(Elem1, AddTokens ++ RemoveTokens),
      create_downstreams(CreateDownstream, ElemsRest, RWSetRest, [DownstreamOp|DownstreamOps]);
    Elem1 > Elem2 ->
      create_downstreams(CreateDownstream, Elems, RWSetRest, DownstreamOps);
    true ->
      DownstreamOp = CreateDownstream(Elem1, []),
      create_downstreams(CreateDownstream, ElemsRest, RWSet, [DownstreamOp|DownstreamOps])
  end.

%% @doc generate a unique identifier (best-effort).
unique() ->
    crypto:strong_rand_bytes(20).

-spec update(downstream_op(), antidote_crdt_set_rw()) -> {ok, antidote_crdt_set_rw()}.
  update(DownstreamOp, RWSet) ->
    RWSet1 = apply_downstreams(DownstreamOp, RWSet),
    RWSet2 = [Entry || {_, {AddTokens, RemoveTokens}} = Entry <- RWSet1, AddTokens =/= [] orelse RemoveTokens =/= []],
    {ok, RWSet2}.

%% @private apply a list of downstream ops to a given orset
apply_downstreams([], RWSet) ->
  RWSet;
apply_downstreams(Ops, []) ->
  [apply_downstream(Elem, SeenTokens, ToAdd, ToRemove, [], []) || {Elem, SeenTokens, ToAdd, ToRemove} <- Ops];
apply_downstreams([{Elem1, SeenTokens, ToAdd, ToRemove}|OpsRest]=Ops, [{Elem2, {CurrentAddTokens, CurrentRemoveTokens}}|RWSetRest]=RWSet) ->
  if
    Elem1 == Elem2 ->
      [apply_downstream(Elem1, SeenTokens, ToAdd, ToRemove, CurrentAddTokens, CurrentRemoveTokens) | apply_downstreams(OpsRest, RWSetRest)];
    Elem1 > Elem2 ->
      [{Elem2, {CurrentAddTokens, CurrentRemoveTokens}} | apply_downstreams(Ops, RWSetRest)];
    true ->
      [apply_downstream(Elem1, SeenTokens, ToAdd, ToRemove, [], []) | apply_downstreams(OpsRest, RWSet)]
  end.

%% @private create an orddict entry from a downstream op
apply_downstream(Elem, SeenTokens, ToAdd, ToRemove, CurrentAddTokens, CurrentRemoveTokens) ->
  AddTokens = (CurrentAddTokens ++ ToAdd) -- SeenTokens,
  RemoveTokens = (CurrentRemoveTokens ++ ToRemove) -- SeenTokens,
  {Elem, {AddTokens, RemoveTokens}}.



-spec equal(antidote_crdt_set_rw(), antidote_crdt_set_rw()) -> boolean().
equal(ORDictA, ORDictB) ->
    ORDictA == ORDictB. % Everything inside is ordered, so this should work

-define(TAG, 77).
-define(V1_VERS, 1).

-spec to_binary(antidote_crdt_set_rw()) -> binary_antidote_crdt_set_rw().
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
is_operation({reset, {}}) -> true;
is_operation(_) -> false.

is_bottom(RWSet) ->
  RWSet == new().

require_state_downstream(_) -> true.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

reset1_test() ->
  Set0 = new(),
  % DC1 reset
  {ok, ResetEffect} = downstream({reset, {}}, Set0),
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
  ?assertEqual([a], value(Set2b)),
  ?assertEqual([a], value(Set2c)).


reset2_test() ->
  Set0 = new(),
  % DC1 reset
  {ok, Reset1Effect} = downstream({reset, {}}, Set0),
  {ok, Set1a} = update(Reset1Effect, Set0),
  % DC1 --> Dc2
  {ok, Set2a} = update(Reset1Effect, Set0),
  % DC2 reset
  {ok, Reset2Effect} = downstream({reset, {}}, Set2a),
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


  add_test() ->
  Set0 = new(),
  io:format("Set0 = ~p~n", [Set0]),
  % DC1 add
  {ok, Add1Effect} = downstream({add, a}, Set0),
  {ok, Set1} = update(Add1Effect, Set0),
  io:format("Set1 = ~p~n", [Set1]),
  % DC1 add
  {ok, Add2Effect} = downstream({add, b}, Set1),
  {ok, Set2} = update(Add2Effect, Set1),
  io:format("Set2 = ~p~n", [Set2]),

  io:format("Add1Effect = ~p~n", [Add1Effect]),
  io:format("Add2Effect = ~p~n", [Add2Effect]),

  ?assertEqual([], value(Set0)),
  ?assertEqual([a], value(Set1)),
  ?assertEqual([a, b], value(Set2)).

  prop1_test() ->
  Set0 = new(),
  % DC1 reset
  {ok, ResetEffect} = downstream({reset, {}}, Set0),
  {ok, Set1a} = update(ResetEffect, Set0),
  % DC3 add b
  {ok, Add3Effect} = downstream({add, b}, Set0),
  {ok, Set3a} = update(Add3Effect, Set0),
  % pull from DC3 to DC1
  {ok, Set1b} = update(Add3Effect, Set1a),

  io:format("ResetEffect = ~p~n", [ResetEffect]),
  io:format("Add3Effect = ~p~n", [Add3Effect]),

  io:format("Set1a = ~p~n", [Set1a]),
  io:format("Set3a = ~p~n", [Set3a]),
  io:format("Set1b = ~p~n", [Set1b]),

  ?assertEqual([], value(Set1a)),
  ?assertEqual([b], value(Set3a)),
  ?assertEqual([b], value(Set1b)).

  prop2_test() ->
  Set0 = new(),
  % DC1 remove b
  {ok, Remove1Effect} = downstream({remove, b}, Set0),
  {ok, Set1a} = update(Remove1Effect, Set0),
  % DC3 add a
  {ok, Add3Effect} = downstream({add, a}, Set0),
  {ok, Set3a} = update(Add3Effect, Set0),
  % pull from DC3 to DC1
  {ok, Set1b} = update(Add3Effect, Set1a),

  io:format("Remove1Effect = ~p~n", [Remove1Effect]),
  io:format("Add3Effect = ~p~n", [Add3Effect]),

  io:format("Set1a = ~p~n", [Set1a]),
  io:format("Set3a = ~p~n", [Set3a]),
  io:format("Set1b = ~p~n", [Set1b]),

  ?assertEqual([], value(Set1a)),
  ?assertEqual([a], value(Set3a)),
  ?assertEqual([a], value(Set1b)).


prop3_test() ->
  Set0 = new(),
  % DC2 add a
  {ok, Add2Effect} = downstream({add, a}, Set0),
  {ok, Set2a} = update(Add2Effect, Set0),
  % DC3 reset
  {ok, Reset3Effect} = downstream({reset, {}}, Set0),
  {ok, Set3a} = update(Reset3Effect, Set0),
  % pull from DC2 to DC3
  {ok, Set3b} = update(Add2Effect, Set3a),

  io:format("Reset3Effect = ~p~n", [Reset3Effect]),
  io:format("Add2Effect = ~p~n", [Add2Effect]),

  io:format("Set2a = ~p~n", [Set2a]),
  io:format("Set3a = ~p~n", [Set3a]),
  io:format("Set3b = ~p~n", [Set3b]),

  ?assertEqual([a], value(Set2a)),
  ?assertEqual([], value(Set3a)),
  ?assertEqual([a], value(Set3b)).

prop4_test() ->
  Set0 = new(),
  % DC2 add a
  {ok, Add2Effect} = downstream({add, a}, Set0),
  {ok, Set2a} = update(Add2Effect, Set0),
  % DC3 reset
  {ok, Reset3Effect} = downstream({reset, {}}, Set0),
  {ok, Set3a} = update(Reset3Effect, Set0),
  % pull from DC3 to DC2
  {ok, Set2b} = update(Reset3Effect, Set2a),

  io:format("Reset3Effect = ~p~n", [Reset3Effect]),
  io:format("Add2Effect = ~p~n", [Add2Effect]),

  io:format("Set2a = ~p~n", [Set2a]),
  io:format("Set3a = ~p~n", [Set3a]),
  io:format("Set3b = ~p~n", [Set2b]),

  ?assertEqual([a], value(Set2a)),
  ?assertEqual([], value(Set3a)),
  ?assertEqual([a], value(Set2b)).

  prop5_test() ->
  Set0 = new(),
  % DC2 add a
  {ok, Add2Effect} = downstream({add, a}, Set0),
  {ok, Set2a} = update(Add2Effect, Set0),
  % DC3 reset
  {ok, Reset2Effect} = downstream({reset, {}}, Set2a),
  {ok, Set2b} = update(Reset2Effect, Set2a),

  io:format("Reset2Effect = ~p~n", [Reset2Effect]),
  io:format("Add2Effect = ~p~n", [Add2Effect]),

  io:format("Set2a = ~p~n", [Set2a]),
  io:format("Set2b = ~p~n", [Set2b]),

  ?assertEqual([a], value(Set2a)),
  ?assertEqual([], Set2b),
  ?assertEqual([], value(Set2b)).

-endif.
