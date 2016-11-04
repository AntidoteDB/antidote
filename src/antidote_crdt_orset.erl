%% -------------------------------------------------------------------
%%
%% crdt_orset: A convergent, replicated, operation based observe remove set
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
%% An operation-based Observed-Remove Set CRDT.
%% As the data structure is operation-based, to issue an operation, one should
%% firstly call `downstream/2' to get the downstream version of the
%% operation and then call `update/2'.
%%
%% It provides five operations: add, which adds an element to a set; add_all,
%% adds a list of elements to a set; remove, which removes an element from a set;
%% remove_all that removes a list of elements from the set; update, that contains
%% a list of previous four commands.
%%
%% This file is adapted from riak_dt_orset, a state-based implementation of
%% Observed-Remove Set.
%% The changes are as follows:
%% 1. `generate_downstream/3' is added, as this is necessary for op-based CRDTs.
%% 2. `merge/2' is removed.
%% 3. There is no tombstone of removed elements.
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end
-module(antidote_crdt_orset).

-include("antidote_crdt.hrl").

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

-export_type([orset/0, binary_orset/0, orset_op/0]).
-opaque orset() :: orddict:orddict(member(), tokens()).

-type binary_orset() :: binary(). %% A binary that from_binary/1 will operate on.

-type orset_op() ::
      {add, member()}
    | {remove, member()}
    | {add_all, [member()]}
    | {remove_all, [member()]}
    | {reset, {}}.

%% The downstream op is a list of triples.
%% In each triple:
%%  - the first component is the elem that was added or removed
%%  - the second component is the list of supporting tokens to be added
%%  - the third component is the list of supporting tokens to be removed
-type downstream_op() :: [{member(), tokens(), tokens()}].

-type member() :: term().
-type token() :: binary().
-type tokens() :: [token()].

-spec new() -> orset().
new() ->
    orddict:new().

%% @doc return all existing elements in the `orset()'.
-spec value(orset()) -> [member()].
value(ORSet) ->
    orddict:fetch_keys(ORSet).

%% @doc generate downstream operations.
%% If the operation is add or add_all, generate unique tokens for
%% each element and fetches the current supporting tokens.
%% If the operation is remove or remove_all, fetches current
%% supporting tokens of these elements existing in the `orset()'.
-spec downstream(orset_op(), orset()) -> {ok, downstream_op()}.
downstream({add, Elem}, ORSet) ->
    downstream({add_all, [Elem]}, ORSet);
downstream({add_all, Elems}, ORSet) ->
    CreateDownstream = fun(Elem, CurrentTokens) ->
        Token = unique(),
        {Elem, [Token], CurrentTokens}
    end,
    DownstreamOps = create_downstreams(CreateDownstream, lists:usort(Elems), ORSet, []),
    {ok, lists:reverse(DownstreamOps)};
downstream({remove, Elem}, ORSet) ->
    downstream({remove_all, [Elem]}, ORSet);
downstream({remove_all, Elems}, ORSet) ->
    CreateDownstream = fun(Elem, CurrentTokens) ->
        {Elem, [], CurrentTokens}
    end,
    DownstreamOps = create_downstreams(CreateDownstream, lists:usort(Elems), ORSet, []),
    {ok, lists:reverse(DownstreamOps)};
downstream({reset, {}}, ORSet) ->
    % reset is like removing all elements
    downstream({remove_all, value(ORSet)}, ORSet).

%% @doc apply downstream operations and update an `orset()'.
-spec update(downstream_op(), orset()) -> {ok, orset()}.
update(DownstreamOp, ORSet) ->
    {ok, apply_downstreams(DownstreamOp, ORSet)}.

-spec equal(orset(), orset()) -> boolean().
equal(ORSetA, ORSetB) ->
    % Everything inside is ordered, so this should work
    ORSetA == ORSetB.

-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(TAG, ?DT_ORSET_TAG).
-define(V1_VERS, 1).

-spec to_binary(orset()) -> binary_orset().
to_binary(ORSet) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(ORSet))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    {ok, binary_to_term(Bin)}.

%% @doc generate a unique identifier (best-effort).
-spec unique() -> token().
unique() ->
    crypto:strong_rand_bytes(20).

%% @private generic downstream op creation for adds and removals
create_downstreams(_CreateDownstream, [], _ORSet, DownstreamOps) ->
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
create_downstreams(CreateDownstream, [Elem1|ElemsRest]=Elems, [{Elem2, Tokens}|ORSetRest]=ORSet, DownstreamOps) ->
    if
        Elem1 == Elem2 ->
            DownstreamOp = CreateDownstream(Elem1, Tokens),
            create_downstreams(CreateDownstream, ElemsRest, ORSetRest, [DownstreamOp|DownstreamOps]);
        Elem1 > Elem2 ->
            create_downstreams(CreateDownstream, Elems, ORSetRest, DownstreamOps);
        true ->
            DownstreamOp = CreateDownstream(Elem1, Tokens),
            create_downstreams(CreateDownstream, ElemsRest, ORSet, [DownstreamOp|DownstreamOps])
    end.

%% @private apply a list of downstream ops to a given orset
apply_downstreams([], ORSet) ->
    ORSet;
apply_downstreams(Ops, []) ->
    lists:foldl(
        fun({Elem, ToAdd, ToRemove}, ORSet) ->
            ORSet ++ apply_downstream(Elem, [], ToAdd, ToRemove)
        end,
        [],
        Ops
    );
apply_downstreams([{Elem1, ToAdd, ToRemove}|OpsRest]=Ops, [{Elem2, CurrentTokens}|ORSetRest]=ORSet) ->
    if
        Elem1 == Elem2 ->
            apply_downstream(Elem1, CurrentTokens, ToAdd, ToRemove) ++ apply_downstreams(OpsRest, ORSetRest);
        Elem1 > Elem2 ->
            [{Elem2, CurrentTokens} | apply_downstreams(Ops, ORSetRest)];
        true ->
            apply_downstream(Elem1, [], ToAdd, ToRemove) ++ apply_downstreams(OpsRest, ORSet)
    end.

%% @private create an orddict entry from a downstream op
apply_downstream(Elem, CurrentTokens, ToAdd, ToRemove) ->
    Tokens = (CurrentTokens ++ ToAdd) -- ToRemove,
    case Tokens of
        [] ->
            [];
        _ ->
            [{Elem, Tokens}]
    end.

%% @doc The following operation verifies
%%      that Operation is supported by this particular CRDT.
is_operation({add, _Elem}) -> true;
is_operation({add_all, L}) when is_list(L) -> true;
is_operation({remove, _Elem}) -> true;
is_operation({remove_all, L}) when is_list(L) -> true;
is_operation({reset, {}}) -> true;
is_operation(_) -> false.

require_state_downstream({add, _}) -> true;
require_state_downstream({add_all, _}) -> true;
require_state_downstream({remove, _}) -> true;
require_state_downstream({remove_all, _}) -> true;
require_state_downstream({reset, {}}) -> true.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual(orddict:new(), new()).

add_test() ->
    Elem = <<"foo">>,
    Elems = [<<"li">>, <<"manu">>],
    Set1 = new(),
    {ok, DownstreamOp1} = downstream({add, Elem}, Set1),
    ?assertMatch([{Elem, _, _}], DownstreamOp1),
    {ok, DownstreamOp2} = downstream({add_all, Elems}, Set1),
    ?assertMatch([{<<"li">>, _, _}, {<<"manu">>, _, _}], DownstreamOp2),
    {ok, Set2} = update(DownstreamOp1, Set1),
    ?assertEqual([Elem], value(Set2)),
    {ok, Set3} = update(DownstreamOp2, Set1),
    ?assertEqual(Elems, value(Set3)).

value_test() ->
    Set1 = new(),
    {ok, DownstreamOp1} = downstream({add, <<"foo">>}, Set1),
    ?assertEqual([], value(Set1)),
    {ok, Set2} = update(DownstreamOp1, Set1),
    ?assertEqual([<<"foo">>], value(Set2)),
    {ok, DownstreamOp2} = downstream({add_all, [<<"foo">>, <<"li">>, <<"manu">>]}, Set2),
    {ok, Set3} = update(DownstreamOp2, Set2),
    ?assertEqual([<<"foo">>, <<"li">>, <<"manu">>], value(Set3)).

remove_test() ->
    Set1 = new(),
    %% Add an element then remove it
    {ok, Op1} = downstream({add, <<"foo">>}, Set1),
    {ok, Set2} = update(Op1, Set1),
    ?assertEqual([<<"foo">>], value(Set2)),
    {ok, Op2} = downstream({remove, <<"foo">>}, Set2),
    {ok, Set3} = update(Op2, Set2),
    ?assertEqual([], value(Set3)),

    %% Add many elements then remove part
    {ok, Op3} = downstream({add_all, [<<"foo">>, <<"li">>, <<"manu">>]}, Set1),
    {ok, Set4} = update(Op3, Set1),
    ?assertEqual([<<"foo">>, <<"li">>, <<"manu">>], value(Set4)),

    {ok, Op5} = downstream({remove_all, [<<"foo">>, <<"li">>]}, Set4),
    {ok, Set5} = update(Op5, Set4),
    ?assertEqual([<<"manu">>], value(Set5)),

    %% Remove more than current have
    {ok, Op6} = downstream({add_all, [<<"foo">>, <<"li">>, <<"manu">>]}, Set1),
    {ok, Set6} = update(Op6, Set1),
    {ok, Op7} = downstream({remove_all, [<<"manu">>, <<"test">>]}, Set6),
    Result = update(Op7, Set6),
    ?assertMatch({ok, _}, Result).


remove2_test() ->
    Set1 = new(),
    %% Add an element then remove it
    {ok, Op1} = downstream({add, <<"foo">>}, Set1),
    {ok, Set2} = update(Op1, Set1),
    ?assertEqual([<<"foo">>], value(Set2)),
    {ok, Op2} = downstream({remove, <<"foo">>}, Set2),
    {ok, Set3} = update(Op2, Set2),
    ?assertEqual([], value(Set3)),

    %% Remove the element again (e.g. on a different replica)
    {ok, Op3} = downstream({remove, <<"foo">>}, Set2),
    {ok, Set4} = update(Op3, Set2),
    ?assertEqual([], value(Set4)),

    %% now execute Op3 on Set3, where the element was already removed locally
    {ok, Set5} = update(Op3, Set3),
    ?assertEqual([], value(Set5)).


concurrent_add_test() ->
    Set1 = new(),
    %% Add an element then remove it
    {ok, Op1} = downstream({add, <<"foo">>}, Set1),
    {ok, Set2} = update(Op1, Set1),
    ?assertEqual([<<"foo">>], value(Set2)),

    %% If remove is concurrent with the second add, will not remove the second added
    {ok, Op2} = downstream({remove, <<"foo">>}, Set2),

    {ok, Op3} = downstream({add, <<"foo">>}, Set1),
    {ok, Set3} = update(Op3, Set2),
    ?assertEqual([<<"foo">>], value(Set3)),

    {ok, Set4} = update(Op2, Set3),
    ?assertEqual([<<"foo">>], value(Set4)),

    %% If remove follows two adds, remove will remove all
    {ok, Op4} = downstream({remove, <<"foo">>}, Set3),
    {ok, Set5} = update(Op4, Set3),
    ?assertEqual([], value(Set5)).

binary_test() ->
    ORSet1 = new(),
    BinaryORSet1 = to_binary(ORSet1),
    {ok, ORSet2} = from_binary(BinaryORSet1),
    ?assert(equal(ORSet1, ORSet2)),

    {ok, Op1} = downstream({add, <<"foo">>}, ORSet1),
    {ok, ORSet3} = update(Op1, ORSet1),
    BinaryORSet3 = to_binary(ORSet3),
    {ok, ORSet4} = from_binary(BinaryORSet3),
    ?assert(equal(ORSet3, ORSet4)).

-endif.
