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
-opaque set() :: orddict:orddict(term(), [token()]).

-type binary_set() :: binary(). %% A binary that from_binary/1 will operate on.

-type set_op() :: {add, member()} | {remove, member()} |
                    {add_all, [member()]} | {remove_all, [member()]}.

-type token() :: term().
-type effect() ::
      {add, [{member(), [token()]}]}
    | {remove, [{member(), [token()]}], token()}.

-type member() :: term().

-spec new() -> set().
new() ->
    orddict:new().

-spec value(set()) -> [member()].
value(Dict) ->
    [Val || {Val, []} <- orddict:to_list(Dict)].


-spec downstream(set_op(), set()) -> {ok, effect()}.
downstream({add, Elem}, Dict) ->
    downstream({add_all, [Elem]}, Dict);
downstream({add_all,Elems}, Dict) ->
    {ok, {add, elemsWithRemovedTombstones(lists:usort(Elems), Dict)}};
downstream({remove, Elem}, Dict) ->
    downstream({remove_all, [Elem]}, Dict);
downstream({remove_all, Elems}, Dict) ->
%%    {ok, {remove, [{E,[]} || E <- lists:usort(Elems)], unique()}}.
% TODO optimization:
    {ok, {remove, elemsWithRemovedTombstones(lists:usort(Elems), Dict), unique()}}.

%% @doc generate a unique identifier (best-effort).
unique() ->
    crypto:strong_rand_bytes(20).

elemsWithRemovedTombstones([], _Dict) -> [];
elemsWithRemovedTombstones(Elems, []) -> [{E,[]} || E <- Elems];
elemsWithRemovedTombstones([E|ElemsRest]=Elems, [{K,Ts}|DictRest]=Dict) ->
    if
        E == K ->
            [{E,Ts}|elemsWithRemovedTombstones(ElemsRest, DictRest)];
        E > K ->
            elemsWithRemovedTombstones(Elems, DictRest);
        true ->
            [{E,[]}|elemsWithRemovedTombstones(ElemsRest, Dict)]
    end.


-spec update(effect(), set()) -> {ok, set()}.
update({add, Elems}, Dict) ->
    {ok, addElems(Elems, Dict,[])};
update({remove,Elems,Token}, Dict) ->
    {ok, addElems(Elems, Dict, [Token])}.

addElems([], Dict, _) -> Dict;
addElems(Elems, [], NewTombs) -> [{E, NewTombs} || {E,_} <- Elems];
addElems([{E,RemovedTombstones}|ElemsRest]=Elems, [{K,Ts}|DictRest]=Dict, NewTombs) ->
    if
        E == K ->
            [{E, NewTombs ++ (Ts -- RemovedTombstones)}|addElems(ElemsRest, DictRest, NewTombs)];
        E > K ->
            [{K,Ts}|addElems(Elems, DictRest, NewTombs)];
        true ->
            [{E, NewTombs}|addElems(ElemsRest, Dict, NewTombs)]
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
is_operation(_) -> false.

require_state_downstream(_) -> true.