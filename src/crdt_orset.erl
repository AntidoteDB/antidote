%% -------------------------------------------------------------------
%%
%% crdt_orset: A convergent, replicated, state based observe remove set
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

-module(crdt_orset).


%% API
-export([new/0, value/1, update/3, equal/2,
         to_binary/1, from_binary/1, value/2, precondition_context/1, stats/1, stat/2]).
-export([update/4, parent_clock/2]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% EQC API
-ifdef(EQC).
-export([init_state/0, gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

-export_type([orset/0, binary_orset/0, orset_op/0]).
-opaque orset() :: orddict:orddict().

-type binary_orset() :: binary(). %% A binary that from_binary/1 will operate on.

-type orset_op() :: {add, member()} | {remove, member()} |
                    {get_downstream, member()}|
                    {add_all, [member()]} | {remove_all, [member()]} |
                    {update, [orset_op()]}.

-type actor() :: riak_dt:actor().
-type member() :: term().

-spec new() -> orset().
new() ->
    orddict:new().

-spec value(orset()) -> [member()].
value(ORDict) ->
    orddict:fetch_keys(ORDict).

-spec value(any(), orset()) -> [member()] | orddict:orddict().
value({fragment, Elem}, ORSet) ->
    case value({tokens, Elem}, ORSet) of
        [] ->
            orddict:new();
        Tokens ->
            orddict:store(Elem, Tokens, orddict:new())
    end;
value({tokens, Elem}, ORSet) ->
    case orddict:find(Elem, ORSet) of
        error ->
            orddict:new();
        {ok, Tokens} ->
            Tokens
    end;
value(_,ORSet) ->
    value(ORSet).

-spec update(orset_op(), actor(), orset()) -> {ok, orset()} |
                                              {error, {precondition ,{not_present, member()}}}.
update({add,Elem}, Actor, _ORDict) ->
    Token = unique(Actor),
    {ok, {downstream_add, {Elem,[Token]}}};
update({downstream_add, {Elem, [Token|_]}}, _Actor, ORDict) ->
    add_elem(Elem,Token,ORDict);
update({add_all,Elems}, Actor, _ORDict0) ->
    DownstreamOp = lists:foldl(fun(Elem, Sum) ->
                                Token = unique(Actor),
                                Sum++[{Elem, [Token]}]
                                end, [], Elems),
    {ok, {downstream_add_all, DownstreamOp}};
update({downstream_add_all,Elems}, Actor, ORDict0) ->
    OD = lists:foldl(fun(Elem,ORDict) ->
                {ok, ORDict1} = update({downstream_add,Elem},Actor,ORDict),
                ORDict1
            end, ORDict0, Elems),
    {ok, OD};
update({remove, Elem}, _Actor, ORDict) ->
    ToRemove = value({tokens, Elem}, ORDict),
    {ok, {downstream_remove, {Elem, ToRemove}}};
update({downstream_remove, Elem}, _Actor, ORDict) ->
    remove_elem(Elem, ORDict);
update({remove_all,Elems}, _Actor, ORDict) ->
    ToRemove = lists:foldl(fun(Elem, Sum) -> Sum++[{Elem, value({tokens, Elem}, ORDict)}] end, [], Elems),
    {ok, {downstream_remove_all, ToRemove}};
update({downstream_remove_all,Elems}, _Actor, ORDict0) ->
    remove_elems(Elems, ORDict0);
update({update, Ops}, Actor, ORDict) ->
    apply_ops(Ops, Actor, ORDict).

-spec update(orset_op(), actor(), orset(), riak_dt:context()) ->
                    {ok, orset()} | {error, {precondition ,{not_present, member()}}}.
update(Op, Actor, ORDict, _Ctx) ->
    update(Op, Actor, ORDict).

-spec parent_clock(riak_dt_vclock:vclock(), orset()) -> orset().
parent_clock(_Clock, ORSet) ->
    ORSet.

-spec equal(orset(), orset()) -> boolean().
equal(ORDictA, ORDictB) ->
    ORDictA == ORDictB. % Everything inside is ordered, so this should work

%% @doc the precondition context is a fragment of the CRDT that
%% operations with pre-conditions can be applied too.  In the case of
%% OR-Sets this is the set of adds observed.  The system can then
%% apply a remove to this context and merge it with a replica.
%% Especially useful for hybrid op/state systems where the context of
%% an operation is needed at a replica without sending the entire
%% state to the client.
-spec precondition_context(orset()) -> orset().
precondition_context(ORDict) ->
    orddict:fold(fun(Elem, Tokens, ORDict1) ->
            case minimum_tokens(Tokens) of
                []      -> ORDict1;
                Tokens1 -> orddict:store(Elem, Tokens1, ORDict1)
            end
        end, orddict:new(), ORDict).

-spec stats(orset()) -> [{atom(), number()}].
stats(ORSet) ->
    [ {S, stat(S, ORSet)} || S <- [element_count] ].

-spec stat(atom(), orset()) -> number() | undefined.
stat(element_count, ORSet) ->
    orddict:size(ORSet);
stat(_, _) -> undefined.

-include("../deps/riak_dt/include/riak_dt_tags.hrl").
-define(TAG, ?DT_ORSET_TAG).
-define(V1_VERS, 1).

-spec to_binary(orset()) -> binary_orset().
to_binary(ORSet) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(ORSet))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    binary_to_term(Bin).

%% Private
add_elem(Elem,Token,ORDict) ->
    case orddict:find(Elem,ORDict) of
        {ok, Tokens} ->
            {ok, orddict:store(Elem, Tokens++[Token], ORDict)};
        error ->
            {ok, orddict:store(Elem, [Token], ORDict)}
    end.

remove_elem({Elem,RemoveTokens},ORDict) ->
    case orddict:find(Elem,ORDict) of
        {ok, Tokens} ->
            RestTokens = Tokens--RemoveTokens,
            case RestTokens of 
                [] ->
                    {ok, orddict:erase(Elem, ORDict)};
                _ -> 
                    {ok, orddict:store(Elem, Tokens--RemoveTokens, ORDict)}
            end;
        error ->
            {error, {precondition, {not_present, Elem}}}
    end.

remove_elems([], ORDict) ->
    {ok, ORDict};
remove_elems([Elem|Rest], ORDict) ->
    case remove_elem(Elem,ORDict) of
        {ok, ORDict1} -> remove_elems(Rest, ORDict1);
        Error         -> Error
    end.

apply_ops([], _Actor, ORDict) ->
    {ok, ORDict};
apply_ops([Op | Rest], Actor, ORDict) ->
    case update(Op, Actor, ORDict) of
        {ok, ORDict1} -> apply_ops(Rest, Actor, ORDict1);
        Error -> Error
    end.

unique(_Actor) ->
    crypto:strong_rand_bytes(20).

minimum_tokens(Tokens) ->
    orddict:filter(fun(_Token, Removed) ->
            not Removed
        end, Tokens).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
new_test() ->
    ?assertEqual(orddict:new(), new()). 

add_test() ->
    Set1 = new(),
    {ok, DownstreamOp1} = update({add, <<"foo">>}, 1, Set1),
    ?assertMatch({downstream_add, {<<"foo">>, _}}, DownstreamOp1),
    {ok, DownstreamOp2} = update({add_all, [<<"li">>,<<"manu">>]}, 1, Set1),
    ?assertMatch({downstream_add_all, [{<<"li">>, _}, {<<"manu">>, _}]}, DownstreamOp2),
    {ok, Set2} = update(DownstreamOp1, 1, Set1),
    {_, Elem1} = DownstreamOp1,
    ?assertEqual([Elem1], orddict:to_list(Set2)),
    {ok, Set3} = update(DownstreamOp2, 1, Set1),
    {_, Elems2} = DownstreamOp2,
    ?assertEqual(Elems2, orddict:to_list(Set3)).

value_test() ->
    Set1 = new(),
    {ok, DownstreamOp1} = update({add, <<"foo">>}, 1, Set1),
    ?assertEqual([], value(Set1)),
    {ok, Set2} = update(DownstreamOp1, 1, Set1),
    ?assertEqual([<<"foo">>], value(Set2)),
    {ok, DownstreamOp2} = update({add_all, [<<"foo">>, <<"li">>,<<"manu">>]}, 1, Set2),
    {ok, Set3} = update(DownstreamOp2, 1, Set2),
    ?assertEqual([<<"foo">>, <<"li">>, <<"manu">>], value(Set3)),

    {_, {_, Token1}}=DownstreamOp1,
    {_, [{_, Token2}|_]}=DownstreamOp2,
    ?assertEqual(Token1, value({tokens, <<"foo">>}, Set2)),
    ?assertEqual(Token1++Token2, value({tokens, <<"foo">>}, Set3)),

    ?assertEqual(orddict:store(<<"foo">>, Token1++Token2, orddict:new()), value({fragment, <<"foo">>}, Set3)).

remove_test() ->
    Set1 = new(),
    %% Add an element then remove it
    {ok, Op1} = update({add, <<"foo">>}, 1, Set1),
    {ok, Set2} = update(Op1, 1, Set1),
    ?assertEqual([<<"foo">>], value(Set2)),
    {ok, Op2} = update({remove, <<"foo">>}, 1, Set2),
    {ok, Set3} = update(Op2, 1, Set2),
    ?assertEqual([], value(Set3)),

    %% Add many elements then remove part
    {ok, Op3} = update({add_all, [<<"foo">>, <<"li">>,<<"manu">>]}, 1, Set1),
    {ok, Set4} = update(Op3, 1, Set1),
    ?assertEqual([<<"foo">>, <<"li">>, <<"manu">>], value(Set4)),

    {ok, Op5} = update({remove_all, [<<"foo">>, <<"li">>]},1 , Set4),
    {ok, Set5} = update(Op5, 1, Set4),
    ?assertEqual([<<"manu">>], value(Set5)),
    
    %% Remove more than current have
    {ok, Op6} = update({add_all, [<<"foo">>, <<"li">>,<<"manu">>]}, 1, Set1),
    {ok, Set6} = update(Op6, 1, Set1),
    {ok, Op7} = update({remove_all, [<<"manu">>, <<"test">>]}, 1, Set6),
    Result = update(Op7, 1, Set6),
    ?assertEqual({error,{precondition,{not_present,<<"test">>}}}, Result).

    
concurrent_add_test() ->
    Set1 = new(),
    %% Add an element then remove it
    {ok, Op1} = update({add, <<"foo">>}, 1, Set1),
    {ok, Set2} = update(Op1, 1, Set1),
    ?assertEqual([<<"foo">>], value(Set2)),
    
    %% If remove is concurrent with the second add, will not remove the second added 
    {ok, Op2} = update({remove, <<"foo">>}, 1, Set2),

    {ok, Op3} = update({add, <<"foo">>}, 1, Set1),
    {ok, Set3} = update(Op3, 1, Set2),
    ?assertEqual([<<"foo">>], value(Set3)),

    {ok, Set4} = update(Op2, 1, Set3),
    ?assertEqual([<<"foo">>], value(Set4)),

    %% If remove follows two adds, remove will remove all
    {ok, Op4} = update({remove, <<"foo">>}, 1, Set3),
    {ok, Set5} = update(Op4, 1, Set3),
    ?assertEqual([], value(Set5)).
    
