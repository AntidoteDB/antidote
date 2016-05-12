%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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

%% @doc module antidote_crdt_gset - A wrapper around riak_dt_gset

-module(antidote_crdt_gset).

-behaviour(antidote_crdt).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(RIAK_MODULE, riak_dt_gset).

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

-type gset() :: riak_dt_gset:gset().
-type gset_op() :: {add, {member(), actor()}} | {remove, {member(), actor()}} |
                    {add_all, {[member()], actor()}} | {remove_all, {[member()], actor()}}.

-type member() :: term().
-type actor() :: riak_dt:actor().

new() ->
    ?RIAK_MODULE:new().

value(Set) ->
    ?RIAK_MODULE:value(Set).

-spec downstream(gset_op(), gset()) -> {ok, term()}.
downstream({Op, {OpParam, Actor}}, State) ->
    {ok, S0} = ?RIAK_MODULE:update({Op, OpParam}, Actor, State),
    {ok, {merge, S0}}.

update({merge, State1}, State2) ->
    {ok, ?RIAK_MODULE:merge(State1, State2)}.

require_state_downstream(_Operation) -> true.

is_operation(Operation) ->
    ?RIAK_MODULE:is_operation(Operation).

equal(CRDT1, CRDT2) ->
    ?RIAK_MODULE:equal(CRDT1,CRDT2).

to_binary(CRDT) ->
    ?RIAK_MODULE:to_binary(CRDT).

from_binary(Bin) ->
    ?RIAK_MODULE:from_binary(Bin).

-ifdef(test).
all_test() ->
    S0 = new(),
    {ok, Downstream} = downstream({add, {a, actor}}, S0),
    {ok, S1} = update(Downstream, S0),
    ?assertEqual(1, riak_dt_gset:stat(element_count, S1)).

-endif.
