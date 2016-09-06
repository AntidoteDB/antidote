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

-module(antidote_crdt_map).

-behaviour(antidote_crdt).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(RIAK_MODULE, riak_dt_map).

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


-type map_op() :: {update, {[map_field_update() | map_field_op()], actorordot()}}.
-type actorordot() :: riak_dt:actor() | riak_dt:dot().

-type map_field_op() ::  {remove, field()}.
-type map_field_update() :: {update, field(), crdt_op()}.
-type crdt_op() :: term(). %% Valid riak_dt udpates
-type field() :: term(). %% riak_dt_map:field().

new() ->
    ?RIAK_MODULE:new().

value(Map) ->
    ?RIAK_MODULE:value(Map).

-spec downstream(map_op(), riak_dt:riak_dt_map()) -> {ok, term()}.
downstream({Op, {OpParam, Actor}}, State) ->
    {ok, S0} = ?RIAK_MODULE:update({Op, OpParam}, Actor, State),
    {ok, {merge, S0}}.

update({merge, State1}, State2) ->
    {ok, ?RIAK_MODULE:merge(State1, State2)}.

require_state_downstream(_Operation) -> true.

is_operation({Op, {Param, _Actor}}) ->
    ?RIAK_MODULE:is_operation({Op, Param}).

equal(CRDT1, CRDT2) ->
    ?RIAK_MODULE:equal(CRDT1,CRDT2).

to_binary(CRDT) ->
    ?RIAK_MODULE:to_binary(CRDT).

from_binary(Bin) ->
    ?RIAK_MODULE:from_binary(Bin).

-ifdef(TEST).
all_test() ->
    S0 = new(),
    Field = {'C', riak_dt_pncounter},
    {ok, Downstream} = downstream({update, {[{update, Field, {increment, 1}}], actor}}, S0),
    {ok, S1} = update(Downstream, S0),
    ?assertEqual([{Field, 1}], value(S1)).

type_check_test() ->
    Res = is_operation({update,{[{update,{key,riak_dt_lwwreg},{assign,<<"A">>}},
                                          {update,{val,riak_dt_pncounter},{increment,1}}],
                                         hardcodedactor}}),
    ?assertEqual(true, Res).

-endif.
