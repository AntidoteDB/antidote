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
-module(antidotec_reg).

-include_lib("antidote_pb_codec/include/antidote_pb.hrl").

-behaviour(antidotec_datatype).

-export([new/0,
         new/1,
         value/1,
         dirty_value/1,
         is_type/1,
         to_ops/2,
         type/0
        ]).

-export([assign/2
        ]).

-record(antidote_reg, {
            value,
            new_value
         }).

-export_type([antidote_reg/0]).
-opaque antidote_reg() :: #antidote_reg{}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-spec new() -> antidote_reg().
new() ->
    #antidote_reg{value=""}.


new(Value) ->
    #antidote_reg{value=Value}.

-spec value(antidote_reg()) -> [term()].
value(#antidote_reg{value=Value}) -> Value.

dirty_value(#antidote_reg{value=Value, new_value=undefined}) -> Value;
dirty_value(#antidote_reg{new_value=NewValue}) -> NewValue.

%% @doc Determines whether the passed term is a reg container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, antidote_reg).

%% @doc Returns the symbolic name of this container.
-spec type() -> reg.
type() -> reg.

to_ops(_, #antidote_reg{new_value=undefined}) ->
  [];

to_ops(BoundObject, #antidote_reg{new_value=NewValue}) ->
    [{BoundObject, assign, NewValue}].

assign(Reg, Value) ->
    Reg#antidote_reg{new_value=Value}.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
-endif.
