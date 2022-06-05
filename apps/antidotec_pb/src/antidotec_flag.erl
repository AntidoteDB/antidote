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
-module(antidotec_flag).

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

-export([assign/2, enable/1, disable/1]).

-record(antidote_flag, {
            value,
            new_value
         }).

-export_type([antidote_flag/0]).
-opaque antidote_flag() :: #antidote_flag{}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-spec new() -> antidote_flag().
new() ->
    #antidote_flag{value=false}.


new(Value) ->
    #antidote_flag{value=Value}.

-spec value(antidote_flag()) -> [term()].
value(#antidote_flag{value=Value}) -> Value.

dirty_value(#antidote_flag{value=Value, new_value=undefined}) -> Value;
dirty_value(#antidote_flag{new_value=NewValue}) -> NewValue.

%% @doc Determines whether the passed term is a reg container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, antidote_flag).

%% @doc Returns the symbolic name of this container.
-spec type() -> flag.
type() -> flag.

to_ops(_, #antidote_flag{new_value=undefined}) ->
    [];

to_ops(BoundObject, #antidote_flag{new_value=true}) ->
    [{BoundObject, enable, {}}];

to_ops(BoundObject, #antidote_flag{new_value=false}) ->
    [{BoundObject, disable, {}}].

-spec assign(antidote_flag(), boolean())  -> antidote_flag().
assign(Flag, Value) ->
    Flag#antidote_flag{new_value=Value}.

-spec enable(antidote_flag())  -> antidote_flag().
enable(Flag) ->
    Flag#antidote_flag{new_value=true}.

-spec disable(antidote_flag())  -> antidote_flag().
disable(Flag) ->
    Flag#antidote_flag{new_value=false}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
-endif.
