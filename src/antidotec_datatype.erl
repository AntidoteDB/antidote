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
-module(antidotec_datatype).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(QC_OUT(P), eqc:on_output(fun(Fmt, Args) -> io:format(user, Fmt, Args) end, P)).
-compile(export_all).
-endif.


-define(MODULES, [antidotec_counter, antidotec_set]).

-export([module_for_type/1,
         module_for_term/1]).

-export_type([datatype/0, update/0]).

-type maybe(T) :: T | undefined.
-type datatype() :: term().
-type typename() :: atom().
-type update() :: [term()].

%% @doc Constructs a new container for the type with the specified
%% value and key. This should only be used internally by the client code.
-callback new(Key::integer(), Value::term()) -> datatype().

%% @doc Returns the original, unmodified value of the object. This does
%% not include the execution of any locally-queued operations.
-callback value(datatype()) -> term().

%% @doc Returns the local value of the object, with the local operations applied.
-callback dirty_value(datatype()) -> term().

%% @doc Returns the message to get an object of the type of this container.
-callback message_for_get(binary()) -> term().

%% @doc Extracts the list of operations to be append to the object's log.
%% 'undefined' should be returned if the type is unmodified.
-callback to_ops(datatype()) -> update().

%% @doc Determines whether the given term is the type managed by the
%% container module.
-callback is_type(datatype()) -> boolean().

%% @doc Determines the symbolic name of the container's type, e.g.
%% antidote_set, antidote_map, antidote_counter.
-callback type() -> typename().

%% @doc Returns the module name for the container of the given CRDT data-type.
-spec module_for_type(riak_dt_orset | riak_dt_pncounter) ->
    antidotec_counter | antidotec_set.
module_for_type(riak_dt_orset) -> antidotec_set;
module_for_type(riak_dt_pncounter)  -> antidotec_counter.

%% @doc Returns the container module name for the given term. 
%% Returns undefined if the module is not known.
-spec module_for_term(datatype()) -> maybe(module()).
module_for_term(T) ->
    lists:foldl(fun(Mod, undefined) ->
                        case Mod:is_type(T) of
                            true -> Mod;
                            false -> undefined
                        end;
                   (_, Mod) ->
                        Mod
                end, undefined, ?MODULES).
