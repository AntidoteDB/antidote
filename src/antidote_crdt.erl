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

%% antidote_crdt.erl : behaviour for op-based CRDTs


-module(antidote_crdt).

-include("antidote_crdt.hrl").

-define(CRDTS, [antidote_crdt_counter,
                antidote_crdt_orset,
                antidote_crdt_gset,
                antidote_crdt_rga,
                antidote_crdt_bcounter,
                antidote_crdt_mvreg,
                antidote_crdt_map,
                antidote_crdt_lwwreg,
                antidote_crdt_gmap,
                antidote_crdt_set_rw,
                antidote_crdt_integer,
                antidote_crdt_map_aw,
                antidote_crdt_map_rr,
                antidote_crdt_fat_counter,
                antidote_crdt_flag_ew,
                antidote_crdt_flag_dw
               ]).

-export([is_type/1
        ]).

-callback new() -> crdt().
-callback value(crdt()) -> value().
-callback downstream(update(), crdt()) -> {ok, effect()} | {error, reason()}.
-callback update(effect(), crdt()) ->  {ok, crdt()}.
-callback require_state_downstream(update()) -> boolean().
-callback is_operation(update()) ->  boolean(). %% Type check

-callback equal(crdt(), crdt()) -> boolean().
-callback to_binary(crdt()) -> binary().
-callback from_binary(binary()) -> {ok, crdt()} | {error, reason()}.

%% Following callbacks taken from riak_dt
%% Not sure if it is useful for antidote
%-callback stats(crdt()) -> [{atom(), number()}].
%-callback stat(atom(), crdt()) ->  number() | undefined.

is_type(Type) ->
    is_atom(Type) andalso lists:member(Type, ?CRDTS).

%% End of Module.
