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
%% Naming pattern of antidote crdts: <type>_<semantics>
%% if there is only one kind of semantics implemented for a certain type
%% only the type is used in the name e.g. rga
%% counter_pn: PN-Counter aka Posistive Negative Counter
%% counter_b: Bounded Counter
%% counter_fat: Fat Counter
%% integer: Integer (Experimental)
%% flag_ew: Enable Wins Flag aka EW-Flag
%% flag_dw: Disable Wins Flag DW-Flag
%% set_go: Grow Only Set aka G-Set
%% set_aw: Add Wins Set aka AW-Set, previously OR-Set (Observed Remove Set)
%% set_rw: Remove Wins Set aka RW-Set
%% register_lww: Last Writer Wins Register aka LWW-Reg
%% register_mv: MultiValue Register aka MV-Reg
%% map_go: Grow Only Map aka G-Map
%% map_aw: Add Wins Map aka AW-Map (Experimental)
%% map_rr: Recursive Resets Map akak RR-Map
%% rga: Replicated Growable Array (Experimental)



-module(antidote_crdt).



-type typ() ::
antidote_crdt_counter_pn
| antidote_crdt_counter_b
| antidote_crdt_counter_fat
| antidote_crdt_flag_ew
| antidote_crdt_flag_dw
| antidote_crdt_set_go
| antidote_crdt_set_aw
| antidote_crdt_set_rw
| antidote_crdt_register_lww
| antidote_crdt_register_mv
| antidote_crdt_map_go
| antidote_crdt_map_rr.

% these types are not correct, the tags just help to find errors
-opaque crdt() :: {antidote_crdt, state, term()}.
-opaque effect() :: {antidote_crdt, effect, term()}.
-type update() :: {atom(), term()}.
-type value() :: term().
-type reason() :: term().

-export_type([
  crdt/0,
  update/0,
  effect/0,
  value/0,
  typ/0
]).


-type internal_crdt() :: term().
-type internal_effect() :: term().

-export([is_type/1, new/1, value/2, downstream/3, update/3, require_state_downstream/2, is_operation/2]).

-callback new() -> internal_crdt().
-callback value(internal_crdt()) -> value().
-callback downstream(update(), internal_crdt()) -> {ok, internal_effect()} | {error, reason()}.
-callback update(internal_effect(), internal_crdt()) -> {ok, internal_crdt()}.
-callback require_state_downstream(update()) -> boolean().
-callback is_operation(update()) -> boolean(). %% Type check

-callback equal(internal_crdt(), internal_crdt()) -> boolean().
-callback to_binary(internal_crdt()) -> binary().
-callback from_binary(binary()) -> {ok, internal_crdt()} | {error, reason()}.

-spec is_type(typ()) -> boolean().
is_type(antidote_crdt_counter_pn) -> true;
is_type(antidote_crdt_counter_b) -> true;
is_type(antidote_crdt_counter_fat) -> true;
is_type(antidote_crdt_flag_ew) -> true;
is_type(antidote_crdt_flag_dw) -> true;
is_type(antidote_crdt_set_go) -> true;
is_type(antidote_crdt_set_aw) -> true;
is_type(antidote_crdt_set_rw) -> true;
is_type(antidote_crdt_register_lww) -> true;
is_type(antidote_crdt_register_mv) -> true;
is_type(antidote_crdt_map_go) -> true;
is_type(antidote_crdt_map_rr) -> true;
is_type(_) -> false.

-spec new(typ()) -> crdt().
new(Type) ->
  true = is_type(Type),
  Type:new().

-spec value(typ(),crdt()) -> any().
value(Type, State) ->
  true = is_type(Type),
  Type:value(State).

-spec downstream(typ(), update(), crdt()) -> {ok, effect()} | {error, reason()}.
downstream(Type, Update, State) ->
  true = is_type(Type),
  true = Type:is_operation(Update),
  Type:downstream(Update, State).

-spec update(typ(), effect(), crdt()) -> {ok, crdt()}.
update(Type, Effect, State) ->
  true = is_type(Type),
  Type:update(Effect, State).

-spec require_state_downstream(typ(), update()) -> boolean().
require_state_downstream(Type, Update) ->
  true = is_type(Type),
  Type:require_state_downstream(Update).

-spec is_operation(typ(), update()) -> boolean().
is_operation(Type, Update) ->
  true = is_type(Type),
  Type:is_operation(Update).
