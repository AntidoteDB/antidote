%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% antidote_crdt.erl : behaviour for op-based CRDTs
%% Naming pattern of antidote crdts: <type>_<semantics>
%% if there is only one kind of semantics implemented for a certain type
%% only the type is used in the name e.g. rga
%% counter_pn: PN-Counter aka Positive Negative Counter
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
%% map_rr: Recursive Resets Map aka RR-Map
%% rga: Replicated Growable Array (Experimental)



-module(antidote_crdt).

-include("antidote_crdt.hrl").

-export([to_binary/1, from_binary/1, dict_to_orddict/1]).

% The CRDTs supported by Antidote:
-type typ() ::
antidote_crdt_counter_pn
| antidote_crdt_counter_b
| antidote_crdt_counter_fat
| antidote_crdt_secure_counter_pn
| antidote_crdt_flag_ew
| antidote_crdt_flag_dw
| antidote_crdt_set_go
| antidote_crdt_set_aw
| antidote_crdt_set_rw
| antidote_crdt_register_lww
| antidote_crdt_register_mv
| antidote_crdt_map_go
| antidote_crdt_map_rr.

-type internal_crdt() :: term().
-type internal_effect() :: term().

-export([is_type/1, alias/1, new/1, value/2, downstream/3, update/3, require_state_downstream/2, is_operation/2]).

% Callbacks implemented by each concrete CRDT implementation
-callback new() -> internal_crdt().
-callback value(internal_crdt()) -> value().
-callback downstream(update(), internal_crdt()) -> {ok, internal_effect()} | {error, reason()}.
-callback update(internal_effect(), internal_crdt()) -> {ok, internal_crdt()}.
-callback require_state_downstream(update()) -> boolean().
-callback is_operation(update()) -> boolean(). %% Type check

-callback equal(internal_crdt(), internal_crdt()) -> boolean().
-callback to_binary(internal_crdt()) -> binary().
-callback from_binary(binary()) -> {ok, internal_crdt()} | {error, reason()}.

% Check if the given type is supported by Antidote
-spec is_type(typ()) -> boolean().
is_type(antidote_crdt_counter_pn)          -> true;
is_type(antidote_crdt_counter_b)           -> true;
is_type(antidote_crdt_counter_fat)         -> true;
is_type(antidote_crdt_flag_ew)             -> true;
is_type(antidote_crdt_flag_dw)             -> true;
is_type(antidote_crdt_set_go)              -> true;
is_type(antidote_crdt_set_aw)              -> true;
is_type(antidote_crdt_set_rw)              -> true;
is_type(antidote_crdt_register_lww)        -> true;
is_type(antidote_crdt_register_mv)         -> true;
is_type(antidote_crdt_map_go)              -> true;
is_type(antidote_crdt_map_rr)              -> true;
is_type(antidote_crdt_secure_counter_pn)   -> true;
is_type(antidote_crdt_secure_set_go)       -> true;
is_type(antidote_crdt_secure_set_aw)       -> true;
is_type(antidote_crdt_secure_set_rw)       -> true;
is_type(antidote_crdt_secure_register_lww) -> true;
is_type(antidote_crdt_secure_register_mv)  -> true;
is_type(antidote_crdt_secure_map_go)       -> true;
is_type(antidote_crdt_secure_map_rr)       -> true;
is_type(_)                                 -> false.

% Makes it possible to map multiple CRDT types to one CRDT implementation.
-spec alias(typ()) -> typ().
alias(antidote_crdt_secure_set_go)       -> antidote_crdt_set_go;
alias(antidote_crdt_secure_set_aw)       -> antidote_crdt_set_aw;
alias(antidote_crdt_secure_set_rw)       -> antidote_crdt_set_rw;
alias(antidote_crdt_secure_register_lww) -> antidote_crdt_register_lww;
alias(antidote_crdt_secure_register_mv)  -> antidote_crdt_register_mv;
alias(antidote_crdt_secure_map_go)       -> antidote_crdt_map_go;
alias(antidote_crdt_secure_map_rr)       -> antidote_crdt_map_rr;
alias(Type)                              -> Type.

% Returns the initial CRDT state for the given Type
-spec new(typ()) -> crdt().
new(Type) ->
    T = antidote_crdt:alias(Type),
    true = is_type(T),
    T:new().

% Reads the value from a CRDT state
-spec value(typ(), crdt()) -> any().
value(Type, State) ->
    T = antidote_crdt:alias(Type),
    true = is_type(T),
    T:value(State).

% Computes the downstream effect for a given update operation and current state.
% This has to be called once at the source replica.
% The effect must then be applied on all replicas using the update function.
% For some update operation it is not necessary to provide the current state
% and the atom 'ignore' can be passed instead (see function require_state_downstream).
-spec downstream(typ(), update(), crdt() | ignore) -> {ok, effect()} | {error, reason()}.
downstream(Type, Update, State) ->
    T = antidote_crdt:alias(Type),
    true = is_type(T),
    true = T:is_operation(Update),
    T:downstream(Update, State).

% Updates the state of a CRDT by applying a downstream effect calculated
% using the downstream function.
% For most types the update function must be called in causal order:
% if Eff2 was calculated on a state where Eff1 was already replied,
% then Eff1 has to be applied before Eff2 on all replicas.
-spec update(typ(), effect(), crdt()) -> {ok, crdt()}.
update(Type, Effect, State) ->
    T = antidote_crdt:alias(Type),
    true = is_type(T),
    T:update(Effect, State).

% Checks whether the current state is required by the downstream function
% for a specific type and update operation
-spec require_state_downstream(typ(), update()) -> boolean().
require_state_downstream(Type, Update) ->
    T = antidote_crdt:alias(Type),
    true = is_type(T),
    T:require_state_downstream(Update).

% Checks whether the given update operation is valid for the given type
-spec is_operation(typ(), update()) -> boolean().
is_operation(Type, Update) ->
    T = antidote_crdt:alias(Type),
    true = is_type(T),
    T:is_operation(Update).

-spec to_binary(crdt()) -> binary().
to_binary(Term) ->
    Opts = case application:get_env(antidote_crdt, binary_compression, 1) of
               true -> [compressed];
               N when N >= 0, N =< 9 -> [{compressed, N}];
               _ -> []
           end,
    term_to_binary(Term, Opts).

-spec from_binary(binary()) -> crdt().
from_binary(Binary) ->
    binary_to_term(Binary).


%% turns a dict into a sorted list of [{key, value}]
-spec dict_to_orddict(dict:dict()) -> orddict:orddict().
dict_to_orddict(Dict) ->
    lists:sort(dict:to_list(Dict)).
