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

-include("antidote_crdt.hrl").

-define(CRDTS, [counter_pn,
                counter_b,
                counter_fat,
                integer,
                flag_ew,
                flag_dw,
                set_go,
                set_aw,
                set_rw,
                register_lww,
                register_mv,
                map_go,
                map_aw,
                map_rr,
                rga]).

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
