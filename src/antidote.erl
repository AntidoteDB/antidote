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
-module(antidote).

-include("antidote.hrl").

-export([append/3,
         read/2,
         ec_execute_tx/1,
         ec_read/2,
         ec_bulk_update/1,
         ec_istart_tx/0,
         ec_iread/3,
         ec_iupdate/4,
         ec_iprepare/1,
         ec_full_icommit/1,
         ec_icommit/1]).

%% Public API

%% @doc The append/2 function adds an operation to the log of the CRDT
%%      object stored at some key.
-spec append(Key::key(), Type::type(), {term(),term()}) -> {ok, term()} | {error, reason()}.
append(Key, Type, {OpParam, Actor}) ->
    Operations = [{update, Key, Type, {OpParam, Actor}}],
    case ec_execute_tx(Operations) of
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc The read/2 function returns the current value for the CRDT
%%      object stored at some key.
-spec read(Key::key(), Type::type()) -> {ok, val()} | {error, reason()}.
read(Key, Type) ->
    case ec_read(Key, Type) of
        {ok,{_, [Val]}} ->
            {ok, Val};
        {error, Reason} ->
            {error, Reason}
    end.

%% EC-Antidote API

%% @doc Starts a new EC transaction.
%%      Input:
%%      Operations: the list of the operations the transaction involves.
%%      Returns:
%%      an ok message along with the result of the read operations involved in the
%%      the transaction, in case the tx ends successfully.
%%      error message in case of a failure.
%%
-spec ec_execute_tx(Operations::[any()]) -> term().
ec_execute_tx(Operations) ->
    ok = ec_tx_coord_server:run([self(), Operations]),
    receive
        EndOfTx ->
            EndOfTx
    end.

%% @doc Starts a new EC interactive transaction.
%%      Returns: an ok message along with the new TxId.
%%
-spec ec_istart_tx() -> term().
ec_istart_tx() ->
    {ok, _} = ec_interactive_tx_coord_sup:start_fsm([self()]),
    receive
        TxId ->
            TxId
    end.

-spec ec_bulk_update(Operations :: [any()]) -> term().
ec_bulk_update(Operations) ->
    ec_execute_tx(Operations).

-spec ec_read(Key :: key(), Type:: type()) -> term().
ec_read(Key, Type) ->
    ec_execute_tx([{read, Key, Type}]).

ec_iread({_, _, CoordFsmPid}, Key, Type) ->
    gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}}).

ec_iupdate({_, _, CoordFsmPid}, Key, Type, OpParams) ->
    gen_fsm:sync_send_event(CoordFsmPid, {update, {Key, Type, OpParams}}).

%% @doc This commits includes both prepare and commit phase. Thus
%%      Client do not need to send to message to complete the 2PC
%%      protocol. The Tx coordinator will pick the best strategie
%%      automatically.
ec_full_icommit({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, empty}).
    
ec_iprepare({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, two_phase}).

ec_icommit({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, commit).
