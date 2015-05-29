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
         clocksi_execute_tx/2,
         clocksi_execute_tx/1,
         clocksi_read/3,
         clocksi_read/2,
         clocksi_bulk_update/2,
         clocksi_bulk_update/1,
         clocksi_istart_tx/1,
         clocksi_istart_tx/0,
         clocksi_iread/3,
         clocksi_iupdate/4,
         clocksi_iprepare/1,
         clocksi_full_icommit/1,
         clocksi_icommit/1]).

%% Public API

%% @doc The append/2 function adds an operation to the log of the CRDT
%%      object stored at some key.
-spec append(Key::key(), Type::type(), {term(),term()}) -> {ok, term()} | {error, reason()}.
append(Key, Type, {OpParam, Actor}) ->
    Operations = [{update, Key, Type, {OpParam, Actor}}],
    case clocksi_execute_tx(Operations) of
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc The read/2 function returns the current value for the CRDT
%%      object stored at some key.
-spec read(Key::key(), Type::type()) -> {ok, val()} | {error, reason()}.
read(Key, Type) ->
    clocksi_interactive_tx_coord_fsm:perform_singleitem_read(Key,Type).

%% Clock SI API

%% @doc Starts a new ClockSI transaction.
%%      Input:
%%      ClientClock: last clock the client has seen from a successful transaction.
%%      Operations: the list of the operations the transaction involves.
%%      Returns:
%%      an ok message along with the result of the read operations involved in the
%%      the transaction, in case the tx ends successfully.
%%      error message in case of a failure.
%%
-spec clocksi_execute_tx(Clock :: snapshot_time(),
                         Operations::[any()]) -> term().
clocksi_execute_tx(Clock, Operations) ->
    {ok, _} = clocksi_static_tx_coord_sup:start_fsm([self(), Clock, Operations]),
    receive
        EndOfTx ->
            EndOfTx
    end.

-spec clocksi_execute_tx(Operations::[any()]) -> term().
clocksi_execute_tx(Operations) ->
    {ok, _} = clocksi_static_tx_coord_sup:start_fsm([self(), Operations]),
    receive
        EndOfTx ->
            EndOfTx
    end.

%% @doc Starts a new ClockSI interactive transaction.
%%      Input:
%%      ClientClock: last clock the client has seen from a successful transaction.
%%      Returns: an ok message along with the new TxId.
%%
-spec clocksi_istart_tx(Clock:: snapshot_time()) -> term().
clocksi_istart_tx(Clock) ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self(), Clock]),
    receive
        TxId ->
            TxId
    end.

clocksi_istart_tx() ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self()]),
    receive
        TxId ->
            TxId
    end.

-spec clocksi_bulk_update(ClientClock:: snapshot_time(),
                          Operations::[any()]) -> term().
clocksi_bulk_update(ClientClock, Operations) ->
    clocksi_execute_tx(ClientClock, Operations).

-spec clocksi_bulk_update(Operations :: [any()]) -> term().
clocksi_bulk_update(Operations) ->
    clocksi_execute_tx(Operations).

-spec clocksi_read(ClientClock :: snapshot_time(),
                   Key :: key(), Type:: type()) -> term().
clocksi_read(ClientClock, Key, Type) ->
    clocksi_execute_tx(ClientClock, [{read, Key, Type}]).

clocksi_read(Key, Type) ->
    clocksi_execute_tx([{read, Key, Type}]).

clocksi_iread({_, _, CoordFsmPid}, Key, Type) ->
    gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}}).

clocksi_iupdate({_, _, CoordFsmPid}, Key, Type, OpParams) ->
    gen_fsm:sync_send_event(CoordFsmPid, {update, {Key, Type, OpParams}}).

%% @doc This commits includes both prepare and commit phase. Thus
%%      Client do not need to send to message to complete the 2PC
%%      protocol. The Tx coordinator will pick the best strategie
%%      automatically.
clocksi_full_icommit({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, empty}).
    
clocksi_iprepare({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, two_phase}).

clocksi_icommit({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, commit).
