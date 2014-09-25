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
-module(floppy).

-export([append/3,
         read/2,
         clocksi_execute_tx/2,
         clocksi_execute_tx/1,
         clocksi_read/3,
         clocksi_bulk_update/2,
         clocksi_bulk_update/1,
         clocksi_istart_tx/1,
         clocksi_istart_tx/0,
         clocksi_iread/3,
         clocksi_iupdate/4,
         clocksi_iprepare/1,
         clocksi_icommit/1]).

%% Public API

%% @doc The append/2 function adds an operation to the log of the CRDT
%%      object stored at some key.
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
read(Key, Type) ->
    case clocksi_read(now(), Key, Type) of
        {ok,{_, [Val], _}} ->
            {ok, Val};
        {error, Reason} ->
            {error, Reason}
    end.

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
clocksi_execute_tx(Clock, Operations) ->
    lager:info("Received transaction with clock: ~p for operations: ~p"),
    ClientClock = case Clock of
        {Mega, Sec, Micro} ->
            clocksi_vnode:now_milisec({Mega, Sec, Micro});
        _ ->
            Clock
        end,    
    {ok, _} = clocksi_static_tx_coord_sup:start_fsm([self(), ClientClock, Operations]),
    receive
        EndOfTx ->
            EndOfTx
    end. 

clocksi_execute_tx(Operations) ->
    lager:info("Received transaction for operations: ~p", [Operations]),
    {ok, _} = clocksi_static_tx_coord_sup:start_fsm([self(), noclock, Operations]),
    receive
        EndOfTx ->
            EndOfTx
    end.

%% @doc Starts a new ClockSI interactive transaction.
%%      Input:
%%      ClientClock: last clock the client has seen from a successful transaction.
%%      Returns: an ok message along with the new TxId.
%%
clocksi_istart_tx(Clock) ->
    lager:info("Starting FSM for interactive transaction."),
    ClientClock = case Clock of
        {Mega, Sec, Micro} ->
            clocksi_vnode:now_milisec({Mega, Sec, Micro});
        _ ->
            Clock
    end,
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self(), ClientClock]),
    receive
        TxId ->
            lager:info("TX started with TxId: ~p", [TxId]),
            TxId
    after
        10000 ->
            lager:info("Tx was not started!"),
            {error, timeout}
    end.

clocksi_istart_tx() ->
    lager:info("Starting FSM for interactive transaction."),
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self()]),
    receive
        TxId ->
            lager:info("TX started with TxId: ~p", [TxId]),
            TxId
    after
        10000 ->
            lager:info("Tx was not started!"),
            {error, timeout}
    end.

clocksi_bulk_update(ClientClock, Operations) ->
    clocksi_execute_tx(ClientClock, Operations).

clocksi_bulk_update(Operations) ->
    clocksi_execute_tx(Operations).

clocksi_read(ClientClock, Key, Type) ->
    clocksi_execute_tx(ClientClock, [{read, Key, Type}]).

clocksi_iread({_, _, CoordFsmPid}, Key, Type) ->
    gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}}).

clocksi_iupdate({_, _, CoordFsmPid}, Key, Type, OpParams) ->
    gen_fsm:sync_send_event(CoordFsmPid, {update, {Key, Type, OpParams}}).

clocksi_iprepare({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, empty}).

clocksi_icommit({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, commit).
