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
         eiger_readtx/1,
         eiger_updatetx/2,
         eiger_updatetx/3,
         eiger_committx/1,
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


eiger_readtx(KeysType) ->
    case KeysType of
        [] ->
            {ok, empty};
        _List ->
            {ok, _} = eiger_readtx_coord_sup:start_fsm([self(), KeysType]),
            receive
                EndOfTx ->
                    EndOfTx
            end
    end.

eiger_updatetx(Updates, _Dependencies) ->
    eiger_updatetx(Updates, _Dependencies, undefined).

eiger_updatetx(Updates, _Dependencies, Debug) ->
    case Updates of
        [{Key, _Type, _Param}|_Rest] ->
            Preflist = log_utilities:get_preflist_from_key(Key),
            IndexNode = hd(Preflist),
            eiger_vnode:coordinate_tx(IndexNode, Updates, Debug);
        [] ->
            {ok, empty};
        _ ->
            {error, bad_format}
    end.

eiger_committx(Coordinator) ->
    gen_fsm:sync_send_event(Coordinator, commit).

%% @doc The append/2 function adds an operation to the log of the CRDT
%%      object stored at some key.
-spec append(key(), type(), {op(),term()}) -> {ok, {txid(), [], snapshot_time()}} | {error, term()}.
append(Key, Type, {OpParams, Actor}) ->
    case materializer:check_operations([{update, {Key, Type, {OpParams, Actor}}}]) of
        ok ->
            clocksi_interactive_tx_coord_fsm:perform_singleitem_update(Key,
                Type,{OpParams,Actor});
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc The read/2 function returns the current value for the CRDT
%%      object stored at some key.
-spec read(key(), type()) -> {ok, val()} | {error, reason()}.
read(Key, Type) ->
    case materializer:check_operations([{read, {Key, Type}}]) of
        ok ->
            clocksi_interactive_tx_coord_fsm:perform_singleitem_read(Key,Type);
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
-spec clocksi_execute_tx(Clock :: snapshot_time(),
                         [client_op()]) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
clocksi_execute_tx(Clock, Operations) ->
    {ok, CoordFsmPid} = clocksi_static_tx_coord_sup:start_fsm([self(), Clock, Operations]),
    gen_fsm:sync_send_event(CoordFsmPid, execute).

-spec clocksi_execute_tx([client_op()]) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
clocksi_execute_tx(Operations) ->
    case materializer:check_operations(Operations) of
        ok ->
            {ok, CoordFsmPid} = clocksi_static_tx_coord_sup:start_fsm([self(), Operations]),
            gen_fsm:sync_send_event(CoordFsmPid, execute);
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc Starts a new ClockSI interactive transaction.
%%      Input:
%%      ClientClock: last clock the client has seen from a successful transaction.
%%      Returns: an ok message along with the new TxId.
%%
-spec clocksi_istart_tx(Clock:: snapshot_time()) -> txid().
clocksi_istart_tx(Clock) ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self(), Clock]),
    receive
        TxId ->
            TxId
    end.

-spec clocksi_istart_tx() -> txid().
clocksi_istart_tx() ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self()]),
    receive
        TxId ->
            TxId
    end.

-spec clocksi_bulk_update(ClientClock:: snapshot_time(),
                          [client_op()]) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
clocksi_bulk_update(ClientClock, Operations) ->
    clocksi_execute_tx(ClientClock, Operations).

-spec clocksi_bulk_update([client_op()]) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
clocksi_bulk_update(Operations) ->
    clocksi_execute_tx(Operations).

-spec clocksi_read(ClientClock :: snapshot_time(),
                   Key :: key(), Type:: type()) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
clocksi_read(ClientClock, Key, Type) ->
    clocksi_execute_tx(ClientClock, [{read, {Key, Type}}]).

-spec clocksi_read(key(), type()) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
clocksi_read(Key, Type) ->
    clocksi_execute_tx([{read, {Key, Type}}]).

-spec clocksi_iread(txid(),key(),type()) -> {ok, snapshot()} | {error, term()}.
clocksi_iread({_, _, CoordFsmPid}, Key, Type) ->
    case materializer:check_operations([{read, {Key, Type}}]) of
        ok ->
            gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}});
        {error, Reason} ->
            {error, Reason}
    end.


-spec clocksi_iupdate(txid(),key(),type(),op()) -> ok | {error, term()}.
clocksi_iupdate({_, _, CoordFsmPid}, Key, Type, OpParams) ->
    case materializer:check_operations([{update, {Key, Type, OpParams}}]) of
        ok ->
            gen_fsm:sync_send_event(CoordFsmPid, {update, {Key, Type,
                OpParams}});
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc This commits includes both prepare and commit phase. Thus
%%      Client do not need to send to message to complete the 2PC
%%      protocol. The Tx coordinator will pick the best strategie
%%      automatically.
%%      To keep with the current api this is still done in 2 steps,
%%      but should be changed when the new transaction api is decided
-spec clocksi_full_icommit(txid()) -> {aborted, txid()} | {ok, {txid(), snapshot_time()}}.
clocksi_full_icommit({_, _, CoordFsmPid})->
    case gen_fsm:sync_send_event(CoordFsmPid, {prepare, empty}) of
	{ok,_PrepareTime} ->
	    gen_fsm:sync_send_event(CoordFsmPid, commit);
	Msg ->
	    Msg
    end.

-spec clocksi_iprepare(txid()) -> {aborted, txid()} | {ok, non_neg_integer()}.
clocksi_iprepare({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, two_phase}).

-spec clocksi_icommit(txid()) -> {aborted, txid()} | {ok, {txid(), snapshot_time()}}.
clocksi_icommit({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, commit).
