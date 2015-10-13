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

%%@doc This file is the public api of antidote

-module(antidote).

-include("antidote.hrl").

%% API for applications
-export([
         start_transaction/2,
         read_objects/2,
         read_objects/3,
         update_objects/2,
         update_objects/3,
         abort_transaction/1,
         commit_transaction/1,
         create_bucket/2,
         create_object/3,
         delete_object/1
        ]).

%% ==========================================================
%% Old APIs, We would still need them for tests and benchmarks
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
%% ===========================================================

-type txn_properties() :: term(). %% TODO: Define
-type op_param() :: term(). %% TODO: Define
-type bound_object() :: {key(), type(), bucket()}.

%% Public API

-spec start_transaction(Clock::snapshot_time(), Properties::txn_properties())
                       -> {ok, txid()} | {error, reason()}.
start_transaction(Clock, _Properties) ->
    clocksi_istart_tx(Clock).

-spec abort_transaction(TxId::txid()) -> {error, reason()}.
abort_transaction(_TxId) ->
    %% TODO
    {error, operation_not_implemented}.

-spec commit_transaction(TxId::txid()) ->
                                {ok, snapshot_time()} | {error, reason()}.
commit_transaction(TxId) ->
    case clocksi_full_icommit(TxId) of
        {ok, {_TxId, CommitTime}} ->
            {ok, CommitTime};
        {error, Reason} ->
            {error, Reason}
    end.

-spec read_objects(Objects::[bound_object()], TxId::txid())
                  -> {ok, [term()]} | {error, reason()}.
read_objects(Objects, TxId) ->
    %%TODO: Transaction co-ordinator handles multiple reads
    %% Executes each read as in a interactive transaction
    Results = lists:map(fun({Key, Type, _Bucket}) ->
                                case clocksi_iread(TxId, Key, Type) of
                                    {ok, Res} ->
                                        Res;
                                    {error, _Reason} ->
                                        error
                                end
                        end, Objects),
    case lists:member(error, Results) of
        true -> {error, read_failed}; %% TODO: Capture the reason for error
        false -> {ok, Results}
    end.

-spec update_objects([{bound_object(), op(), op_param()}], txid())
                    -> ok | {error, reason()}.
update_objects(Updates, TxId) ->
    %% TODO: How to generate Actor,
    %% Actor ID must be removed from crdt update interface
    Actor = TxId,
    %% Execute each update as in an interactive transaction
    Results = lists:map(
                fun({{Key, Type, _Bucket}, Op, OpParam}) ->
                        case clocksi_iupdate(TxId, Key, Type,
                                             {{Op, OpParam}, Actor}) of
                            ok -> ok;
                            {error, _Reason} ->
                                error
                        end
                end, Updates),
    case lists:member(error, Results) of
        true -> {error, read_failed}; %% TODO: Capture the reason for error
        false -> ok
    end.

%% For static transactions: bulk updates and bulk reads
-spec update_objects(snapshot_time(), term(), [{bound_object(), op(), op_param()}]) ->
                            {ok, snapshot_time()} | {error, reason()}.
update_objects(Clock, _Properties, Updates) ->
    Actor = actor, %% TODO: generate unique actors
    Operations = lists:map(
                   fun({{Key, Type, _Bucket}, Op, OpParam}) ->
                           {update, {Key, Type, {{Op,OpParam}, Actor}}}
                   end,
                   Updates),
    case clocksi_execute_tx(Clock, Operations) of
        {ok, {_TxId, [], CommitTime}} ->
            {ok, CommitTime};
        {error, Reason} -> {error, Reason}
    end.

read_objects(Clock, _Properties, Objects) ->
    Args = lists:map(
             fun({Key, Type, _Bucket}) ->
                     {read, {Key, Type}}
             end,
             Objects),
    case clocksi_execute_tx(Clock, Args) of
        {ok, {_TxId, Result, CommitTime}} ->
            {ok, Result, CommitTime};
        {error, Reason} -> {error, Reason}
    end.


%% Object creation and types

create_bucket(_Bucket, _Type) ->
    %% TODO: Bucket is not currently supported
    {error, operation_not_supported}.

create_object(_Key, _Type, _Bucket) ->
    %% TODO: Object creation is not currently supported
    {error, operation_not_supported}.

delete_object({_Key, _Type, _Bucket}) ->
    %% TODO: Object deletion is not currently supported
    {error, operation_not_supported}.

%% =============================================================================
%% OLD API, We might still need them

%% @doc The append/2 function adds an operation to the log of the CRDT
%%      object stored at some key.
-spec append(key(), type(), {op(),term()}) -> 
                    {ok, {txid(), [], snapshot_time()}} | {error, term()}.
append(Key, Type, {OpParams, Actor}) ->
    case materializer:check_operations([{update,
                                         {Key, Type, {OpParams, Actor}}}]) of
        ok ->
            clocksi_interactive_tx_coord_fsm:
                perform_singleitem_update(Key, Type,{OpParams,Actor});
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
%% TODO: Move these functions into clocksi files. Public interface should only 
%%       contain generic transaction interface

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


%% @doc Starts a new ClockSI interactive transaction.
%%      Input:
%%      ClientClock: last clock the client has seen from a successful transaction.
%%      Returns: an ok message along with the new TxId.
%%
-spec clocksi_istart_tx(Clock:: snapshot_time()) ->
                               {ok, txid()} | {error, reason()}.
clocksi_istart_tx(Clock) ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self(), Clock]),
    receive
        {ok, TxId} ->
            {ok, TxId};
        Other ->
            {error, Other}
    end.

clocksi_istart_tx() ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self()]),
    receive
        {ok, TxId} ->
            {ok, TxId};
        Other ->
            {error, Other}
    end.
    

-spec clocksi_iread(txid(), key(), type()) -> {ok, term()} | {error, reason()}.
clocksi_iread({_, _, CoordFsmPid}, Key, Type) ->
    case materializer:check_operations([{read, {Key, Type}}]) of
        ok ->
            case  gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}}) of
                {ok, Res} -> {ok, Res};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec clocksi_iupdate(txid(), key(), type(), term()) -> ok | {error, reason()}.
clocksi_iupdate({_, _, CoordFsmPid}, Key, Type, OpParams) ->
    case materializer:check_operations([{update, {Key, Type, OpParams}}]) of
        ok ->
            case gen_fsm:sync_send_event(CoordFsmPid,
                                         {update, {Key, Type, OpParams}}) of
                ok -> ok;
                {aborted, _} -> {error, aborted};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc This commits includes both prepare and commit phase. Thus
%%      Client do not need to send to message to complete the 2PC
%%      protocol. The Tx coordinator will pick the best strategie
%%      automatically.
%%      To keep with the current api this is still done in 2 steps,
%%      but should be changed when the new transaction api is decided
-spec clocksi_full_icommit(txid()) -> {aborted, txid()} | {ok, {txid(), snapshot_time()}} | {error, reason()}.
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
