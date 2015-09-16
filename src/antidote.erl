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

-export([
         start_transaction/2,
         %start_transaction/1,
         read_objects/2,
         update_objects/2,
         abort_transaction/1,
         commit_transaction/1
        ]).

-type bucket() :: term().
-type txn_properties() :: term(). %% TODO: Define
-type op_param() :: term(). %% TODO: Define
-type bound_object() :: {key(), type(), bucket()}.

%% Public API

-spec start_transaction(Clock::snapshot_time(), Properties::txn_properties())
                       -> {ok, txid()}.
start_transaction(Clock, _Properties) ->
    clocksi_istart_tx(Clock).

-spec abort_transaction(TxId::txid()) -> ok.
abort_transaction(_TxId) ->
    %% TODO
    ok.

-spec commit_transaction(TxId::txid()) ->
                                {ok, snapshot_time()} | {error, reason()}.
commit_transaction(TxId) ->
    clocksi_iprepare(TxId),
    {ok, {_TxId, CommitTime}} = clocksi_icommit(TxId),
    {ok, CommitTime}.

-spec read_objects(Objects::[bound_object()], TxId::txid())
                  -> {ok, [term()]} | {error, reason()}.
read_objects(Objects, TxId) ->
    %%TODO: Transaction co-ordinator handles multiple reads
    %% Executes each read as in a interactive transaction
    Results = lists:map(fun({Key, Type, _Bucket}) ->
                      {ok, Res} = clocksi_iread(TxId, Key, Type),
                      Res end, Objects),
    {ok, Results}.

-spec update_objects([{bound_object(), op(), op_param()}], txid())
                    -> ok | {error, reason()}.
update_objects(Updates, TxId) ->
    %% TODO: How to generate Actor, 
    %% Actor ID must be removed from crdt update interface
    Actor = TxId,
    %% Execute each update as in an interactive transaction
    lists:map(fun({{Key, Type, _Bucket}, Op, OpParam}) ->
                      ok = clocksi_iupdate(TxId, Key, Type, {{Op, OpParam}, Actor}),
                      ok end, Updates),
    ok.

%% Clock SI API

%% @doc Starts a new ClockSI interactive transaction.
%%      Input:
%%      ClientClock: last clock the client has seen from a successful transaction.
%%      Returns: an ok message along with the new TxId.
%%
-spec clocksi_istart_tx(Clock:: snapshot_time()) -> term().
clocksi_istart_tx(Clock) ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self(), Clock]),
    receive
        {ok, TxId} ->
            {ok, TxId};
        Other ->
            {error, Other}
    end.

clocksi_iread({_, _, CoordFsmPid}, Key, Type) ->
    gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}}).

clocksi_iupdate({_, _, CoordFsmPid}, Key, Type, OpParams) ->
    gen_fsm:sync_send_event(CoordFsmPid, {update, {Key, Type, OpParams}}).

clocksi_iprepare({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, empty}).

clocksi_icommit({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, commit).
