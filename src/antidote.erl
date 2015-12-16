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
         ec_execute_tx/1,
         ec_read/2,
         ec_bulk_update/1,
         ec_istart_tx/0,
         ec_iread/3,
         ec_iupdate/4,
         ec_iprepare/1,
         ec_full_icommit/1,
         ec_icommit/1,
         does_certification_check/0]).
%% ===========================================================

-type txn_properties() :: term(). %% TODO: Define
-type op_param() :: term(). %% TODO: Define
-type bound_object() :: {key(), type(), bucket()}.

%% Public API

-spec start_transaction(Clock :: snapshot_time(), Properties :: txn_properties())
      -> {ok, txid()} | {error, reason()}.
start_transaction(_Clock, _Properties) ->
    ec_istart_tx().

-spec abort_transaction(TxId :: txid()) -> {error, reason()}.
abort_transaction(_TxId) ->
    %% TODO
    {error, operation_not_implemented}.

-spec commit_transaction(TxId :: txid()) ->
    {ok, snapshot_time()} | {error, reason()}.
commit_transaction(TxId) ->
    case ec_full_icommit(TxId) of
        {ok, {_TxId, CommitTime}} ->
            {ok, CommitTime};
        {error, Reason} ->
            {error, Reason};
        Other ->
            {error, Other}
    end.

-spec read_objects(Objects :: [bound_object()], TxId :: txid())
      -> {ok, [term()]} | {error, reason()}.
read_objects(Objects, TxId) ->
    %%TODO: Transaction co-ordinator handles multiple reads
    %% Executes each read as in a interactive transaction
    Results = lists:map(fun({Key, Type, _Bucket}) ->
        case ec_iread(TxId, Key, Type) of
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
            case ec_iupdate(TxId, Key, Type,
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
update_objects(_Clock, _Properties, Updates) ->
    Actor = actor, %% TODO: generate unique actors
    Operations = lists:map(
        fun({{Key, Type, _Bucket}, Op, OpParam}) ->
            {update, {Key, Type, {{Op, OpParam}, Actor}}}
        end,
        Updates),
    case ec_execute_tx(Operations) of
        {ok, {_TxId, [], CommitTime}} ->
            {ok, CommitTime};
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
-spec append(key(), type(), {op(), term()}) ->
    {ok, {txid(), [], snapshot_time()}} | {error, term()}.
append(Key, Type, {OpParams, Actor}) ->
    case ec_materializer:check_operations([{update,
        {Key, Type, {OpParams, Actor}}}]) of
        ok ->
            ec_interactive_tx_coord_fsm:
            perform_singleitem_update(Key, Type, {OpParams, Actor});
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc The read/2 function returns the current value for the CRDT
%%      object stored at some key.
-spec read(key(), type()) -> {ok, val()} | {error, reason()} | {error, {type_check, term()}}.
read(Key, Type) ->
    case ec_materializer:check_operations([{read, {Key, Type}}]) of
        ok ->
            {ok, Val} = ec_interactive_tx_coord_fsm:
                perform_singleitem_read(Key,Type),
            {ok, Val};
        {error, Reason} ->
            {error, Reason}
    end.


%% Clock SI API
%% TODO: Move these functions into ec files. Public interface should only
%%       contain generic transaction interface

%% @doc Starts a new ec transaction.
%%      Input:
%%      ClientClock: last clock the client has seen from a successful transaction.
%%      Operations: the list of the operations the transaction involves.
%%      Returns:
%%      an ok message along with the result of the read operations involved in the
%%      the transaction, in case the tx ends successfully.
%%      error message in case of a failure.
%%

-spec ec_execute_tx([client_op()]) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
ec_execute_tx(Operations) ->
    case ec_materializer:check_operations(Operations) of
        ok ->
            {ok, CoordFsmPid} = ec_static_tx_coord_sup:start_fsm([self(), Operations]),
            gen_fsm:sync_send_event(CoordFsmPid, execute);
        {error, Reason} ->
            {error, Reason}
    end.

-spec ec_bulk_update([client_op()]) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
ec_bulk_update(Operations) ->
    ec_execute_tx(Operations).

-spec ec_read(key(), type()) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
ec_read(Key, Type) ->
    ec_execute_tx([{read, {Key, Type}}]).


%% @doc Starts a new ec interactive transaction.
%%      Input:
%%      Returns: an ok message along with the new TxId.

ec_istart_tx() ->
    {ok, _} = ec_interactive_tx_coord_sup:start_fsm([self()]),
    receive
        {ok, TxId} ->
            {ok, TxId};
        Other ->
            {error, Other}
    end.

-spec ec_iread(txid(), key(), type()) -> {ok, term()} | {error, reason()}.
ec_iread({_, _, CoordFsmPid}, Key, Type) ->
    case ec_materializer:check_operations([{read, {Key, Type}}]) of
        ok ->
            case  gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}}, ?OP_TIMEOUT) of
                {ok, Res} -> {ok, Res};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec ec_iupdate(txid(), key(), type(), term()) -> ok | {error, reason()}.
ec_iupdate({_, _, CoordFsmPid}, Key, Type, OpParams) ->
    case ec_materializer:check_operations([{update, {Key, Type, OpParams}}]) of
        ok ->
            case gen_fsm:sync_send_event(CoordFsmPid,
                                         {update, {Key, Type, OpParams}}, ?OP_TIMEOUT) of
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
-spec ec_full_icommit(txid()) -> {aborted, txid()} | {ok, {txid(), snapshot_time()}} | {error, reason()}.
ec_full_icommit({_, _, CoordFsmPid}) ->
    case gen_fsm:sync_send_event(CoordFsmPid, {prepare, empty}, ?OP_TIMEOUT) of
        {ok, _PrepareTime} ->
            gen_fsm:sync_send_event(CoordFsmPid, commit, ?OP_TIMEOUT);
        Msg ->
            Msg
    end.

-spec ec_iprepare(txid()) -> {aborted, txid()} | {ok, non_neg_integer()}.
ec_iprepare({_, _, CoordFsmPid}) ->
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, two_phase}, ?OP_TIMEOUT).

-spec ec_icommit(txid()) -> {aborted, txid()} | {ok, {txid(), snapshot_time()}, ?OP_TIMEOUT}.
ec_icommit({_, _, CoordFsmPid}) ->
    gen_fsm:sync_send_event(CoordFsmPid, commit, ?OP_TIMEOUT).

-spec does_certification_check() -> boolean().
does_certification_check() ->
    case application:get_env(antidote, txn_cert) of
        {ok, true}
            -> true;
        _
            -> false
    end.