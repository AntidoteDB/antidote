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
-export([ start/0, stop/0,
         start_transaction/2,
         start_transaction/3,
         read_objects/2,
         read_objects/3,
         read_objects/4,
         update_objects/2,
         update_objects/3,
         update_objects/4,
         abort_transaction/1,
         commit_transaction/1,
         create_bucket/2,
         create_object/3,
         delete_object/1,
         register_pre_hook/3,
         register_post_hook/3,
         unregister_hook/2
        ]).

%% ==========================================================
%% Old APIs, We would still need them for tests and benchmarks
-export([append/3,
         read/2,
         clocksi_execute_tx/4,
         clocksi_execute_tx/3,
         clocksi_execute_tx/2,
         clocksi_execute_tx/1,
         clocksi_execute_int_tx/1,
         clocksi_read/3,
         clocksi_read/2,
         clocksi_bulk_update/2,
         clocksi_bulk_update/1,
         clocksi_istart_tx/2,
         clocksi_istart_tx/1,
         clocksi_istart_tx/0,
         clocksi_iread/3,
         clocksi_iupdate/4,
         clocksi_iprepare/1,
         clocksi_full_icommit/1,
         clocksi_icommit/1]).
%% ===========================================================

%% Public API

-spec start() -> {ok, _} | {error, term()}.
start() ->
  application:ensure_all_started(antidote).

-spec stop() -> ok.
stop() ->
  application:stop(antidote).


-spec start_transaction(Clock::snapshot_time() | ignore , Properties::txn_properties(), boolean())
                       -> {ok, txid()} | {error, reason()}.
start_transaction(Clock, _Properties, KeepAlive) ->
    clocksi_istart_tx(Clock, KeepAlive).

-spec start_transaction(Clock::snapshot_time(), Properties::txn_properties())
                       -> {ok, txid()} | {error, reason()}.
start_transaction(Clock, _Properties) ->
    clocksi_istart_tx(Clock, false).

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
            {error, Reason};
        Other ->
            {error, Other}
    end.

-spec read_objects(Objects::[bound_object()], TxId::txid())
                  -> {ok, [term()]} | {error, reason()}.
read_objects(BoundObjects, TxId) ->
    {_, _, CoordFsmPid} = TxId,
    NewObjects = lists:map(fun({Key, Type, Bucket}) ->
                                case materializer:check_operations([{read, {{Key, Bucket}, Type}}]) of
                                    ok ->
                                        {{Key, Bucket}, Type};
                                    {error, Reason} ->
                                        lager:debug("typing problem, check your ops! ~n~p", [Reason]),
                                        {error, Reason}
                                end
                           end, BoundObjects),
    case lists:member({error, type_check}, NewObjects) of
        true -> {error, type_check};
        false ->
            case gen_fsm:sync_send_event(CoordFsmPid, {read_objects, NewObjects}, ?OP_TIMEOUT) of
                     {ok, Res} ->
                         {ok, Res};
                     {error, Reason} -> {error, Reason}
                 end
    end.

-spec update_objects([{bound_object(), op_name(), op_param()}], txid())
                    -> ok | {error, reason()}.
update_objects(Updates, TxId) ->
    {_, _, CoordFsmPid} = TxId,
    case check_and_format_ops(Updates) of
        {error, Reason} ->
            {error, Reason};
        Operations ->
            case gen_fsm:sync_send_event(CoordFsmPid, {update_objects, Operations}, ?OP_TIMEOUT) of
                ok ->
                    ok;
                {aborted, TxId} ->
                    {error, {aborted, TxId}};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% For static transactions: bulk updates and bulk reads
-spec update_objects(snapshot_time() | ignore , term(), [{bound_object(), op_name(), op_param()}]) ->
                            {ok, snapshot_time()} | {error, reason()}.
update_objects(Clock, Properties, Updates) ->
    update_objects(Clock, Properties, Updates, false).

-spec update_objects(snapshot_time() | ignore , term(), [{bound_object(), op_name(), op_param()}], boolean()) ->
    {ok, snapshot_time()} | {error, reason()}.
update_objects(_Clock, _Properties, [], _StayAlive) ->
    {ok, vectorclock:new()};
update_objects(ClientCausalVC, _Properties, Updates, StayAlive) ->
    case check_and_format_ops(Updates) of
        {error, Reason} ->
            {error, Reason};
        Operations ->
            start_static_transaction(update_objects, Operations, StayAlive, ClientCausalVC)
    end.

%% @doc The following function is called by the static versions of read and update_objects
%%      to execute a static transaction.
-spec start_static_transaction(update_objects | read_objects, list(), boolean(), vectorclock()) -> {ok, vectorclock()} | {error, reason()}.
start_static_transaction(TransactionKind, ListOfOperations, StayAlive, ClientCausalVC) ->
            TxPid = case StayAlive of
                true ->
                    whereis(clocksi_interactive_tx_coord_fsm:generate_name(self()));
                false ->
                    undefined
            end,
            case TxPid of
                undefined ->
                    {ok, _CoordFSM} = clocksi_interactive_tx_coord_sup:start_fsm([self(), ClientCausalVC, update_clock, StayAlive, {TransactionKind, ListOfOperations}]);
                TxPid ->
                    ok = gen_fsm:send_event(TxPid, {start_tx, self(), ClientCausalVC, update_clock, {TransactionKind, ListOfOperations}})
            end,
            receive
                Reply ->
                    Reply
            end.


%% @doc This function is used temporarily to unify the
%% interfaces of old and new transactions. It should
%% be removed once tests call only the new interface.
%% It checks the format of the
%% operation for compatibility with
%% some systests that call updates with
%% {Operation, Params} (as a single parameter),
%% and Operation, Params (two separate parameters).
-spec check_and_format_ops([{key(), type(), bucket(), op(), op_param()} | {key(), type(), bucket(), {op(), op_param()}}]) ->
                                [{{key(), bucket()}, type(), {op(), op_param()}}] | {error, reason()}.
check_and_format_ops(Updates) ->
    try
        lists:map(fun(Update) ->
            {Key, Type, Bucket, Op} = case Update of
                {{K, T, B}, O} ->
                    {K, T, B, O};
                {{K, T, B}, O, P} ->
                    {K, T, B, {O, P}}
            end,
            case materializer:check_operations([{update, {{Key, Bucket}, Type, Op}}]) of
                ok ->
                    {{Key, Bucket}, Type, Op};
                {error, Reason} ->
                    throw(Reason)
            end
        end, Updates)
    catch
        _:Reason ->
            {error, Reason}
    end.


read_objects(Clock, Properties, Objects) ->
    read_objects(Clock, Properties, Objects, false).

-spec read_objects(vectorclock(), any(), [bound_object()], boolean()) ->
                        {ok, list(), vectorclock()} | {error, reason()}.
read_objects(Clock, _Properties, Objects, StayAlive) ->
    Args = lists:map(
             fun({Key, Type, Bucket}) ->
                     {read, {{Key,Bucket}, Type}}
             end,
             Objects),
    SingleKey = case Args of
                    [_O] -> %% Single key update
                        case Clock of
                            ignore -> true;
                            _ -> false
                        end;
                    [_H|_T] -> false
                end,
    case SingleKey of
        true -> %% Execute the fast path
            [{read, {Key, Type}}] = Args,
            case materializer:check_operations([{read, {Key, Type}}]) of
                ok ->
                    {ok, Val, CommitTime} = clocksi_interactive_tx_coord_fsm:
                        perform_singleitem_read(Key,Type),
                    {ok, [Val], CommitTime};
                {error, Reason} ->
                    {error, Reason}
            end;
        false ->
            case application:get_env(antidote, txn_prot) of
                {ok, clocksi} ->
                    case clocksi_execute_tx(Clock, Args, update_clock, StayAlive) of
                        {ok, {_TxId, Result, CommitTime}} ->
                            {ok, Result, CommitTime};
                        {error, Reason} -> {error, Reason}
                    end;
                {ok, gr} ->
                    case Args of
                        [_Op] -> %% Single object read = read latest value
                            start_static_transaction(read_objects, Objects, StayAlive, Clock);
                        [_|_] -> %% Read Multiple objects  = read from a snapshot
                            %% Snapshot includes all updates committed at time GST
                            %% from local and remore replicas
                            case gr_snapshot_read(Clock, Args) of
                                {ok, {_TxId, Result, CommitTime}} ->
                                    {ok, Result, CommitTime};
                                {error, Reason} -> {error, Reason}
                            end
                    end
            end
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

-spec register_post_hook(bucket(), module_name(), function_name()) -> ok | {error, function_not_exported}.
register_post_hook(Bucket, Module, Function) ->
    antidote_hooks:register_post_hook(Bucket, Module, Function).

-spec register_pre_hook(bucket(), module_name(), function_name()) -> ok | {error, function_not_exported}.
register_pre_hook(Bucket, Module, Function) ->
    antidote_hooks:register_pre_hook(Bucket, Module, Function).

-spec unregister_hook(pre_commit | post_commit, bucket()) -> ok.
unregister_hook(Prefix, Bucket) ->
    antidote_hooks:unregister_hook(Prefix, Bucket).

%% =============================================================================
%% OLD API, We might still need them

%% @doc The append/2 function adds an operation to the log of the CRDT
%%      object stored at some key.
-spec append(key(), type(), {op() | transfer,term()}) ->
                    {ok, {txid(), [], snapshot_time()}} | {error, term()}.
append(Key, Type, OpParams) ->
    case materializer:check_operations([{update,
                                         {Key, Type, OpParams}}]) of
        ok ->
            clocksi_interactive_tx_coord_fsm:
                perform_singleitem_update(Key, Type, OpParams);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc The read/2 function returns the current value for the CRDT
%%      object stored at some key.
-spec read(key(), type()) -> {ok, val()} | {error, reason()} | {error, {type_check, term()}}.
read(Key, Type) ->
    case materializer:check_operations([{read, {Key, Type}}]) of
        ok ->
            {ok, Val, _CommitTime} = clocksi_interactive_tx_coord_fsm:
                perform_singleitem_read(Key,Type),
            {ok, Val};
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
                         [client_op()],snapshot_time(),boolean()) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
clocksi_execute_tx(Clock, Operations, UpdateClock, KeepAlive) ->
    {ok, TxId} = start_transaction(Clock, [UpdateClock], KeepAlive),
    ReadSet = execute_ops(Operations, TxId, []),
    {ok, CommitTime} = commit_transaction(TxId),
    case ReadSet of
        {error, Reason} ->
            {error, Reason};
        _ ->
            {ok, {TxId, ReadSet, CommitTime}}
    end.

clocksi_execute_tx(Clock, Operations, UpdateClock) ->
    clocksi_execute_tx(Clock, Operations, UpdateClock, false).

clocksi_execute_tx(Clock, Operations) ->
    clocksi_execute_tx(Clock, Operations, update_clock).

-spec clocksi_execute_tx([client_op()]) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
clocksi_execute_tx(Operations) ->
    clocksi_execute_tx(ignore, Operations).

-spec clocksi_execute_int_tx([client_op()]) -> {ok, {txid(), [snapshot()], snapshot_time()}} | {error, term()}.
clocksi_execute_int_tx(Operations) ->
    {ok, TxId} = clocksi_istart_tx(),
    case execute_ops(Operations, TxId, []) of
        {error, Reason} ->
            {error, Reason};
        ReadSet ->
            case clocksi_full_icommit(TxId) of
                {ok, {TxId, CommitTime}} ->
                    {ok, {TxId, ReadSet, CommitTime}};
                Other ->
                    Other
            end
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
clocksi_istart_tx(Clock, KeepAlive) ->
    TxPid = case KeepAlive of
		true ->
		    whereis(clocksi_interactive_tx_coord_fsm:generate_name(self()));
		false ->
		    undefined
	    end,
    _ = case TxPid of
	undefined ->
	    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self(), Clock, update_clock, KeepAlive]);
	TxPid ->
	    ok = gen_fsm:send_event(TxPid, {start_tx, self(), Clock, update_clock})
    end,
    receive
        {ok, TxId} ->
            {ok, TxId};
        Other ->
            {error, Other}
    end.

clocksi_istart_tx(Clock) ->
    clocksi_istart_tx(Clock, false).

clocksi_istart_tx() ->
    clocksi_istart_tx(ignore, false).

-spec clocksi_iread(txid(), key(), type()) -> {ok, term()} | {error, reason()}.
clocksi_iread({_, _, CoordFsmPid}, Key, Type) ->
    case materializer:check_operations([{read, {Key, Type}}]) of
        ok ->
            case gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}}, ?OP_TIMEOUT) of
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
            gen_fsm:sync_send_event(CoordFsmPid, {update, {Key, Type, OpParams}}, ?OP_TIMEOUT);
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
    case gen_fsm:sync_send_event(CoordFsmPid, {prepare, empty}, ?OP_TIMEOUT) of
        {ok,_PrepareTime} ->
            gen_fsm:sync_send_event(CoordFsmPid, commit, ?OP_TIMEOUT);
        Msg ->
            Msg
    end.

-spec clocksi_iprepare(txid()) -> {aborted, txid()} | {ok, non_neg_integer()}.
clocksi_iprepare({_, _, CoordFsmPid})->
    case gen_fsm:sync_send_event(CoordFsmPid, {prepare, two_phase}, ?OP_TIMEOUT) of
        {error, {aborted, TxId}} ->
            {aborted, TxId};
        Reply ->
            Reply
    end.

-spec clocksi_icommit(txid()) -> {aborted, txid()} | {ok, {txid(), snapshot_time()}}.
clocksi_icommit({_, _, CoordFsmPid})->
    gen_fsm:sync_send_event(CoordFsmPid, commit, ?OP_TIMEOUT).

%%% Snapshot read for Gentlerain protocol
gr_snapshot_read(ClientClock, Args) ->
    %% GST = scalar stable time
    %% VST = vector stable time with entries for each dc
    {ok, GST, VST} = dc_utilities:get_scalar_stable_time(),
    DcId = dc_meta_data_utilities:get_my_dc_id(),
    Dt = vectorclock:get_clock_of_dc(DcId, ClientClock),
    case Dt =< GST of
        true ->
            %% Set all entries in snapshot as GST
            ST = dict:map(fun(_,_) -> GST end, VST),
            %% ST doesnot contain entry for local dc, hence explicitly
            %% add it in snapshot time
            SnapshotTime = vectorclock:set_clock_of_dc(DcId, GST, ST),
            clocksi_execute_tx(SnapshotTime, Args, no_update_clock);
        false ->
            timer:sleep(10),
            gr_snapshot_read(ClientClock, Args)
    end.

execute_ops([], _TxId, ReadSet) ->
    lists:reverse(ReadSet);
execute_ops([{update, {Key, Type, OpParams}}|Rest], TxId, ReadSet) ->
    case clocksi_iupdate(TxId, Key, Type, OpParams) of
        ok -> execute_ops(Rest, TxId, ReadSet);
        {error, Reason} ->
            {error, Reason}
    end;
execute_ops([{read, {Key, Type}}|Rest], TxId, ReadSet) ->
    {ok, Value} = clocksi_iread(TxId, Key, Type),
    execute_ops(Rest, TxId, [Value|ReadSet]).
