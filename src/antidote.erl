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
-include("querying.hrl").
-include("lock_mgr.hrl").
-include("lock_mgr_es.hrl").

%% API for applications
-export([ start/0, stop/0,
          start_transaction/2,
          read_objects/2,
          read_objects/3,
          query_objects/2,
          update_objects/2,
          update_objects/3,
          abort_transaction/1,
          commit_transaction/1,
          create_bucket/2,
          create_object/3,
          delete_object/1,
          register_pre_hook/3,
          register_post_hook/3,
          unregister_hook/2,
          get_objects/3,
          get_log_operations/1,
          get_default_txn_properties/0,
          get_txn_property/2,
          get_locks/3,
          get_locks/4,
          release_locks/2
        ]).

%% Public API

-spec start() -> {ok, _} | {error, term()}.
start() ->
    application:ensure_all_started(antidote).

-spec stop() -> ok.
stop() ->
    application:stop(antidote).


%% Takes as input a list of tuples of bound objects and snapshot times
%% Returns a list for each object that contains all logged update operations more recent than the give snapshot time
-spec get_log_operations([{bound_object(), snapshot_time()}]) -> {ok, [[{non_neg_integer(), clocksi_payload()}]]} | {error, reason()}.
get_log_operations(ObjectClockPairs) ->
    %% result is a list of lists of lists
    %% internal list is {number, clocksi_payload}
    get_log_operations_internal(ObjectClockPairs, []).

get_log_operations_internal([], Acc) ->
    {ok, lists:reverse(Acc)};
get_log_operations_internal([{{Key, Type, Bucket}, Clock}|Rest], Acc) ->
    case materializer:check_operations([{read, {{Key, Bucket}, Type}}]) of
    ok ->
        LogId = log_utilities:get_logid_from_key({Key, Bucket}),
        [Node] = log_utilities:get_preflist_from_key({Key, Bucket}),
        case logging_vnode:get_from_time(Node, LogId, Clock, Type, {Key, Bucket}) of
        #snapshot_get_response{ops_list = Ops} ->
            get_log_operations_internal(Rest, [lists:reverse(Ops)|Acc]);
        {error, Reason} ->
            {error, Reason}
        end;
    {error, Reason} ->
        {error, Reason}
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

%% Register a post commit hook.
%% Module:Function({Key, Type, Op}) will be executed after successful commit of
%% each transaction that updates Key.
-spec register_post_hook(bucket(), module_name(), function_name()) -> ok | {error, function_not_exported}.
register_post_hook(Bucket, Module, Function) ->
    antidote_hooks:register_post_hook(Bucket, Module, Function).

%% Register a pre commit hook.
%% Module:Function({Key, Type, Op}) will be executed before executing an update "op"
%% on key. If pre commit hook fails, transaction will be aborted
-spec register_pre_hook(bucket(), module_name(), function_name()) -> ok | {error, function_not_exported}.
register_pre_hook(Bucket, Module, Function) ->
    antidote_hooks:register_pre_hook(Bucket, Module, Function).

-spec unregister_hook(pre_commit | post_commit, bucket()) -> ok.
unregister_hook(Prefix, Bucket) ->
    antidote_hooks:unregister_hook(Prefix, Bucket).


%% Transaction API %%
%% ==============  %%

-spec start_transaction(Clock::snapshot_time(), Properties::txn_properties())
                       -> {ok, txid()} | {error, reason()}.
start_transaction(Clock, Properties) ->
    cure:start_transaction(Clock, Properties, false).

-spec abort_transaction(TxId::txid()) -> ok | {error, reason()}.
abort_transaction(TxId) ->
    cure:abort_transaction(TxId).

-spec commit_transaction(TxId::txid()) ->
                                {ok, snapshot_time()} | {error, reason()}.
commit_transaction(TxId) ->
    cure:commit_transaction(TxId).
%% TODO: Execute post_commit hooks here?

-spec read_objects(Objects::[bound_object()], TxId::txid())
                  -> {ok, [term()]} | {error, reason()}.
read_objects(Objects, TxId) ->
    case type_check(Objects, fun type_check_read/1) of
        ok ->
            cure:read_objects(Objects, TxId);
        {error, Reason} ->
            {error, Reason}
    end.

-spec read_objects(vectorclock(), txn_properties(), [bound_object()])
                  -> {ok, list(), vectorclock()} | {error, reason()}.
read_objects(Clock, Properties, Objects) ->
    case type_check(Objects, fun type_check_read/1) of
        ok ->
            cure:read_objects(Clock, Properties, Objects);
        {error, Reason} ->
            {error, Reason}
    end.

%%%%%%%%%%%%%%%%%%% QUERY ZONE %%%%%%%%%%%%%%%%%%%

-spec query_objects(filter(), txid()) -> {ok, [term()]} | {error, reason()}.
query_objects(Filter, TxId) ->
    query_optimizer:query(Filter, TxId).

%%%%%%%%%%%%%%%% END OF QUERY ZONE %%%%%%%%%%%%%%%

%% Returns a list containing tuples of object state and commit time for each
%% of those objects
-spec get_objects(vectorclock(), txn_properties(), [bound_object()])
                  -> {ok, list(), vectorclock()} | {error, reason()}.
get_objects(Clock, Objects, Properties) ->
    cure:get_objects(Clock, Objects, Properties).

-spec update_objects([{bound_object(), op_name(), op_param()} | {bound_object(), {op_name(), op_param()}}], txid())
                    -> ok | {error, reason()}.
update_objects(Updates, TxId) ->
    case type_check(Updates, fun type_check_update/1) of
        ok ->
            %ok = indexing:create_index_hooks(Updates),
            %cure:update_objects(Updates, TxId);

            {ok, AddUpdates} = index_manager:create_index_hooks(Updates),
            NewUpdates = lists:append(Updates, AddUpdates),
            %triggers:create_triggers(Updates),
            cure:update_objects(NewUpdates, TxId);
        {error, Reason} ->
            {error, Reason}
    end.

%% For static transactions: bulk updates and bulk reads
-spec update_objects(snapshot_time() | ignore , txn_properties(), [{bound_object(), op_name(), op_param()}| {bound_object(), {op_name(), op_param()}}])
                     -> {ok, snapshot_time()} | {error, reason()}.
update_objects(Clock, Properties, Updates) ->
    case type_check(Updates, fun type_check_update/1) of
        ok ->
            %ok = indexing:create_index_hooks(Updates, TxId),
            %cure:update_objects(Clock, Properties, Updates);

            {ok, AddUpdates} = index_manager:create_index_hooks(Updates),
            NewUpdates = lists:append(Updates, AddUpdates),
            %triggers:create_triggers(Updates),
            cure:update_objects(Clock, Properties, NewUpdates);
        {error, Reason} ->
            {error, Reason}
    end.

-spec get_locks(default | integer(), [key()], txid()) -> {ok,snapshot_time()} | {locks_not_available,[key()]} | {missing_locks, [{txid(),[key()]}]}.
get_locks(default, Locks, TxId) ->
    clocksi_interactive_coord:get_locks(?How_LONG_TO_WAIT_FOR_LOCKS, TxId, Locks);
get_locks(Timeout, Locks, TxId) ->
    clocksi_interactive_coord:get_locks(Timeout, TxId, Locks).

-spec get_locks(default | integer(), [key()], [key()], txid()) -> {ok, [snapshot_time()]} | {missing_locks, [key()]} | {locks_in_use, [txid()]}.
get_locks(default, SharedLocks, ExclusiveLocks, TxId) ->
    clocksi_interactive_coord:get_locks(?How_LONG_TO_WAIT_FOR_LOCKS_ES, TxId, SharedLocks, ExclusiveLocks);
get_locks(Timeout, SharedLocks, ExclusiveLocks, TxId) ->
    clocksi_interactive_coord:get_locks(Timeout, TxId, SharedLocks, ExclusiveLocks).

-spec release_locks(locks | es_locks, txid()) -> ok.
release_locks(Type, TxId) ->
    clocksi_interactive_coord:release_locks(Type, TxId).

%%% Internal function %%
%%% ================= %%

type_check_update({{_K, Type, _bucket}, Op, Param}) ->
    antidote_crdt:is_type(Type) andalso
        Type:is_operation({Op, Param}).

type_check_read({{_K, _T, _B} = BoundObject, {_Op, _Args}}) ->
    %% TODO: Check Op is valid. It requires each CRDT to export its read operations.
    type_check_read(BoundObject);
type_check_read({_K, Type, _Bucket}) ->
    antidote_crdt:is_type(Type).

-spec type_check([{bound_object(), op_name(), op_param()} | bound_object()], fun()) -> ok | {error, reason()}.
type_check(Updates, TypeCheckFun) ->
    try
        lists:foreach(fun(Update) ->
                              case TypeCheckFun(Update) of
                                  true -> ok;
                                  false -> throw({badtype, Update})
                              end
                      end, Updates),
        ok
    catch
        _:Reason ->
            {error, Reason}
    end.

-spec get_default_txn_properties() -> txn_properties().
get_default_txn_properties() ->
    [{update_clock, true}].

-spec get_txn_property(atom(), txn_properties()) -> atom().
get_txn_property(update_clock, Properties) ->
    case lists:keyfind(update_clock, 1, Properties) of
     false ->
        update_clock;
    {update_clock, ShouldUpdate} ->
        case ShouldUpdate of
        true ->
            update_clock;
        false ->
            no_update_clock
        end
    end;
get_txn_property(certify, Properties) ->
    case lists:keyfind(certify, 1, Properties) of
    false ->
        application:get_env(antidote, txn_cert, true);
    {certify, Certify} ->
        case Certify of
        use_default ->
            application:get_env(antidote, txn_cert, true);
        certify ->
            %% Note that certify will only work correctly when
            %% application:get_env(antidote, txn_cert, true); returns true
            %% the reason is is that in clocksi_vnode:commit, the timestamps
            %% for committed transactions will only be saved if application:get_env(antidote, txn_cert, true)
            %% is true
            %% we might want to change this in the future
            true;
        dont_certify ->
            false
        end
    end.
