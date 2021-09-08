%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% @doc Provides an API to read and write to the materializer cache.
%%      Materializer is partioned as per the CURE protocol.

-module(materializer_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("kernel/include/logger.hrl").


%% Number of snapshots to trigger GC
-define(SNAPSHOT_THRESHOLD, 10).
%% Number of cache Levels.
-define(SNAPSHOT_CACHE_LEVELS, 10).
%% Number of snapshots to keep after GC
-define(SNAPSHOT_MIN, 3).
%% Number of ops to keep before GC
-define(OPS_THRESHOLD, 50).
%% If after the op GC there are only this many or less spaces
%% free in the op list then increase the list size
-define(RESIZE_THRESHOLD, 5).
%% Only store the new SS if the following number of ops
%% were applied to the previous SS
-define(MIN_OP_STORE_SS, 5).
%% Expected time to wait until the logging vnode is up
-define(LOG_STARTUP_WAIT, 1000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
    check_tables_ready/0,
    read/6,
    store_ss/3,
    update/2]).

%% Callbacks
-export([init/1,
    terminate/2,
    handle_command/3,
    is_empty/1,
    delete/1,
    handle_handoff_command/3,
    handoff_starting/2,
    handoff_cancelled/1,
    handoff_finished/2,
    handle_handoff_data/2,
    encode_handoff_item/2,
    handle_coverage/4,
    handle_exit/3,
    handle_overload_command/3,
    handle_overload_info/2
]).

-type op_and_id() :: {non_neg_integer(), clocksi_payload()}.
-record(state, {
    partition :: partition_id(),
    ops_cache :: cache_id(),
    snapshot_cache :: cache_id(),
    log_index:: cache_id(),
    is_ready :: boolean()
}).
-type state() :: #state{}.


%% @doc Read state of key at given snapshot time, this does not touch the vnode process
%%      directly, instead it just reads from the operations and snapshot tables that
%%      are in shared memory, allowing concurrent reads.
-spec read(key(), type(), snapshot_time(), txid(), clocksi_readitem:read_property_list(), partition_id()) -> {ok, snapshot()} | {error, reason()}.
read(Key, Type, SnapshotTime, TxId, PropertyList, Partition) ->
    OpsCache = get_cache_name(Partition, ops_cache),
    SnapshotCache = lists:map(fun(Level) ->
        {get_cache_name(Partition, list_to_atom("snapshot_cache" ++ integer_to_list(Level)) ), ?SNAPSHOT_THRESHOLD} end,
        lists:seq(1, ?SNAPSHOT_CACHE_LEVELS)),
    LogIndex = get_cache_name(Partition, log_index),
    State = #state{ops_cache=OpsCache, snapshot_cache=SnapshotCache, log_index=LogIndex, partition=Partition, is_ready=false},
    internal_read(Key, Type, SnapshotTime, TxId, PropertyList, false, State).

%%@doc write operation to cache for future read, updates are stored
%%     one at a time into the ets tables
-spec update(key(), clocksi_payload()) -> ok | {error, reason()}.
update(Key, DownstreamOp) ->
    IndexNode = log_utilities:get_key_partition(Key),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp},
        materializer_vnode_master).





%%---------------- API Functions -------------------%%

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    OpsCache = open_table(Partition, ops_cache),
    SnapshotCache = lists:map(
        fun(Level) ->
            {open_table(Partition, list_to_atom("snapshot_cache" ++ integer_to_list(Level)) ), ?SNAPSHOT_THRESHOLD}
        end,
        lists:seq(1, ?SNAPSHOT_CACHE_LEVELS)),
    LogIndex = open_table(Partition, log_index),
    IsReady = case application:get_env(antidote, recover_from_log) of
                  {ok, true} ->
                      ?LOG_DEBUG("Trying to recover the materializer from log ~p", [Partition]),
                      riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
                      false;
                  _ ->
                      true
              end,
    {ok, #state{is_ready = IsReady, partition=Partition, ops_cache=OpsCache, log_index = LogIndex, snapshot_cache=SnapshotCache}}.


handle_command({hello}, _Sender, State) ->
    {reply, ok, State};

handle_command({check_ready}, _Sender, State = #state{partition=Partition, is_ready=IsReady}) ->
    Result = case has_ops_cache(Partition) and has_snapshot_cache(Partition) and has_log_index_cache(Partition) of
                 false ->
                     false;
                 true ->
                     true
             end,
    Result2 = Result and IsReady,
    {reply, Result2, State};

handle_command({read, Key, Type, SnapshotTime, TxId}, _Sender, State) ->
    {reply, read(Key, Type, SnapshotTime, TxId, [], State), State};

handle_command({update, Key, DownstreamOp}, _Sender, State) ->
    true = op_insert_gc(Key, DownstreamOp, State),
    {reply, ok, State};

handle_command({store_ss, Key, Snapshot, CommitTime}, _Sender, State) ->
    cacheInsert(Key, Snapshot, CommitTime, State),
    {noreply, State};

handle_command(load_from_log, _Sender, State=#state{partition=Partition}) ->
    IsReady = try
                  case load_from_log_to_tables(Partition, State) of
                      ok ->
                          ?LOG_DEBUG("Finished loading from log to materializer on partition ~w", [Partition]),
                          true;
                      {error, not_ready} ->
                          false;
                      {error, Reason} ->
                          ?LOG_ERROR("Unable to load logs from disk: ~w, continuing", [Reason]),
                          true
                  end
              catch
                  _:Reason1 ->
                      ?LOG_DEBUG("Error loading from log ~w, will retry", [Reason1]),
                      false
              end,
    ok = case IsReady of
             false ->
                 riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
                 ok;
             true ->
                 ok
         end,
    {noreply, State#state{is_ready=IsReady}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(#riak_core_fold_req_v2{foldfun=Fun, acc0=Acc0},
    _Sender,
    State = #state{ops_cache = OpsCache}) ->
    F = fun(Key, A) ->
        [Key1|_] = tuple_to_list(Key),
        Fun(Key1, Key, A)
        end,
    Acc = cache_table_fold(F, Acc0, OpsCache),
    {reply, Acc, State};

handle_handoff_command(Command, _Sender, State) ->
    ?LOG_WARNING("Unexpected access to the materializer while in handoff lifecycle: ~p", [Command]),
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State=#state{ops_cache=OpsCache}) ->
    {_Key, Operation} = binary_to_term(Data),
    true = insert_ops_cache_tuple(OpsCache, Operation),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{ops_cache=OpsCache}) ->
    case cache_is_empty(OpsCache) of
        true ->
            {true, State};
        false ->
            {false, State}
    end.

delete(State=#state{ops_cache=_OpsCache}) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_overload_command(_, _, _) ->
    ok.
handle_overload_info(_, _) ->
    ok.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State=#state{ops_cache=OpsCache, snapshot_cache=SnapshotCache, log_index = LogIndex}) ->
    try
        delete_cache(OpsCache),
        delete_cache(SnapshotCache),
        delete_cache(LogIndex)
    catch
        _:_Reason2->
            ok
    end,
    ok.



%%---------------- Internal Functions -------------------%%

%%@doc write snapshot to cache for future read, snapshots are stored
%%     one at a time into the ets table
-spec store_ss(key(), materialized_snapshot(), snapshot_time()) -> ok.
store_ss(Key, Snapshot, CommitTime) ->
    IndexNode = log_utilities:get_key_partition(Key),
    riak_core_vnode_master:command(IndexNode, {store_ss, Key, Snapshot, CommitTime},
        materializer_vnode_master).


%% @doc The tables holding the updates and snapshots are shared with concurrent non-blocking
%%      readers.
%%      Returns true if all tables have been initialized, false otherwise.
-spec check_tables_ready() -> boolean().
check_tables_ready() ->
    PartitionList = dc_utilities:get_all_partitions_nodes(),
    check_table_ready(PartitionList).

-spec check_table_ready([{partition_id(), node()}]) -> boolean().
check_table_ready([]) ->
    true;
check_table_ready([{Partition, Node}|Rest]) ->
    Result =
        try
            riak_core_vnode_master:sync_command({Partition, Node},
                {check_ready},
                materializer_vnode_master,
                infinity)
        catch
            _:_Reason ->
                false
        end,
    case Result of
        true ->
            check_table_ready(Rest);
        false ->
            false
    end.




-spec load_from_log_to_tables(partition_id(), state()) -> ok | {error, reason()}.
load_from_log_to_tables(Partition, State) ->
    LogId = [Partition],
    Node = {Partition, log_utilities:get_my_node(Partition)},
    loop_until_loaded(Node, LogId, start, dict:new(), State).

-spec loop_until_loaded({partition_id(), node()},
    log_id(),
    start | disk_log:continuation(),
    dict:dict(txid(), [any_log_payload()]),
    state()) ->
    ok | {error, reason()}.
loop_until_loaded(Node, LogId, Continuation, Ops, State) ->
    case logging_vnode:get_all(Node, LogId, Continuation, Ops) of
        {error, Reason} ->
            ?LOG_ERROR("Could not load all entries from log ~p and node ~p: ~p", [LogId, Node, Reason]),
            {error, Reason};
        {NewContinuation, NewOps, OpsDict} ->
            load_ops(OpsDict, State),
            loop_until_loaded(Node, LogId, NewContinuation, NewOps, State);
        {eof, OpsDict} ->
            load_ops(OpsDict, State),
            ok
    end.

-spec load_ops(dict:dict(key(), [{non_neg_integer(), clocksi_payload()}]), state()) -> true.
load_ops(OpsDict, State) ->
    dict:fold(fun(Key, CommittedOps, _Acc) ->
        lists:foreach(fun({_OpId, Op}) ->
            #clocksi_payload{key = Key} = Op,
            op_insert_gc(Key, Op, State)
                      end, CommittedOps)
              end, true, OpsDict).


%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
-spec internal_read(key(), type(), snapshot_time(), txid() | ignore, clocksi_readitem:read_property_list(), boolean(), state())
        -> {ok, snapshot()} | {error, no_snapshot}.

internal_read(Key, Type, MinSnapshotTime, TxId, _PropertyList, ShouldGc, State) ->
    %% First look for any existing snapshots in the cache that is compatible with
    SnapshotGetResp = get_from_snapshot_cache(Key, Type, MinSnapshotTime, State),

    %% Now apply the operations to the snapshot, and return a materialized value
    materialize_snapshot(TxId, Key, Type, MinSnapshotTime, ShouldGc, State, SnapshotGetResp).

%% @doc Get the most recent snapshot from the cache (smaller than the given commit time) for a given key.
%%
%%      If there's no in-memory suitable snapshot, it will fetch it from the replication log.
%%
-spec get_from_snapshot_cache(key(), type(), snapshot_time(), state()) -> snapshot_get_response().
get_from_snapshot_cache(Key, Type, MinSnaphsotTime, State = #state{
    ops_cache=OpsCache,
    snapshot_cache=SnapshotCache
}) ->
    case cacheLookup(SnapshotCache, Key) of
        {error, not_exist} ->
            EmptySnapshot = #materialized_snapshot{
                last_op_id=0,
                value=clocksi_materializer:new(Type)
            },

            store_snapshot(Key, EmptySnapshot, vectorclock:new(), State),
            %% Create a base version committed at time ignore, i.e. bottom
            BaseVersion = {{ignore, EmptySnapshot}, true},
            update_snapshot_from_cache(BaseVersion, Key, OpsCache);

        {ok, SnapshotDict} ->
            case vector_orddict:get_smaller(MinSnaphsotTime, SnapshotDict) of
                {undefined, _} ->
                    %% No in-memory snapshot, get it from replication log
                    get_from_snapshot_log(Key, Type, MinSnaphsotTime);

                FoundVersion ->
                    %% Snapshot was present, now update it with the operations found in the cache.
                    %%
                    %% Operations are taken from the in-memory cache.
                    %% Any snapshot already in the cache will have more recent operations
                    %% also in the cache, so no need to hit the log.
                    update_snapshot_from_cache(FoundVersion, Key, OpsCache)
            end
    end.

-spec get_from_snapshot_log(key(), type(), snapshot_time()) -> snapshot_get_response().
get_from_snapshot_log(Key, Type, SnapshotTime) ->
    LogId = log_utilities:get_logid_from_key(Key),
    Partition = log_utilities:get_key_partition(Key),
    logging_vnode:get_up_to_time(Partition, LogId, SnapshotTime, Type, Key).

%% @doc Store a new key snapshot in the in-memory cache at the given commit time.
%%
%%      If `ShouldGC' is true, it will try to prune the in-memory cache before inserting.
%%
-spec store_snapshot(key(), materialized_snapshot(), snapshot_time(), state()) -> ok.
store_snapshot(Key, Snapshot, Time, _State) ->

    materializer_vnode:store_ss(Key, Snapshot, Time).

%% @doc Given a snapshot from the cache, update it from the ops cache.
-spec update_snapshot_from_cache({{snapshot_time() | ignore, materialized_snapshot()}, boolean()}, key(), cache_id())
        -> snapshot_get_response().

update_snapshot_from_cache(SnapshotResponse, Key, OpsCache) ->
    {{SnapshotCommitTime, LatestSnapshot}, IsFirst} = SnapshotResponse,
    {Ops, OpsLen} = fetch_updates_from_cache(OpsCache, Key),
    #snapshot_get_response{
        ops_list=Ops,
        number_of_ops=OpsLen,
        is_newest_snapshot=IsFirst,
        snapshot_time=SnapshotCommitTime,
        materialized_snapshot=LatestSnapshot
    }.

%% @doc Given a key, get all the operations in the ops cache.
%%
%%      Will also return how many operations were in the cache.
%%
-spec fetch_updates_from_cache(cache_id(), key()) -> {[op_and_id()] | tuple(), non_neg_integer()}.
fetch_updates_from_cache(OpsCache, Key) ->
    case get_ops_from_cache(OpsCache, Key) of
        not_found ->
            {[], 0};

        {ok, {_Key, Length, _OpId, _ListLen, CachedOps}} ->
            {CachedOps, Length}
    end.

-spec materialize_snapshot(txid() | ignore, key(), type(), snapshot_time(), boolean(), state(), snapshot_get_response())
        -> {ok, snapshot_time()} | {error, reason()}.

materialize_snapshot(_TxId, _Key, _Type, _SnapshotTime, _ShouldGC, _State, #snapshot_get_response{
    number_of_ops=0,
    materialized_snapshot=Snapshot
}) ->
    {ok, Snapshot#materialized_snapshot.value};

materialize_snapshot(TxId, Key, Type, SnapshotTime, ShouldGC, State, SnapshotResponse = #snapshot_get_response{
    is_newest_snapshot=IsNewest
}) ->
    case clocksi_materializer:materialize(Type, TxId, SnapshotTime, SnapshotResponse) of
        {error, Reason} ->
            {error, Reason};

        {ok, MaterializedSnapshot, NewLastOp, CommitTime, WasUpdated, OpsAdded} ->
            %% the following checks for the case there were no snapshots and there were operations,
            %% but none was applicable for the given snapshot_time
            %% But is the snapshot not safe?
            case CommitTime of
                ignore ->
                    {ok, MaterializedSnapshot};
                _ ->
                    %% Check if we need to refresh the cache
                    SufficientOps = OpsAdded >= ?MIN_OP_STORE_SS,
                    ShouldRefreshCache = WasUpdated and IsNewest and SufficientOps,

                    %% Only store the snapshot if it would be at the end of the list and
                    %% has new operations added to the previous snapshot
                    ok = case ShouldRefreshCache orelse ShouldGC of
                             false ->
                                 ok;

                             true ->
                                 ToCache = #materialized_snapshot{
                                     last_op_id=NewLastOp,
                                     value=MaterializedSnapshot
                                 },
                                 store_snapshot(Key, ToCache, CommitTime, State)
                         end,
                    {ok, MaterializedSnapshot}
            end
    end.


%% @doc Extract from the tuple stored in the operation cache
%% 1) the key, 2) length of the op list (stored in form of a tuple),
%% 3) sequence number of operation (used to trigger GC regularly),
%% 4) size of the tuple that contains the op list, 5) the tuple containing the op list
%% Note that the ops in the tuple are stored in the ets with the most recent op at the end of
%% the tuple.
-spec deconstruct_opscache_entry(tuple()) ->
    {key(), non_neg_integer(), non_neg_integer(), non_neg_integer(), [op_and_id()] | tuple()}.
deconstruct_opscache_entry(Tuple) ->
    Key = element(1, Tuple),
    {Length, ListLen} = element(2, Tuple),
    OpId = element(3, Tuple),
    {Key, Length, OpId, ListLen, Tuple}.

%% @doc Insert an operation and optionally start garbage collection triggered by writes.
-spec op_insert_gc(key(), clocksi_payload(), state()) -> true.
op_insert_gc(Key, DownstreamOp, State = #state{ops_cache = OpsCache}) ->
    %% Check whether there is an ops cache entry for the key
    case has_ops_for_key(OpsCache, Key) of
        false ->
            insert_ops_cache_tuple(OpsCache, erlang:make_tuple(?FIRST_OP+?OPS_THRESHOLD, 0, [{1, Key}, {2, {0, ?OPS_THRESHOLD}}]));
        true ->
            ok
    end,
    NewId = increment_op_id(OpsCache, Key),
    {Length, ListLen} = get_op_list_length(OpsCache, Key),

    %% Perform GC by triggering a read when there are more than OPS_THRESHOLD
    %% operations for a given key or when the list is full
    case (Length >= ListLen) orelse ((NewId rem ?OPS_THRESHOLD) == 0) of
        true ->
            Type = DownstreamOp#clocksi_payload.type,
            SnapshotTime = DownstreamOp#clocksi_payload.snapshot_time,
            %% Here is where the GC is done (with the 5th argument being "true", GC is performed by the internal read
            {_, _} = internal_read(Key, Type, SnapshotTime, ignore, [], true, State),
            %% Have to obtain again the list/tuple information because the internal_read can change it
            {NewLength, NewListLen} = get_op_list_length(OpsCache, Key),
            %% Insert the new op
            true = update_ops_element(OpsCache, Key, [{NewLength+?FIRST_OP, {NewId, DownstreamOp}}, {2, {NewLength+1, NewListLen}}]);
        false ->
            true = update_ops_element(OpsCache, Key, [{Length+?FIRST_OP, {NewId, DownstreamOp}}, {2, {Length+1, ListLen}}])
    end.

%%%===================================================================
%%%  Ets tables
%%%
%%%  ops_cache:
%%%  snapshot_cache:
%%%  log_index:
%%%===================================================================

-spec get_cache_name(non_neg_integer(), atom()) -> atom().
get_cache_name(Partition, Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).

-spec open_table(partition_id(), 'ops_cache' | 'log_index') -> atom() | cache_id().
open_table(Partition, Name) ->
    case ets:info(get_cache_name(Partition, Name)) of
        undefined ->
            ets:new(get_cache_name(Partition, Name),
                [set, protected, named_table, ?TABLE_CONCURRENCY]);
        _ ->
            %% Other vnode hasn't finished closing tables
            ?LOG_DEBUG("Unable to open ets table in materializer vnode, retrying"),
            timer:sleep(100),
            try
                ets:delete(get_cache_name(Partition, Name))
            catch
                _:_Reason ->
                    ok
            end,
            open_table(Partition, Name)
    end.

-spec has_ops_cache(partition_id()) -> boolean().
has_ops_cache(Partition) ->
    case ets:info(get_cache_name(Partition, ops_cache)) of
        undefined ->
            false;
        _ ->
            true
    end.

-spec has_snapshot_cache(partition_id()) -> boolean().
has_snapshot_cache(Partition) ->
    case ets:info(get_cache_name(Partition, snapshot_cache)) of
        undefined ->
            false;
        _ ->
            true
    end.

-spec has_log_index_cache(partition_id()) -> boolean().
has_log_index_cache(Partition) ->
    case ets:info(get_cache_name(Partition, log_index)) of
        undefined ->
            false;
        _ ->
            true
    end.

-spec cache_table_fold(fun(), term(), cache_id()) -> term().
cache_table_fold(F, Acc0, OpsCache) ->
    ets:foldl(F, Acc0, OpsCache).

-spec insert_ops_cache_tuple(cache_id(), tuple()) -> true.
insert_ops_cache_tuple(OpsCache, Tuple) ->
    ets:insert(OpsCache, Tuple).

-spec cache_is_empty(cache_id()) -> boolean().
cache_is_empty(OpsCache) ->
    case ets:first(OpsCache) of
        '$end_of_table' ->
            true;
        _ ->
            false
    end.

-spec delete_cache(cache_id()) -> true.
delete_cache(Cache) ->
    ets:delete(Cache).

-spec get_ops_from_cache(cache_id(), key()) -> not_found | {ok, {key(), non_neg_integer(), non_neg_integer(), non_neg_integer(), [op_and_id()] | tuple()}}.
get_ops_from_cache(OpsCache, Key) ->
    case ets:lookup(OpsCache, Key) of
        [] ->
            not_found;
        [Tuple] ->
            {ok, deconstruct_opscache_entry(Tuple)}
    end.

-spec has_ops_for_key(cache_id(), key()) -> boolean().
has_ops_for_key(OpsCache, Key) ->
    ets:member(OpsCache, Key).

-spec increment_op_id(cache_id(), key()) -> non_neg_integer().
increment_op_id(OpsCache, Key) ->
    ets:update_counter(OpsCache, Key, {3, 1}).

-spec get_op_list_length(cache_id(), key()) -> {non_neg_integer(), non_neg_integer()}.
get_op_list_length(OpsCache, Key) ->
    ets:lookup_element(OpsCache, Key, 2).

-spec update_ops_element(cache_id(), key(), [{non_neg_integer(), term()}]) -> boolean().
update_ops_element(OpsCache, Key, Update) ->
    ets:update_element(OpsCache, Key, Update).

cacheLookup([],_Key) ->
    {error, not_exist};
cacheLookup([{CacheStore,_Size}| CacheIdentifiers], Key) ->
    case ets:lookup(CacheStore, Key) of
        [] -> cacheLookup(CacheIdentifiers, Key);
        [Object] -> {ok, Object}
    end.


cacheInsert(Key, Snapshot, Clock, _State = #state{snapshot_cache = CacheIdentifiers }) ->
    {CacheStore, MaxSize} = lists:nth(1, CacheIdentifiers),
    Size = ets:info(CacheStore, size),
    ets:insert(CacheStore, {Key, Snapshot, Clock}),
    case Size >= MaxSize of
        false -> {CacheIdentifiers, Size+1};
        true ->
            case ets:info(CacheStore, size) >= MaxSize of
                false -> {CacheIdentifiers, Size+1};
                true -> {garbageCollect(CacheIdentifiers), 0}
            end
    end.

-spec garbageCollect(list()) -> list().
garbageCollect(CacheIdentifiers) ->
    logger:debug("Initiating Garbage Collection"),
    {LastSegment, Size} = lists:last(CacheIdentifiers),
    ets:delete(LastSegment),
    ets:new(LastSegment, [named_table, ?TABLE_CONCURRENCY]),
    SubList = lists:droplast(CacheIdentifiers),
    UpdatedCacheIdentifiers = lists:append([{LastSegment,Size}],SubList),
    logger:debug("New CacheIdentifier List is ~p ~n",[UpdatedCacheIdentifiers]),
    UpdatedCacheIdentifiers.


-ifdef(TEST).

create_snapshot_caches() ->
    [{ets:new(snapshot_cache1, [set]), 20}, {ets:new(snapshot_cache2, [set]), 20}, {ets:new(snapshot_cache3, [set]), 20}].

%% This tests to make sure when garbage collection happens, no updates are lost
gc_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = create_snapshot_caches(),
    Key = mycount,
    DC1 = 1,
    Type = antidote_crdt_counter_pn,

    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Make max. number of snapshots
    lists:map(
        fun(N) ->
            {ok, Res} = internal_read(Key, Type, vectorclock:from_list([{DC1, N * 10 + 2}]), ignore, [], false, State),
            io:format(Res),
            ?assertEqual(N, Type:value(Res)),
            op_insert_gc(Key, generate_payload(N * 10, N * 10 + 1, Res, a), State)
        end, lists:seq(0, ?SNAPSHOT_THRESHOLD)),

    %% Insert some new values

    op_insert_gc(Key, generate_payload(15, 111, 1, a), State),
    op_insert_gc(Key, generate_payload(16, 121, 1, a), State),

    %% Trigger the clean
    {ok, Res10} = internal_read(Key, Type, vectorclock:from_list([{DC1, 102}]), ignore, [], true, State),
    ?assertEqual(11, Type:value(Res10)),

    op_insert_gc(Key, generate_payload(102, 131, 9, a), State),

    %% Be sure you didn't loose any updates
    {ok, Res13} = internal_read(Key, Type, vectorclock:from_list([{DC1, 142}]), ignore, [], true, State),
    ?assertEqual(14, Type:value(Res13)).

%% This tests to make sure operation lists can be large and resized
large_list_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = create_snapshot_caches(),
    Key = mycount,
    DC1 = 1,
    Type = antidote_crdt_counter_pn,
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Make 1000 updates to grow the list, whithout generating a snapshot to perform the gc
    {ok, Res0} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2}]), ignore, [], false, State),
    ?assertEqual(0, Type:value(Res0)),

    lists:foreach(fun(Val) ->
        op_insert_gc(Key, generate_payload(10, 11+Val, Res0, mycount), State)
                  end, lists:seq(1, 1000)),

    {ok, Res1000} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2000}]), ignore, [], false, State),
    ?assertEqual(1000, Type:value(Res1000)),

    %% Now check everything is ok as the list shrinks from generating new snapshots
    lists:foreach(fun(Val) ->
        op_insert_gc(Key, generate_payload(10+Val, 11+Val, Res0, mycount), State),
        {ok, Res} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2000}]), ignore, [], false, State),
        ?assertEqual(Val, Type:value(Res))
                  end, lists:seq(1001, 1100)).

generate_payload(SnapshotTime, CommitTime, Prev, Key) ->
    Type = antidote_crdt_counter_pn,
    DC1 = 1,

    {ok, Op1} = Type:downstream({increment, 1}, Prev),
    #clocksi_payload{key = Key,
        type = Type,
        op_param = Op1,
        snapshot_time = vectorclock:from_list([{DC1, SnapshotTime}]),
        commit_time = {DC1, CommitTime},
        txid = 1
    }.

seq_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = create_snapshot_caches(),
    Key = mycount,
    Type = antidote_crdt_counter_pn,
    DC1 = 1,
    S1 = Type:new(),
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Insert one increment
    {ok, Op1} = Type:downstream({increment, 1}, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
        type = Type,
        op_param = Op1,
        snapshot_time = vectorclock:from_list([{DC1, 10}]),
        commit_time = {DC1, 15},
        txid = 1
    },
    op_insert_gc(Key, DownstreamOp1, State),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(Res1)),
    %% Insert second increment
    {ok, Op2} = Type:downstream({increment, 1}, S1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
        op_param = Op2,
        snapshot_time = vectorclock:from_list([{DC1, 16}]),
        commit_time = {DC1, 20},
        txid = 2},

    op_insert_gc(Key, DownstreamOp2, State),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 21}]), ignore, [], false, State),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(ReadOld)).

multipledc_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = create_snapshot_caches(),
    Key = mycount,
    Type = antidote_crdt_counter_pn,
    DC1 = 1,
    DC2 = 2,
    S1 = Type:new(),
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},


    %% Insert one increment in DC1
    {ok, Op1} = Type:downstream({increment, 1}, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
        type = Type,
        op_param = Op1,
        snapshot_time = vectorclock:from_list([{DC2, 0}, {DC1, 10}]),
        commit_time = {DC1, 15},
        txid = 1
    },
    op_insert_gc(Key, DownstreamOp1, State),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}, {DC2, 0}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(Res1)),

    %% Insert second increment in other DC
    {ok, Op2} = Type:downstream({increment, 1}, S1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
        op_param = Op2,
        snapshot_time = vectorclock:from_list([{DC2, 16}, {DC1, 16}]),
        commit_time = {DC2, 20},
        txid = 2},
    op_insert_gc(Key, DownstreamOp2, State),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}, {DC2, 21}]), ignore, [], false, State),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1, 15}, {DC2, 15}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(ReadOld)).

concurrent_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = create_snapshot_caches(),
    Key = mycount,
    Type = antidote_crdt_counter_pn,
    DC1 = local,
    DC2 = remote,
    S1 = Type:new(),
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Insert one increment in DC1
    {ok, Op1} = Type:downstream({increment, 1}, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
        type = Type,
        op_param = Op1,
        snapshot_time = vectorclock:from_list([{DC1, 0}, {DC2, 0}]),
        commit_time = {DC2, 1},
        txid = 1},
    op_insert_gc(Key, DownstreamOp1, State),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC2, 1}, {DC1, 0}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(Res1)),

    %% Another concurrent increment in other DC
    {ok, Op2} = Type:downstream({increment, 1}, S1),
    DownstreamOp2 = #clocksi_payload{ key = Key,
        type = Type,
        op_param = Op2,
        snapshot_time = vectorclock:from_list([{DC1, 0}, {DC2, 0}]),
        commit_time = {DC1, 1},
        txid = 2},
    op_insert_gc(Key, DownstreamOp2, State),

    %% Read different snapshots
    {ok, ReadDC1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 1}, {DC2, 0}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(ReadDC1)),
    {ok, ReadDC2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 0}, {DC2, 1}]), ignore, [], false, State),
    ?assertEqual(1, Type:value(ReadDC2)),

    %% Read snapshot including both increments
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC2, 1}, {DC1, 1}]), ignore, [], false, State),
    ?assertEqual(2, Type:value(Res2)).

%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%% E.g., for a gcounter, return 0.
read_nonexisting_key_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = create_snapshot_caches(),
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},
    Type = antidote_crdt_counter_pn,
    {ok, ReadResult} = internal_read(key, Type, vectorclock:from_list([{dc1, 1}, {dc2, 0}]), ignore, [], false, State),
    ?assertEqual(0, Type:value(ReadResult)).

-endif.
