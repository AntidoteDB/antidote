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
-module(materializer_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%% Number of snapshots to trigger GC
-define(SNAPSHOT_THRESHOLD, 10).
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

-type op_and_id() :: {non_neg_integer(), #clocksi_payload{}}.
-record(state, {
    partition :: partition_id(),
    ops_cache :: cache_id(),
    snapshot_cache :: cache_id(),
    is_ready :: boolean()
}).

%%---------------- API Functions -------------------%%

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time, this does not touch the vnode process
%%      directly, instead it just reads from the operations and snapshot tables that
%%      are in shared memory, allowing concurrent reads.
-spec read(key(), type(), snapshot_time(), txid(), clocksi_readitem_server:read_property_list(), partition_id()) -> {ok, snapshot()} | {error, reason()}.
read(Key, Type, SnapshotTime, TxId, PropertyList, Partition) ->
    OpsCache = get_cache_name(Partition, ops_cache),
    SnapshotCache = get_cache_name(Partition, snapshot_cache),

    State = #state{ops_cache=OpsCache, snapshot_cache=SnapshotCache, partition=Partition, is_ready=false},
    internal_read(Key, Type, SnapshotTime, TxId, PropertyList, false, State).

%%@doc write operation to cache for future read, updates are stored
%%     one at a time into the ets tables
-spec update(key(), clocksi_payload()) -> ok | {error, reason()}.
update(Key, DownstreamOp) ->
    IndexNode = log_utilities:get_key_partition(Key),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp},
                                        materializer_vnode_master).

%%@doc write snapshot to cache for future read, snapshots are stored
%%     one at a time into the ets table
-spec store_ss(key(), #materialized_snapshot{}, snapshot_time()) -> ok.
store_ss(Key, Snapshot, CommitTime) ->
    IndexNode = log_utilities:get_key_partition(Key),
    riak_core_vnode_master:command(IndexNode, {store_ss, Key, Snapshot, CommitTime},
                                        materializer_vnode_master).

init([Partition]) ->
    OpsCache = open_table(Partition, ops_cache),
    SnapshotCache = open_table(Partition, snapshot_cache),
    IsReady = case application:get_env(antidote, recover_from_log) of
                {ok, true} ->
                    lager:debug("Trying to recover the materializer from log ~p", [Partition]),
                    riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
                    false;
                _ ->
                    true
    end,
    {ok, #state{is_ready = IsReady, partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache}}.


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

handle_command({hello}, _Sender, State) ->
  {reply, ok, State};

handle_command({check_ready}, _Sender, State = #state{partition=Partition, is_ready=IsReady}) ->
    Result = case ets:info(get_cache_name(Partition, ops_cache)) of
                 undefined ->
                     false;
                 _ ->
                     case ets:info(get_cache_name(Partition, snapshot_cache)) of
                         undefined ->
                             false;
                         _ ->
                             true
                     end
             end,
    Result2 = Result and IsReady,
    {reply, Result2, State};

handle_command({read, Key, Type, SnapshotTime, TxId}, _Sender, State) ->
    {reply, read(Key, Type, SnapshotTime, TxId, [], State), State};

handle_command({update, Key, DownstreamOp}, _Sender, State) ->
    true = op_insert_gc(Key, DownstreamOp, State),
    {reply, ok, State};

handle_command({store_ss, Key, Snapshot, CommitTime}, _Sender, State) ->
    internal_store_ss(Key, Snapshot, CommitTime, false, State),
    {noreply, State};

handle_command(load_from_log, _Sender, State=#state{partition=Partition}) ->
    IsReady = try
                case load_from_log_to_tables(Partition, State) of
                    ok ->
                        lager:debug("Finished loading from log to materializer on partition ~w", [Partition]),
                        true;
                    {error, not_ready} ->
                        false;
                    {error, Reason} ->
                        lager:error("Unable to load logs from disk: ~w, continuing", [Reason]),
                        true
                end
            catch
                _:Reason1 ->
                    lager:debug("Error loading from log ~w, will retry", [Reason1]),
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

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0},
                       _Sender,
                       State = #state{ops_cache = OpsCache}) ->
    F = fun(Key, A) ->
            [Key1|_] = tuple_to_list(Key),
            Fun(Key1, Key, A)
        end,
    Acc = ets:foldl(F, Acc0, OpsCache),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State=#state{ops_cache=OpsCache}) ->
    {_Key, Operation} = binary_to_term(Data),
    true = ets:insert(OpsCache, Operation),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{ops_cache=OpsCache}) ->
    case ets:first(OpsCache) of
        '$end_of_table' ->
            {true, State};
        _ ->
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

terminate(_Reason, _State=#state{ops_cache=OpsCache, snapshot_cache=SnapshotCache}) ->
    try
        ets:delete(OpsCache),
        ets:delete(SnapshotCache)
    catch
        _:_Reason->
            ok
    end,
    ok.



%%---------------- Internal Functions -------------------%%

-spec get_cache_name(non_neg_integer(), atom()) -> atom().
get_cache_name(Partition, Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).

-spec load_from_log_to_tables(partition_id(), #state{}) -> ok | {error, reason()}.
load_from_log_to_tables(Partition, State) ->
    LogId = [Partition],
    Node = {Partition, log_utilities:get_my_node(Partition)},
    loop_until_loaded(Node, LogId, start, dict:new(), State).

-spec loop_until_loaded({partition_id(), node()},
            log_id(),
            start | disk_log:continuation(),
            dict:dict(txid(), [any_log_payload()]),
            #state{}) ->
                   ok | {error, reason()}.
loop_until_loaded(Node, LogId, Continuation, Ops, State) ->
    case logging_vnode:get_all(Node, LogId, Continuation, Ops) of
        {error, Reason} ->
            {error, Reason};
        {NewContinuation, NewOps, OpsDict} ->
            load_ops(OpsDict, State),
            loop_until_loaded(Node, LogId, NewContinuation, NewOps, State);
        {eof, OpsDict} ->
            load_ops(OpsDict, State),
            ok
    end.

-spec load_ops(dict:dict(key(), [{non_neg_integer(), clocksi_payload()}]), #state{}) -> true.
load_ops(OpsDict, State) ->
    dict:fold(fun(Key, CommittedOps, _Acc) ->
                  lists:foreach(fun({_OpId, Op}) ->
                                    #clocksi_payload{key = Key} = Op,
                                    op_insert_gc(Key, Op, State)
                                end, CommittedOps)
              end, true, OpsDict).

-spec open_table(partition_id(), 'ops_cache' | 'snapshot_cache') -> atom() | ets:tid().
open_table(Partition, Name) ->
    case ets:info(get_cache_name(Partition, Name)) of
        undefined ->
            ets:new(get_cache_name(Partition, Name),
                [set, protected, named_table, ?TABLE_CONCURRENCY]);
        _ ->
            %% Other vnode hasn't finished closing tables
            lager:debug("Unable to open ets table in materializer vnode, retrying"),
            timer:sleep(100),
            try
                ets:delete(get_cache_name(Partition, Name))
            catch
                _:_Reason ->
                    ok
            end,
            open_table(Partition, Name)
    end.


-spec internal_store_ss(key(), #materialized_snapshot{}, snapshot_time(), boolean(), #state{}) -> boolean().
internal_store_ss(Key, Snapshot = #materialized_snapshot{last_op_id = NewOpId}, CommitTime, ShouldGc, State = #state{snapshot_cache=SnapshotCache}) ->
    SnapshotDict = case ets:lookup(SnapshotCache, Key) of
                       [] ->
                           vector_orddict:new();
                       [{_, SnapshotDictA}] ->
                           SnapshotDictA
                   end,
    %% Check if this snapshot is newer than the ones already in the cache. Since reads are concurrent multiple
    %% insert requests for the same snapshot could have occured
    ShouldInsert =
        case vector_orddict:size(SnapshotDict) > 0 of
            true ->
                {_Vector, #materialized_snapshot{last_op_id = OldOpId}} = vector_orddict:first(SnapshotDict),
                ((NewOpId - OldOpId) >= ?MIN_OP_STORE_SS);
            false -> true
        end,
    case (ShouldInsert or ShouldGc) of
        true ->
            SnapshotDict1 = vector_orddict:insert_bigger(CommitTime, Snapshot, SnapshotDict),
            snapshot_insert_gc(Key, SnapshotDict1, ShouldGc, State);
        false ->
            false
    end.

%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
-spec internal_read(key(), type(), snapshot_time(), txid() | ignore, clocksi_readitem_server:read_property_list(), boolean(), #state{})
           -> {ok, snapshot()} | {error, no_snapshot}.

internal_read(Key, Type, MinSnapshotTime, TxId, _PropertyList, ShouldGc, State) ->
    %% First look for any existing snapshots in the cache that is compatible with
    SnapshotGetResp = get_from_snapshot_cache(TxId, Key, Type, MinSnapshotTime, State),

    %% Now apply the operations to the snapshot, and return a materialized value
    materialize_snapshot(TxId, Key, Type, MinSnapshotTime, ShouldGc, State, SnapshotGetResp).

%% @doc Get the most recent snapshot from the cache (smaller than the given commit time) for a given key.
%%
%%      If there's no in-memory suitable snapshot, it will fetch it from the replication log.
%%
-spec get_from_snapshot_cache(txid() | ignore, key(), type(), snapshot_time(), #state{}) -> #snapshot_get_response{}.

get_from_snapshot_cache(TxId, Key, Type, MinSnaphsotTime, State = #state{
            ops_cache=OpsCache,
            snapshot_cache=SnapshotCache
        }) ->
    case ets:lookup(SnapshotCache, Key) of
        [] ->
            EmptySnapshot = #materialized_snapshot{
                last_op_id=0,
                value=clocksi_materializer:new(Type)
            },
            store_snapshot(TxId, Key, EmptySnapshot, vectorclock:new(), false, State),
            %% Create a base version committed at time ignore, i.e. bottom
            BaseVersion = {{ignore, EmptySnapshot}, true},
            update_snapshot_from_cache(BaseVersion, Key, OpsCache);

        [{_, SnapshotDict}] ->
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

-spec get_from_snapshot_log(key(), type(), snapshot_time()) -> #snapshot_get_response{}.
get_from_snapshot_log(Key, Type, SnapshotTime) ->
    LogId = log_utilities:get_logid_from_key(Key),
    Partition = log_utilities:get_key_partition(Key),
    logging_vnode:get_up_to_time(Partition, LogId, SnapshotTime, Type, Key).

%% @doc Store a new key snapshot in the in-memory cache at the given commit time.
%%
%%      If `ShouldGC' is true, it will try to prune the in-memory cache before inserting.
%%
-spec store_snapshot(txid() | ignore, key(), #materialized_snapshot{}, snapshot_time(), boolean(), #state{}) -> ok.
store_snapshot(TxId, Key, Snapshot, Time, ShouldGC, State) ->
    %% AB: Why don't we need to synchronize through the gen_server if the TxId is ignore??
     case TxId of
         ignore ->
             internal_store_ss(Key, Snapshot, Time, ShouldGC, State),
             ok;
         _ ->
             materializer_vnode:store_ss(Key, Snapshot, Time)
     end.

%% @doc Given a snapshot from the cache, update it from the ops cache.
-spec update_snapshot_from_cache({{snapshot_time() | ignore, #materialized_snapshot{}}, boolean()}, key(), cache_id())
    -> #snapshot_get_response{}.

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
-spec fetch_updates_from_cache(cache_id(), key()) -> {[op_and_id()] | tuple(), integer()}.
fetch_updates_from_cache(OpsCache, Key) ->
    case ets:lookup(OpsCache, Key) of
        [] ->
            {[], 0};

        [Tuple] ->
            {Key, Length, _OpId, _ListLen, CachedOps} = deconstruct_opscache_entry(Tuple),
            {CachedOps, Length}
    end.

-spec materialize_snapshot(txid() | ignore, key(), type(), snapshot_time(), boolean(), #state{}, #snapshot_get_response{})
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
                            store_snapshot(TxId, Key, ToCache, CommitTime, ShouldGC, State)
                     end,
                    {ok, MaterializedSnapshot}
            end
    end.

%% @doc Operation to insert a Snapshot in the cache and start
%%      Garbage collection triggered by reads.
-spec snapshot_insert_gc(key(), vector_orddict:vector_orddict(),
                         boolean(), #state{}) -> true.
snapshot_insert_gc(Key, SnapshotDict, ShouldGc, #state{snapshot_cache = SnapshotCache, ops_cache = OpsCache})->
    %% Perform the garbage collection when the size of the snapshot dict passed the threshold
    %% or when a GC is forced (a GC is forced after every ?OPS_THRESHOLD ops are inserted into the cache)
    %% Should check op size here also, when run from op gc
    case ((vector_orddict:size(SnapshotDict))>=?SNAPSHOT_THRESHOLD) orelse ShouldGc of
        true ->
            %% snapshots are no longer totally ordered
            PrunedSnapshots = vector_orddict:sublist(SnapshotDict, 1, ?SNAPSHOT_MIN),
            FirstOp = vector_orddict:last(PrunedSnapshots),
            {CT, _S} = FirstOp,
            CommitTime = lists:foldl(fun({CT1, _ST}, Acc) ->
                                         vectorclock:min([CT1, Acc])
                                     end, CT, vector_orddict:to_list(PrunedSnapshots)),
            {Key, Length, OpId, ListLen, OpsDict} =
                case ets:lookup(OpsCache, Key) of
                    [] ->
                        {Key, 0, 0, 0, {}};
                    [Tuple] ->
                        deconstruct_opscache_entry(Tuple)
                end,
            {NewLength, PrunedOps} = prune_ops({Length, OpsDict}, CommitTime),
            true = ets:insert(SnapshotCache, {Key, PrunedSnapshots}),
            %% Check if the pruned ops are larger or smaller than the previous list size
            %% if so create a larger or smaller list (by dividing or multiplying by 2)
            %% (Another option would be to shrink to a more "minimum" size, but need to test to see what is better)
            NewListLen = case NewLength > ListLen - ?RESIZE_THRESHOLD of
                         true ->
                             ListLen * 2;
                         false ->
                             HalfListLen = ListLen div 2,
                         case HalfListLen =< ?OPS_THRESHOLD of
                             true ->
                                 %% Don't shrink list, already minimun size
                                 ListLen;
                             false ->
                                 %% Only shrink if shrinking would leave some space for new ops
                                 case HalfListLen - ?RESIZE_THRESHOLD > NewLength of
                                     true ->
                                         HalfListLen;
                                     false ->
                                         ListLen
                                 end
                         end
                     end,
        NewTuple = erlang:make_tuple(?FIRST_OP+NewListLen, 0, [{1, Key}, {2, {NewLength, NewListLen}}, {3, OpId}|PrunedOps]),
        true = ets:insert(OpsCache, NewTuple);
    false ->
        true = ets:insert(SnapshotCache, {Key, SnapshotDict})
    end.

%% @doc Remove from OpsDict all operations that have committed before Threshold.
-spec prune_ops({non_neg_integer(), tuple()}, snapshot_time())->
               {non_neg_integer(), [{non_neg_integer(), op_and_id()}]}.
prune_ops({Len, OpsTuple}, Threshold)->
    %% should write custom function for this in the vector_orddict
    %% or have to just traverse the entire list?
    %% since the list is ordered, can just stop when all values of
    %% the op is smaller (i.e. not concurrent)
    %% So can add a stop function to ordered_filter
    %% Or can have the filter function return a tuple, one vale for stopping
    %% one for including
    {NewSize, NewOps} = check_filter(fun({_OpId, Op}) ->
                                         OpCommitTime=Op#clocksi_payload.commit_time,
                                         (materializer:belongs_to_snapshot_op(Threshold, OpCommitTime, Op#clocksi_payload.snapshot_time))
                                     end, ?FIRST_OP, ?FIRST_OP+Len, ?FIRST_OP, OpsTuple, 0, []),
    case NewSize of
        0 ->
            First = element(?FIRST_OP+Len, OpsTuple),
            {1, [{?FIRST_OP, First}]};
        _ -> {NewSize, NewOps}
    end.

%% This function will go through a tuple of operations, filtering out the operations
%% that are out of date (given by the input function Fun), and returning a list
%% of the remaining operations and the size of that list
%% It is used during garbage collection to filter out operations that are older than any
%% of the cached snapshots
-spec check_filter(fun(({non_neg_integer(), #clocksi_payload{}}) -> boolean()), non_neg_integer(), non_neg_integer(),
           non_neg_integer(), tuple(), non_neg_integer(), [{non_neg_integer(), op_and_id()}]) ->
              {non_neg_integer(), [{non_neg_integer(), op_and_id()}]}.
check_filter(_Fun, Id, Last, _NewId, _Tuple, NewSize, NewOps) when (Id == Last) ->
    {NewSize, NewOps};
check_filter(Fun, Id, Last, NewId, Tuple, NewSize, NewOps) ->
    Op = element(Id, Tuple),
    case Fun(Op) of
        true ->
            check_filter(Fun, Id+1, Last, NewId+1, Tuple, NewSize+1, [{NewId, Op}|NewOps]);
        false ->
            check_filter(Fun, Id+1, Last, NewId, Tuple, NewSize, NewOps)
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
-spec op_insert_gc(key(), clocksi_payload(), #state{}) -> true.
op_insert_gc(Key, DownstreamOp, State = #state{ops_cache = OpsCache}) ->
    %% Check whether there is an ops cache entry for the key
    case ets:member(OpsCache, Key) of
        false ->
            ets:insert(OpsCache, erlang:make_tuple(?FIRST_OP+?OPS_THRESHOLD, 0, [{1, Key}, {2, {0, ?OPS_THRESHOLD}}]));
        true ->
            ok
    end,
    NewId = ets:update_counter(OpsCache, Key, {3, 1}),
    {Length, ListLen} = ets:lookup_element(OpsCache, Key, 2),

    %% Perform GC by triggering a read when there are more than OPS_THRESHOLD
    %% operations for a given key or when the list is full
    case (Length >= ListLen) orelse ((NewId rem ?OPS_THRESHOLD) == 0) of
        true ->
            Type = DownstreamOp#clocksi_payload.type,
            SnapshotTime = DownstreamOp#clocksi_payload.snapshot_time,
            %% Here is where the GC is done (with the 5th argument being "true", GC is performed by the internal read
            {_, _} = internal_read(Key, Type, SnapshotTime, ignore, [], true, State),
            %% Have to obtain again the list/tuple information because the internal_read can change it
            {NewLength, NewListLen} = ets:lookup_element(OpsCache, Key, 2),
            %% Insert the new op
            true = ets:update_element(OpsCache, Key, [{NewLength+?FIRST_OP, {NewId, DownstreamOp}}, {2, {NewLength+1, NewListLen}}]);
        false ->
            true = ets:update_element(OpsCache, Key, [{Length+?FIRST_OP, {NewId, DownstreamOp}}, {2, {Length+1, ListLen}}])
    end.

-ifdef(TEST).

%% This tests to make sure when garbage collection happens, no updates are lost
gc_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    DC1 = 1,
    Type = antidote_crdt_counter_pn,

    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Make max. number of snapshots
    lists:map(
      fun(N) ->
        {ok, Res} = internal_read(Key, Type, vectorclock:from_list([{DC1, N * 10 + 2}]), ignore, [], false, State),
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
    SnapshotCache = ets:new(snapshot_cache, [set]),
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
    SnapshotCache = ets:new(snapshot_cache, [set]),
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
    SnapshotCache = ets:new(snapshot_cache, [set]),
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
    SnapshotCache = ets:new(snapshot_cache, [set]),
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
    io:format("Result1 = ~p", [ReadDC1]),
    {ok, ReadDC2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 0}, {DC2, 1}]), ignore, [], false, State),
    io:format("Result2 = ~p", [ReadDC2]),
    ?assertEqual(1, Type:value(ReadDC2)),

    %% Read snapshot including both increments
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC2, 1}, {DC1, 1}]), ignore, [], false, State),
    ?assertEqual(2, Type:value(Res2)).

%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%% E.g., for a gcounter, return 0.
read_nonexisting_key_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},
    Type = antidote_crdt_counter_pn,
    {ok, ReadResult} = internal_read(key, Type, vectorclock:from_list([{dc1, 1}, {dc2, 0}]), ignore, [], false, State),
    ?assertEqual(0, Type:value(ReadResult)).

-endif.
