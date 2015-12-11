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
-module(ec_materializer_vnode).

-behaviour(riak_core_vnode).

-include("ec_antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
    check_tables_ready/0,
    read/5,
    get_cache_name/2,
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
    handle_exit/3]).


-record(state, {
    partition :: partition_id(),
    ops_cache :: cache_id(),
    snapshot_cache :: cache_id()}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time, this does not touch the vnode process
%%      directly, instead it just reads from the operations and snapshot tables that
%%      are in shared memory, allowing concurrent reads.
-spec read(key(), type(), cache_id(), cache_id(), partition_id()) -> {ok, snapshot()} | {error, reason()}.
read(Key, Type, OpsCache, SnapshotCache, Partition) ->
    case ets:info(OpsCache) of
        undefined ->
            lager:info("interactive_coord: opscache undefined"),
            riak_core_vnode_master:sync_command({Partition, node()},
                {read, Key, Type},
                ec_materializer_vnode_master,
                infinity);
        _ ->
            lager:info("interactive_coord: running internal_read"),
            internal_read(Key, Type, OpsCache, SnapshotCache)
    end.

-spec get_cache_name(non_neg_integer(), atom()) -> atom().
get_cache_name(Partition, Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).

%%@doc write operation to cache for future read, updates are stored
%%     one at a time into the ets tables
-spec update(key(), ec_payload()) -> ok | {error, reason()}.
update(Key, DownstreamOp) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp},
        ec_materializer_vnode_master).

%%@doc write snapshot to cache for future read, snapshots are stored
%%     one at a time into the ets table
-spec store_ss(key(), snapshot(), snapshot_time()) -> ok.
store_ss(Key, Snapshot, CommitTime) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:command(IndexNode, {store_ss, Key, Snapshot, CommitTime},
        ec_materializer_vnode_master).

init([Partition]) ->
    OpsCache = ets:new(get_cache_name(Partition, ops_cache), [set, protected, named_table, ?TABLE_CONCURRENCY]),
    SnapshotCache = ets:new(get_cache_name(Partition, snapshot_cache), [set, protected, named_table, ?TABLE_CONCURRENCY]),
    {ok, #state{partition = Partition, ops_cache = OpsCache, snapshot_cache = SnapshotCache}}.

%% @doc The tables holding the updates and snapshots are shared with concurrent
%%      readers, allowing them to be non-blocking and concurrent.
%%      This function checks whether or not all tables have been intialized or not yet.
%%      Returns true if the have, false otherwise.
check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).


check_table_ready([]) ->
    true;
check_table_ready([{Partition, Node} | Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition, Node},
        {check_ready},
        ec_materializer_vnode_master,
        infinity),
    case Result of
        true ->
            check_table_ready(Rest);
        false ->
            false
    end.

handle_command({check_ready}, _Sender, State = #state{partition = Partition}) ->
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
    {reply, Result, State};


handle_command({read, Key, Type}, _Sender,
  State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache, partition = Partition}) ->
    {reply, read(Key, Type, OpsCache, SnapshotCache, Partition), State};

handle_command({update, Key, DownstreamOp}, _Sender,
  State = #state{ops_cache = OpsCache}) ->
    true = op_insert(Key, DownstreamOp, OpsCache),
    {reply, ok, State};


handle_command({store_ss, Key, Snapshot, CommitTime}, _Sender,
  State = #state{snapshot_cache = SnapshotCache}) ->
    internal_store_ss(Key, Snapshot, CommitTime, SnapshotCache),
    {noreply, State};


handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun = Fun, acc0 = Acc0},
  _Sender,
  State = #state{ops_cache = OpsCache}) ->
    F = fun({Key, Operation}, A) ->
        Fun(Key, Operation, A)
        end,
    Acc = ets:foldl(F, Acc0, OpsCache),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State = #state{ops_cache = OpsCache}) ->
    {Key, Operation} = binary_to_term(Data),
    true = ets:insert(OpsCache, {Key, Operation}),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State = #state{ops_cache = OpsCache}) ->
    case ets:first(OpsCache) of
        '$end_of_table' ->
            {true, State};
        _ ->
            {false, State}
    end.

delete(State = #state{ops_cache = _OpsCache}) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State = #state{ops_cache = OpsCache, snapshot_cache = SnapshotCache}) ->
    ets:delete(OpsCache),
    ets:delete(SnapshotCache),
    ok.


%%---------------- Internal Functions -------------------%%

-spec internal_store_ss(key(), snapshot(), commit_time(), cache_id()) -> true.
internal_store_ss(Key, Snapshot, CommitTime, SnapshotCache) ->
    snapshot_insert(Key, Snapshot, CommitTime, SnapshotCache).


%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
-spec internal_read(key(), type(), cache_id(), cache_id()) -> {ok, snapshot()}.
internal_read(Key, Type, OpsCache, SnapshotCache) ->
    {LatestSnapshot, LatestCommitTime} = case ets:lookup(SnapshotCache, Key) of
                                             [] ->
                                                 {ec_materializer:new(Type), ignore};
                                             [{_, {StoredCommitTime, StoredSnapshot}}] ->
                                                 {StoredSnapshot, StoredCommitTime}
                                         end,
    case ets:lookup(OpsCache, Key) of
        [] ->
            {ok, LatestSnapshot};
        {_, [{Op1CommitTime, Op1}]} ->
            case (LatestCommitTime = ignore or vectorclock:is_greater_than(Op1CommitTime, LatestCommitTime)) of
                true ->
                    lager:info("materializer_vnode: about to materialize to Type: ~p, LatestSnapshot=~p, Operation:~p",[Type, LatestSnapshot, Op1]),
                    case ec_materializer:materialize_eager(Type, LatestSnapshot, Op1) of
                        {error, Reason} ->
                            {error, Reason};
                        UpdatedSnapshot ->
                            internal_store_ss(Key, UpdatedSnapshot, Op1CommitTime, SnapshotCache)
                    end
            end
    end.


%% @doc Operation to insert a Snapshot in the cache
-spec snapshot_insert(key(), snapshot(), commit_time(), cache_id()) -> true.
snapshot_insert(Key, Snapshot, SnapshotCommitTime, SnapshotCache) ->
    ets:insert(SnapshotCache, {Key, {SnapshotCommitTime, Snapshot}}).


%% @doc Insert an operation
-spec op_insert(key(), ec_payload(), cache_id()) -> true.
op_insert(Key, DownstreamOp, OpsCache) ->
    OpCommitTime = DownstreamOp#ec_payload.commit_time,
    case ets:lookup(OpsCache, Key) of
        [] ->
            ets:insert(OpsCache, {Key, {OpCommitTime, DownstreamOp}});
        [{_, {StoredCommitTime, _}}] ->
            case OpCommitTime > StoredCommitTime of
                true ->
                    ets:insert(OpsCache, {Key, {DownstreamOp#ec_payload.commit_time, DownstreamOp}});
                _ ->
                    true
            end
    end.
%%-ifdef(TEST).
%%
%%
%%generate_payload(CommitTime, Prev, Name) ->
%%    Key = mycount,
%%    Type = riak_dt_gcounter,
%%
%%    {ok, Op1} = Type:update(increment, Name, Prev),
%%    #ec_payload{key = Key,
%%        type = Type,
%%        op_param = {merge, Op1},
%%        commit_time = CommitTime,
%%        txid = 1
%%    }.
%%
%%
%%
%%seq_write_test() ->
%%    OpsCache = ets:new(ops_cache, [set]),
%%    SnapshotCache = ets:new(snapshot_cache, [set]),
%%    Key = mycount,
%%    Type = riak_dt_gcounter,
%%    DC1 = 1,
%%    S1 = Type:new(),
%%
%%    %% Insert one increment
%%    {ok, Op1} = Type:update(increment, a, S1),
%%    DownstreamOp1 = #ec_payload{key = Key,
%%        type = Type,
%%        op_param = {merge, Op1},
%%        snapshot_time = vectorclock:from_list([{DC1, 10}]),
%%        commit_time = {DC1, 15},
%%        txid = 1
%%    },
%%    op_insert(Key, DownstreamOp1, OpsCache),
%%    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}]), ignore, OpsCache),
%%    ?assertEqual(1, Type:value(Res1)),
%%    %% Insert second increment
%%    {ok, Op2} = Type:update(increment, a, Res1),
%%    DownstreamOp2 = DownstreamOp1#ec_payload{
%%        op_param = {merge, Op2},
%%        snapshot_time = vectorclock:from_list([{DC1, 16}]),
%%        commit_time = {DC1, 20},
%%        txid = 2},
%%
%%    op_insert(Key, DownstreamOp2, OpsCache),
%%    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 21}]), ignore, OpsCache),
%%    ?assertEqual(2, Type:value(Res2)),
%%
%%    %% Read old version
%%    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}]), ignore, OpsCache),
%%    ?assertEqual(1, Type:value(ReadOld)).
%%
%%
%%multipledc_write_test() ->
%%    OpsCache = ets:new(ops_cache, [set]),
%%    SnapshotCache = ets:new(snapshot_cache, [set]),
%%    Key = mycount,
%%    Type = riak_dt_gcounter,
%%    DC1 = 1,
%%    DC2 = 2,
%%    S1 = Type:new(),
%%
%%    %% Insert one increment in DC1
%%    {ok, Op1} = Type:update(increment, a, S1),
%%    DownstreamOp1 = #ec_payload{key = Key,
%%        type = Type,
%%        op_param = {merge, Op1},
%%        snapshot_time = vectorclock:from_list([{DC2, 0}, {DC1, 10}]),
%%        commit_time = {DC1, 15},
%%        txid = 1
%%    },
%%    op_insert(Key, DownstreamOp1, OpsCache),
%%    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}, {DC2, 0}]), ignore, OpsCache),
%%    ?assertEqual(1, Type:value(Res1)),
%%
%%    %% Insert second increment in other DC
%%    {ok, Op2} = Type:update(increment, b, Res1),
%%    DownstreamOp2 = DownstreamOp1#ec_payload{
%%        op_param = {merge, Op2},
%%        snapshot_time = vectorclock:from_list([{DC2, 16}, {DC1, 16}]),
%%        commit_time = {DC2, 20},
%%        txid = 2},
%%
%%    op_insert(Key, DownstreamOp2, OpsCache),
%%    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}, {DC2, 21}]), ignore, OpsCache),
%%    ?assertEqual(2, Type:value(Res2)),
%%
%%    %% Read old version
%%    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1, 15}, {DC2, 15}]), ignore, OpsCache),
%%    ?assertEqual(1, Type:value(ReadOld)).
%%
%%concurrent_write_test() ->
%%    OpsCache = ets:new(ops_cache, [set]),
%%    SnapshotCache = ets:new(snapshot_cache, [set]),
%%    Key = mycount,
%%    Type = riak_dt_gcounter,
%%    DC1 = local,
%%    DC2 = remote,
%%    S1 = Type:new(),
%%
%%    %% Insert one increment in DC1
%%    {ok, Op1} = Type:update(increment, a, S1),
%%    DownstreamOp1 = #ec_payload{key = Key,
%%        type = Type,
%%        op_param = {merge, Op1},
%%        snapshot_time = vectorclock:from_list([{DC1, 0}, {DC2, 0}]),
%%        commit_time = {DC2, 1},
%%        txid = 1
%%    },
%%    op_insert(Key, DownstreamOp1, OpsCache),
%%    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC2, 1}, {DC1, 0}]), ignore, OpsCache),
%%    ?assertEqual(1, Type:value(Res1)),
%%
%%    %% Another concurrent increment in other DC
%%    {ok, Op2} = Type:update(increment, b, S1),
%%    DownstreamOp2 = #ec_payload{key = Key,
%%        type = Type,
%%        op_param = {merge, Op2},
%%        snapshot_time = vectorclock:from_list([{DC1, 0}, {DC2, 0}]),
%%        commit_time = {DC1, 1},
%%        txid = 2},
%%    op_insert(Key, DownstreamOp2, OpsCache),
%%
%%    %% Read different snapshots
%%    {ok, ReadDC1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 1}, {DC2, 0}]), ignore, OpsCache),
%%    ?assertEqual(1, Type:value(ReadDC1)),
%%    io:format("Result1 = ~p", [ReadDC1]),
%%    {ok, ReadDC2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 0}, {DC2, 1}]), ignore, OpsCache),
%%    io:format("Result2 = ~p", [ReadDC2]),
%%    ?assertEqual(1, Type:value(ReadDC2)),
%%
%%    %% Read snapshot including both increments
%%    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC2, 1}, {DC1, 1}]), ignore, OpsCache),
%%    ?assertEqual(2, Type:value(Res2)).
%%
%%%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%%%% E.g., for a gcounter, return 0.
%%read_nonexisting_key_test() ->
%%    OpsCache = ets:new(ops_cache, [set]),
%%    SnapshotCache = ets:new(snapshot_cache, [set]),
%%    Type = riak_dt_gcounter,
%%    {ok, ReadResult} = internal_read(key, Type, vectorclock:from_list([{dc1, 1}, {dc2, 0}]), ignore, OpsCache),
%%    ?assertEqual(0, Type:value(ReadResult)).
%%
%%
%%-endif.
