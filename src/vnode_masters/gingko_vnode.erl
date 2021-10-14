-module(gingko_vnode).
-behaviour(riak_core_vnode).
-include("gingko.hrl").
-export([start_vnode/1,
    init/1,
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
    handle_overload_command/3,
    handle_overload_info/2,
    handle_coverage/4,
    handle_exit/3]).
%%----------------External API -------------------%%
-export([
    update/5,
    prepare/3,
    commit/4,
    commit/5,
    abort/2,
    get_version/3,
    get_version/5,
    set_stable/1, %%TODO: Implement for the checkpoint store,
    get_stats/0
]).

-ignore_xref([
    start_vnode/1
]).

-record(state, {partition, gingko}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    GingkoSupervisionTree = gingko_sup:start_link(Partition),
    {ok, #state { partition=Partition ,gingko = GingkoSupervisionTree}}.


%%====================================================================
%% API functions
%%====================================================================

%% @equiv get_version(Key, Type, undefined)
-spec get_version(term(),key(), type()) -> {ok, snapshot()}.
get_version(TxId, Key, Type) ->
    get_version(TxId, Key, Type, ignore, ignore).

%% @doc Retrieves a materialized version of the object at given key with expected given type.
%% If MaximumSnapshotTime is given, then the version is guaranteed to not be older than the given snapshot.
%%
%% Example usage:
%%
%% Operations of a counter @my_counter in the log: +1, +1, -1, +1(not committed), -1(not committed).
%%
%% 2 = get_version(my_counter, antidote_crdt_counter_pn, undefined)
%%
%% @param Key the Key under which the object is stored
%% @param Type the expected CRDT type of the object
%% @param MaximumSnapshotTime if not 'undefined', then retrieves the latest object version which is not older than this timestamp
-spec get_version(key(), type(),txid(), snapshot_time(),snapshot_time()) -> {ok, snapshot()}.
get_version(Key, Type,TxId, MinimumSnapshotTime, MaximumSnapshotTime ) ->
    logger:debug(#{function => "GET_VERSION", key => Key, type => Type, min_snapshot_timestamp => MinimumSnapshotTime, max_snapshot_timestamp => MaximumSnapshotTime}),
    IndexNode = antidote_riak_utilities:get_key_partition(Key),
    riak_core_vnode_master:sync_spawn_command(IndexNode, {get_version, TxId, Key, Type, MinimumSnapshotTime,MaximumSnapshotTime}, gingko_vnode_master).


%% @doc Applies an update for the given key for given transaction id with a calculated valid downstream operation.
%% It is currently not checked if the downstream operation is valid for given type.
%% Invalid downstream operations will corrupt a key, which will cause get_version to throw an error upon invocation.
%%
%% A update log record consists of the transaction id, the op_type 'update' and the actual payload.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%%
%% @param Key the Key under which the object is stored
%% @param Type the expected CRDT type of the object
%% @param TransactionId the id of the transaction this update belongs to
%% @param DownstreamOp the calculated downstream operation of a CRDT update
-spec update(key(), type(), txid(), op(), {atom(), atom(), pid()}) -> ok | {error, reason()}.
update(Key, Type, TransactionId, DownstreamOp, ReplyTo) ->
    logger:debug(#{function => "UPDATE", key => Key, type => Type, transaction => TransactionId, op => DownstreamOp}),
    IndexNode = antidote_riak_utilities:get_key_partition(Key),
    riak_core_vnode_master:command(IndexNode, {update, Key, Type, TransactionId,DownstreamOp}, ReplyTo, gingko_vnode_master).



%% @doc Adds a prepare entry to the log of the partition.
%%
%% The prepare payload contains the key and the transaction ID for which the prepare message was sent.
%% The prepare timestamp is the dc microsecond.
%%
%% @param Keys list of keys to prepare
%% @param TransactionId the id of the transaction this prepare belongs to
%% @param PrepareTimestamp TODO

prepare(Key, TransactionId, PrepareTimestamp) ->
    logger:debug(#{function => "PREPARE", key => Key, transaction => TransactionId}),
    IndexNode = antidote_riak_utilities:get_key_partition(Key),
    riak_core_vnode_master:command(IndexNode, {prepare,TransactionId,PrepareTimestamp}, gingko_vnode_master).

%% @doc Commits all operations belonging to given transaction id for given list of keys.
%%
%% A commit log record consists of the transaction id, the op_type 'commit'
%% and the actual payload which consists of the commit time and the snapshot time.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%%
%% @param Keys list of keys to commit
%% @param TransactionId the id of the transaction this commit belongs to
%% @param CommitTime TODO
%% @param SnapshotTime TODO
-spec commit(non_neg_integer(), txid(), list(), dc_and_commit_time()) -> ok.
commit(Partition, TransactionId, WriteSet, CommitTime)->
    commit(Partition, TransactionId,WriteSet, CommitTime, vectorclock:new()).
-spec commit([integer()], txid(),list(), dc_and_commit_time(), snapshot_time()) -> ok.
commit(Partition, TransactionId, WriteSet, CommitTime, SnapshotTime)->
    logger:debug(#{function => "COMMIT", partitions => Partition, transaction => TransactionId, commit_timestamp => CommitTime, snapshot_timestamp => SnapshotTime}),

    Entry = #log_operation{
        tx_id = TransactionId,
        op_type = commit,
        log_payload = #commit_log_payload{commit_time = CommitTime, snapshot_time = SnapshotTime}},

    LogRecord = #log_record {
        version = ?LOG_RECORD_VERSION,
        log_operation = Entry
    },
    riak_core_vnode_master:command(Partition, {commit, LogRecord, WriteSet}, gingko_vnode_master),
    ok.



%% @doc Aborts all operations belonging to given transaction id for given list of keys.
%%
%% An abort log record consists of the transaction id, the op_type 'abort'
%% and the actual payload which is empty.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%%
%% @param Keys list of keys to abort a transaction
%% @param TransactionId the id of the transaction to abort
-spec abort(non_neg_integer(), txid()) -> ok.
abort(Partition, TransactionId) ->
    logger:debug(#{function => "ABORT", keys => Partition, transaction => TransactionId}),

    Entry = #log_operation{
        tx_id = TransactionId,
        op_type = abort,
        log_payload = #abort_log_payload{}},

    LogRecord = #log_record {
        version = ?LOG_RECORD_VERSION,
        log_operation = Entry
    },
    riak_core_vnode_master:command(Partition, {abort, LogRecord}, gingko_vnode_master),
    ok.


%% @doc Sets a timestamp for when all operations below that timestamp are considered stable.
%%
%% Currently not implemented.
%% @param SnapshotTime TODO
-spec set_stable(snapshot_time()) -> ok.
set_stable(SnapshotTime) ->
    logger:warning(#{function => "SET_STABLE", timestamp => SnapshotTime, message => "not implemented"}),
    ok.


get_stats() ->
    gen_server:call(?CACHE_DAEMON, {get_event_stats}).


%% Sample command: respond to a ping
handle_command({hello}, _Sender, State) ->
    {reply, ok, State};
handle_command({get_version, TxId, Key, Type, MinimumSnapshotTime,MaximumSnapshotTime}, _Sender, State = #state{partition = Partition}) ->
    %% Ask the cache for the object.
    %% If tha cache has that object, it is returned.
    %% If the cache does not have it, it is materialised from the log and stored in the cache.
    %% All subsequent reads of the object will return from the cache without reading the whole log.

    {ok, {Key, Type, Value, Timestamp}} = cache_daemon:get_from_cache(TxId, Key,Type,MinimumSnapshotTime,MaximumSnapshotTime, Partition),
    logger:notice(#{step => "materialize", materialized => {Key, Type, Value, Timestamp}}),
    {reply,{ok, {Key, Type, Value}}, State};

handle_command({prepare, TransactionId,PrepareTimestamp}, _Sender, State = #state{partition = Partition}) ->
    Entry = #log_operation{
        tx_id = TransactionId,
        op_type = prepare,
        log_payload = #prepare_log_payload{prepare_time = PrepareTimestamp}},

    LogRecord = #log_record {
        version = ?LOG_RECORD_VERSION,

        log_operation = Entry
    },
    Result = gingko_op_log:append(LogRecord, Partition),
    {reply, Result, State};

handle_command({update, Key, Type, TransactionId,DownstreamOp}, _Sender, State = #state{partition = Partition}) ->
    Entry = #log_operation{
        tx_id = TransactionId,
        op_type = update,
        log_payload = #update_log_payload{key = Key, type = Type , op = DownstreamOp}},

    LogRecord = #log_record {
        version = ?LOG_RECORD_VERSION,
        log_operation = Entry
    },
    Result = gingko_op_log:append(LogRecord, Partition),
    {reply,Result, State};

handle_command({commit, LogRecord, WriteSet}, _Sender, State = #state{partition = Partition}) ->
    gingko_op_log:append(LogRecord, Partition),
    checkpoint_daemon:trigger_checkpoint(WriteSet),
    {reply, committed, State};

handle_command({abort, LogRecord}, _Sender, State = #state{partition = Partition}) ->
    gingko_op_log:append(LogRecord, Partition),
    {reply, aborted, State};

handle_command(Message, _Sender, State) ->
    logger:warning("unhandled_command ~p", [Message]),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

handle_overload_command(_, _, _) ->
    ok.

handle_overload_info(_, _Idx) ->
    ok.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.


