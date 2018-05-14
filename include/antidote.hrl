
-define(BUCKET, <<"antidote">>).
-define(MASTER, antidote_vnode_master).
-define(LOGGING_MASTER, logging_vnode_master).
-define(CLOCKSI_MASTER, clocksi_vnode_master).
-define(CLOCKSI_GENERATOR_MASTER, clocksi_downstream_generator_vnode_master).
-define(CLOCKSI, clocksi).
-define(REPMASTER, antidote_rep_vnode_master).
-define(N, 1).
-define(OP_TIMEOUT, infinity).
-define(COORD_TIMEOUT, infinity).
-define(COMM_TIMEOUT, infinity).
-define(ZMQ_TIMEOUT, 5000).
-define(NUM_W, 2).
-define(NUM_R, 2).
-define(DEFAULT_PUBSUB_PORT, 8086).
-define(DEFAULT_LOGREADER_PORT, 8085).

%% Allow read concurrency on shared ets tables
%% These are the tables that store materialized objects
%% and information about live transactions, so the assumption
%% is there will be several more reads than writes
-define(TABLE_CONCURRENCY, {read_concurrency,true}).
%% The read concurrency is the maximum number of concurrent
%% readers per vnode.  This is so shared memory can be used
%% in the case of keys that are read frequently.  There is
%% still only 1 writer per vnode
-define(READ_CONCURRENCY, 20).
%% The log reader concurrency is the pool of threads per
%% physical node that handle requests from other DCs
%% for example to request missing log operations
-define(INTER_DC_QUERY_CONCURRENCY, 20).
%% This defines the concurrency for the meta-data tables that
%% are responsible for storing the stable time that a transaction
%% can read.  It is set to false because the erlang docs say
%% you should only set to true if you have long bursts of either
%% reads or writes, but here they should be interleaved (maybe).  But should
%% do some performance testing.
-define(META_TABLE_CONCURRENCY, {read_concurrency, false}, {write_concurrency, false}).
-define(META_TABLE_STABLE_CONCURRENCY, {read_concurrency, true}, {write_concurrency, false}).
%% This can be used for testing, so that transactions start with
%% old snapshots to avoid clock-skew.
%% This can break the tests is not set to 0
-define(OLD_SS_MICROSEC,0).
%% The number of supervisors that are responsible for
%% supervising transaction coorinator fsms
-define(NUM_SUP, 100).
%% Threads will sleep for this length when they have to wait
%% for something that is not ready after which they
%% wake up and retry. I.e. a read waiting for
%% a transaction currently in the prepare state that is blocking
%% that read.
-define(SPIN_WAIT, 10).
%% HEARTBEAT_PERIOD: Period of sending the heartbeat messages in interDC layer
-define(HEARTBEAT_PERIOD, 1000).
%% VECTORCLOCK_UPDATE_PERIOD: Period of updates of the stable snapshot per partition
-define(VECTORCLOCK_UPDATE_PERIOD, 100).
%% This is the time that nodes will sleep inbetween sending meta-data
%% to other physical nodes within the DC
-define(META_DATA_SLEEP, 1000).
-define(META_TABLE_NAME, a_meta_data_table).
-define(REMOTE_META_TABLE_NAME, a_remote_meta_data_table).
-define(META_TABLE_STABLE_NAME, a_meta_data_table_stable).
%% At commit, if this is set to true, the logging vnode
%% will ensure that the transaction record is written to disk
-define(SYNC_LOG, false).
%% Uncomment the following line to use erlang:now()
%% Otherwise os:timestamp() is used which can go backwards
%% which is unsafe for clock-si
-define(SAFE_TIME, true).
%% Version of log records being used
-define(LOG_RECORD_VERSION, 0).
%% Bounded counter manager parameters.
%% Period during which transfer requests from the same DC to the same key are ignored.
-define(GRACE_PERIOD, 1000000). % in Microseconds
%% Time to forget a pending request.
-define(REQUEST_TIMEOUT, 500000). % In Microseconds
%% Frequency at which manager requests remote resources.
-define(TRANSFER_FREQ, 100). %in Milliseconds

%% The definition "FIRST_OP" is used by the materializer.
%% The materialzer caches a tuple for each key containing
%% information about the state of operations performed on that key.
%% The first 3 elements in the tuple are the following meta-data:
%% First is the key itself
%% Second is a tuple defined as {the number of update operations stored in the tupe, the size of the tuple}
%% Thrid is a counter that keeps track of how many update operations have been performed on this key
%% Fourth is where the first update operation is stored
%% The remaining elements are update operations
-define(FIRST_OP, 4).

-record(payload, {
    key :: key(),
    type :: type(),
    op_param,
    actor :: actor()
}).

-record(commit_log_payload, {
    commit_time :: dc_and_commit_time(),
    snapshot_time :: snapshot_time()
}).

-record(update_log_payload, {
    key :: key(),
    bucket :: bucket(),
    type :: type(),
    op :: op()
}).

-record(abort_log_payload, {}).

-record(prepare_log_payload, {prepare_time :: non_neg_integer()}).

-type any_log_payload() :: #update_log_payload{}
                         | #commit_log_payload{}
                         | #abort_log_payload{}
                         | #prepare_log_payload{}.

-record(log_operation, {
    tx_id :: txid(),
    op_type :: update
             | prepare
             | commit
             | abort
             | noop,
    log_payload :: any_log_payload()
}).

-record(op_number, {
    %% TODO 19 undefined is required here, because of the use in inter_dc_log_sender_vnode.
    %% The use there should be refactored.
    node :: undefined | {node(), dcid()},
    global :: undefined | non_neg_integer(),
    local :: undefined | non_neg_integer()
}).

%% The way records are stored in the log.
-record(log_record, {
    %% The version of the log record, for backwards compatability
    version :: non_neg_integer(),
    op_number :: #op_number{},
    bucket_op_number :: #op_number{},
    log_operation :: #log_operation{}
}).

%% Clock SI

%% MIN is Used for generating the timeStamp of a new snapshot
%% in the case that a client has already seen a snapshot time
%% greater than the current time at the replica it is starting
%% a new transaction.
-define(MIN, 1).

%% DELTA has the same meaning as in the clock-SI paper.
-define(DELTA, 10000).

-define(CLOCKSI_TIMEOUT, 1000).

-type txn_properties() :: [{update_clock, boolean()} | {certify, use_default | certify | dont_certify}].

-record(transaction, {
    snapshot_time :: snapshot_time(),
    vec_snapshot_time :: snapshot_time(),
    properties :: txn_properties(),
    txn_id :: txid()
}).

-record(materialized_snapshot, {
    %% This is the opid of the latest op in the list
    %% of ops for this key included in this snapshot
    %% before an op that was not included, so to a new
    %% snapshot will be generated by starting from this op
    last_op_id :: op_num(),
    value :: snapshot()
}).

%%---------------------------------------------------------------------
-type actor() :: term().
-type key() :: term().
-type type() :: atom().
-type op_name() :: atom().
-type op_param() :: term().
-type op() :: {op_name(), op_param()}.
-type effect() :: term().

-type dcid() :: 'undefined' | {atom(),tuple()}. %% TODO, is this the only structure that is returned by riak_core_ring:cluster_name(Ring)?
-type snapshot_time() :: vectorclock:vectorclock().
-type clock_time() :: non_neg_integer().
-type dc_and_commit_time() :: {dcid(), clock_time()}.

-record(tx_id, {
    local_start_time :: clock_time(),
    server_pid :: atom() | pid()
}).

-record(clocksi_payload, {
    key :: key(),
    type :: type(),
    op_param :: effect(),
    snapshot_time :: snapshot_time(),
    commit_time :: dc_and_commit_time(),
    txid :: txid()
}).

-type vectorclock() :: vectorclock:vectorclock().
-type txid() :: #tx_id{}.
-type clocksi_payload() :: #clocksi_payload{}.

-type client_op() :: {update, {key(), type(), op()}}
                   | {read, {key(), type()}}
                   | {prepare, term()}
                   | commit.

-type crdt() :: term().
-type val() :: term().
-type reason() :: atom().
%%chash:index_as_int() is the same as riak_core_apl:index().
%%If it is changed in the future this should be fixed also.
-type index_node() :: {chash:index_as_int(), node()}.
-type preflist() :: riak_core_apl:preflist().
-type log() :: term().
-type op_num() :: non_neg_integer().
-type op_id() :: {op_num(), node()}.
-type payload() :: term().
-type partition_id() :: ets:tid() | integer(). % TODO 19 adding integer basically makes the tid type non-opaque, because some places of the code depend on it being an integer. This dependency should be removed, if possible.
-type log_id() :: [partition_id()].
-type bucket() :: term().
-type snapshot() :: term().

-type tx() :: #transaction{}.
-type cache_id() :: ets:tab().
-type inter_dc_conn_err() :: {error, {partition_num_mismatch, non_neg_integer(), non_neg_integer()}
                           | {error, connection_error}}.

-type bound_object() :: {key(), type(), bucket()}.

-type module_name() :: atom().
-type function_name() :: atom().

-export_type([key/0, effect/0, crdt/0, val/0, reason/0, preflist/0,
              log/0, op_id/0, payload/0, partition_id/0,
              type/0, snapshot/0, txid/0, tx/0,
              bucket/0,
              txn_properties/0,
              op_param/0, op_name/0,
              bound_object/0,
              module_name/0,
              function_name/0,
              clocksi_payload/0]).


%% The record is using during materialization to keep the
%% state needed to materialize an object from the cache (or log)
-record(snapshot_get_response, {
    %% list of new ops
    ops_list :: [{op_num(),clocksi_payload()}] | tuple(),
    %% size of ops_list
    number_of_ops :: non_neg_integer(),
    %% the previous snapshot to apply the ops to
    materialized_snapshot :: #materialized_snapshot{},
    %% The version vector time of the snapshot
    snapshot_time :: snapshot_time() | ignore,
    %% true if this is the most recent snapshot in the cache
    is_newest_snapshot :: boolean()
}).
