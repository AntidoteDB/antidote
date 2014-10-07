-define(BUCKET, <<"floppy">>).
-define(MASTER, floppy_vnode_master).
-define(LOGGING_MASTER, logging_vnode_master).
-define(CLOCKSI_MASTER, clocksi_vnode_master).
-define(CLOCKSI_GENERATOR_MASTER,
        clocksi_downstream_generator_vnode_master).
-define(CLOCKSI, clocksi).
-define(REPMASTER, floppy_rep_vnode_master).
-define(N, 1).
-define(OP_TIMEOUT, infinity).
-define(COORD_TIMEOUT, infinity).
-define(COMM_TIMEOUT, infinity).
-define(NUM_W, 2).
-define(NUM_R, 2).
-record (payload, {key, type, op_param, actor}).

%% Used by the replication layer
-record(operation, {op_number, payload}).
-type operation() :: #operation{}.

%% The way records are stored in the log.
-record(log_record, {tx_id,
                     op_type:: update | prepare | commit | abort | noop,
                     op_payload}).

%% Clock SI

%% MIN is Used for generating the timeStamp of a new snapshot
%% in the case that a client has already seen a snapshot time
%% greater than the current time at the replica it is starting
%% a new transaction.
-define(MIN, 1).

%% DELTA has the same meaning as in the clock-SI paper.
-define(DELTA, 10000).

-define(CLOCKSI_TIMEOUT, 1000).

%% Data Type: tx
%% where:
%%    snapshot_time:
%%        clock time of the tx's originating partition as returned by now()
%%    commit_time: final commit time of the tx.
%%    prepare_time: intermediate prepare-commit tx time.
%%    state:
%%        the state of the transaction, {active|prepare|committing|committed}.
%%     write_set: the tx's write set.
%%     origin: the transaction's originating partition
%%----------------------------------------------------------------------

%% -record(tx, {id, snapshot_time, commit_time, prepare_time, state, origin}).
-record(tx_id, {snapshot_time, server_pid}).
-record(clocksi_payload, {key :: term(),
                          type :: term(),
                          op_param :: {term(), term()},
                          snapshot_time :: vectorclock:vectorclock(),
                          commit_time :: {term(), non_neg_integer()},
                          txid :: #tx_id{}
                         }).
-record(transaction, {snapshot_time, server_pid, vec_snapshot_time, txn_id}).

%%---------------------------------------------------------------------
-type key() :: term().
-type op()  :: {term(), term()}.
-type crdt() :: term().
-type val() :: term().
-type reason() :: atom().
-type preflist() :: riak_core_apl:preflist().
-type log() :: term().
-type op_id() :: {non_neg_integer(), node()}.
-type payload() :: term().
-type partition_id()  :: non_neg_integer().
-type log_id() :: [partition_id()].
-type type() :: atom().
-type snapshot() :: term().
-type txid() :: #tx_id{}.
-type dcid() :: term().

-export_type([key/0, op/0, crdt/0, val/0, reason/0, preflist/0, log/0, op_id/0, payload/0, operation/0, partition_id/0, type/0, snapshot/0, txid/0]).
