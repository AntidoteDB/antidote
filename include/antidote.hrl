-define(BUCKET, <<"antidote">>).
-define(MASTER, antidote_vnode_master).
-define(LOGGING_MASTER, logging_vnode_master).
-define(CLOCKSI_MASTER, clocksi_vnode_master).
-define(CLOCKSI_GENERATOR_MASTER,
        clocksi_downstream_generator_vnode_master).
-define(CLOCKSI, clocksi).
-define(REPMASTER, antidote_rep_vnode_master).
-define(N, 1).
-define(OP_TIMEOUT, infinity).
-define(COORD_TIMEOUT, infinity).
-define(COMM_TIMEOUT, infinity).
-define(NUM_W, 2).
-define(NUM_R, 2).
-record (payload, {key:: key(), type :: type(), op_param, actor}).

%% Used by the replication layer
-record(operation, {op_number, payload :: payload()}).
-type operation() :: #operation{}.
-type vectorclock() :: dict().


%% The way records are stored in the log.
-record(log_record, {tx_id :: txid(),
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

-record(tx_id, {snapshot_time, server_pid :: pid()}).
-record(clocksi_payload, {key :: key(),
                          type :: type(),
                          op_param :: {term(), term()},
                          snapshot_time :: snapshot_time(),
                          commit_time :: commit_time(),
                          txid :: txid()
                         }).
-record(transaction, {snapshot_time :: snapshot_time(),
                      server_pid :: pid(), 
                      vec_snapshot_time, 
                      txn_id :: txid() }).

%%---------------------------------------------------------------------
-type key() :: term().
-type op()  :: {term(), term()}.
-type crdt() :: term().
-type val() :: term().
-type reason() :: atom().
%%chash:index_as_int() is the same as riak_core_apl:index().
%%If it is changed in the future this should be fixed also.
-type index_node() :: {chash:index_as_int(), node()}.
-type preflist() :: riak_core_apl:preflist().
-type log() :: term().
-type op_id() :: {non_neg_integer(), node()}.
-type payload() :: term().
-type partition_id()  :: non_neg_integer().
-type log_id() :: [partition_id()].
-type type() :: atom().
-type snapshot() :: term().
-type snapshot_time() ::  vectorclock:vectorclock().
-type commit_time() ::  {term(), non_neg_integer()}.
-type txid() :: #tx_id{}.
-type clocksi_payload() :: #clocksi_payload{}.
-type dcid() :: term().
-type tx() :: #transaction{}.
-type dc_address():: {inet:ip_address(),inet:port_number()}.

-export_type([key/0, op/0, crdt/0, val/0, reason/0, preflist/0, log/0, op_id/0, payload/0, operation/0, partition_id/0, type/0, snapshot/0, txid/0, tx/0,
             dc_address/0]).
