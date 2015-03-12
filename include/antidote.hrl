-define(BUCKET, <<"antidote">>).
-define(MASTER, antidote_vnode_master).
-define(LOGGING_MASTER, logging_vnode_master).
-define(EC_MASTER, ec_vnode_master).
-define(ec_GENERATOR_MASTER,
        ec_downstream_generator_vnode_master).
-define(ec, ec).
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
%-type vectorclock() :: dict().


%% The way records are stored in the log.
-record(log_record, {tx_id :: tx_id(),
                     op_type:: update | prepare | commit | abort | noop,
                     op_payload}).



-record(tx_id, {time_id :: integer(), server_pid :: pid()}).
-record(ec_payload, {key :: key(),
                          type :: type(),
                          op_param :: {term(), term()},
                          tx_id :: tx_id()
                         }).

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
-type tx_id() :: #tx_id{}.
-type ec_payload() :: #ec_payload{}.
-type dcid() :: term().

-export_type([key/0, op/0, crdt/0, val/0, reason/0, preflist/0, log/0, op_id/0, payload/0, operation/0, partition_id/0, type/0, snapshot/0, tx_id/0]).
