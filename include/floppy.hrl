-define(PRINT(Var),
        io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(BUCKET, <<"floppy">>).
-define(MASTER, floppy_vnode_master).
-define(LOGGINGMASTER, logging_vnode_master).
-define(CLOCKSIMASTER, clocksi_vnode_master).
-define(CLOCKSI, clocksi).
-define(REPMASTER, floppy_rep_vnode_master).
-define(OP_TIMEOUT, 5000).
-define(COORD_TIMEOUT, 2500).
-define(COMM_TIMEOUT, 500).
-define(N, 3).
-define(NUM_W, 2).
-define(NUM_R, 2).
-define(OTHER_DC, 'floppy1@127.0.0.1').
-define(MAXRING,1461501637330902918203684832716283019655932542975).
-record (payload, {key, op_param, actor}).

%% Used by the replication layer
-record(operation, {op_number, payload}).

%% The way records are stored in the log.
-record(log_record, {tx_id, op_type::atom(), op_payload}).

%% Clock SI

%% MIN is Used for generating the timeStamp of a new snapshot
%% in the case that a client has already seen a snapshot time
%% greater than the current time at the replica it is starting
%% a new transaction.
-define(MIN, 1).

%% DELTA has the same meaning as in the clock-SI paper.
-define(DELTA, 10000).

-define(CLOCKSI_TIMEOUT, 1000).

%%---------------------------------------------------------------------
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
