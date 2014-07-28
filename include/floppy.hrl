-define(PRINT(Var),
        io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(BUCKET, <<"floppy">>).
-define(MASTER, floppy_vnode_master).
-define(LOGGINGMASTER, logging_vnode_master).
%-define(CLOCKSIMASTER, clocksi_vnode_master).
-define(REPMASTER, floppy_rep_vnode_master).
-define(OP_TIMEOUT, 5000).
-define(COORD_TIMEOUT, 2500).
-define(COMM_TIMEOUT, 500).
-define(N, 3).
-define(NUM_W, 2).
-define(NUM_R, 2).
-define(OTHER_DC, 'floppy1@127.0.0.1').
-record(operation, {op_number, payload}).
-type operation() :: #operation{}.


% Clock SI

% MIN is used for generating the timestamp of a new snapshot
% in the case that a client has already seen a snapshot time
% greater than the current time at the replica it is starting
% a new transaction.

% DELTA has the same meaning as in the clock-SI paper.

-define(MIN, 1).
-define(DELTA, 10000). 
-define(CLOCKSI_TIMEOUT, 1000).

-record(tx, {snapshot_time, commit_time, prepare_time, state, write_set, operations}).
-type tx() :: #tx{}.

-type key() :: term().
-type op()  :: term().
-type crdt() :: term().
-type val() :: term().
-type reason() :: atom().
-type preflist() :: riak_core_apl:preflist().
-type log() :: term().
-type op_id() :: {Number::non_neg_integer(), node()}.
-type payload() :: term().

-export_type([key/0, op/0, crdt/0, val/0, reason/0, preflist/0, log/0, op_id/0, payload/0, operation/0, tx/0]).
