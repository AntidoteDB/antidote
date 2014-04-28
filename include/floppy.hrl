-define(PRINT(Var),
        io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(BUCKET, <<"floppy">>).
-define(MASTER, floppy_vnode_master).
-define(LOGGINGMASTER, logging_vnode_master).
-define(CLOCKSIMASTER, clocksi_vnode_master).
-define(REPMASTER, floppy_rep_vnode_master).
-define(INDC_TIMEOUT, 1000).
-define(N, 3).
-define(NUM_W, 2).
-define(NUM_R, 2).
-define(OTHER_DC, 'floppy1@127.0.0.1').
-record(operation, {op_number, payload}).


% Clock SI

% MIN is Used for generating the timeStamp of a new snapshot
% in the case that a client has already seen a snapshot time
% greater than the current time at the replica it is starting
% a new transaction.

% DELTA has the same meaning as in the clock-SI paper.

-define(MIN, 1).
-define(DELTA, 10000). 
-record(tx, {snapshot_time, commit_time, prepare_time, state, write_set, operations}).
