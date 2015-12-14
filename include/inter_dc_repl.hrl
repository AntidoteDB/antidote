-record(recvr_state,
        {lastRecvd :: orddict:orddict(), %TODO: this may not be required
         lastCommitted :: orddict:orddict(),
         %%Track timestamps from other DC which have been committed by this DC
         recQ :: orddict:orddict(), %% Holds recieving updates from each DC separately in causal order.
         statestore,
         partition}).

-type socket_address() :: {inet:ip_address(), inet:port_number()}.
-type zmq_socket() :: any().
-type pdcid() :: {dcid(), partition_id()}.
-type log_opid() :: non_neg_integer().

-record(interdc_txn, {
 dcid :: dcid(),
 partition :: partition_id(),
 prev_log_opid :: log_opid() | none, %% the value is *none* if the transaction is read directly from the log
 snapshot :: snapshot_time(),
 timestamp :: non_neg_integer(),
 operations :: [operation()] %% if the OP list is empty, the message is a HEARTBEAT
}).

-record(descriptor, {
 dcid :: dcid(),
 partition_num :: non_neg_integer(),
 publishers :: [socket_address()],
 logreaders :: [socket_address()]
}).