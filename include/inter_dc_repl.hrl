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
-type interdc_descriptor() :: {dcid(), [socket_address()], [socket_address()]}.

-record(interdc_txn, {
 dcid :: dcid(),
 partition :: partition_id(),
 logid_range :: {non_neg_integer(), non_neg_integer()},
 snapshot :: snapshot_time(),
 timestamp :: non_neg_integer(),
 operations :: [operation()] %% if the OP list is empty, the message is a HEARTBEAT
}).