-record(recvr_state,
        {lastRecvd :: orddict:orddict(), %TODO: this may not be required
         lastCommitted :: orddict:orddict(),
         %%Track timestamps from other DC which have been committed by this DC
         recQ :: orddict:orddict(), %% Holds recieving updates from each DC separately in causal order.
         statestore,
         partition}).

-type interdc_descriptor() :: {dcid : dcid(), publishers : socket_address_list(), logreaders : socket_address_list()}.
-type zmq_socket() :: any().
-type socket_address() :: {inet : ip_address(), inet : port_number()}.
-type socket_address_list() :: [socket_address()].
-type pdcid() :: {dcid(), partition_id()}.

-record(interdc_txn, {
 partition :: partition_id(),
 dcid :: dcid(),
 operations :: [operation()]
}).
