-record(recvr_state,
        {lastRecvd :: orddict:orddict(), %TODO: this may not be required
         lastCommitted :: orddict:orddict(),
         %%Track timestamps from other DC which have been committed by this DC
         recQ :: orddict:orddict(), %% Holds recieving updates from each DC separately in causal order.
         statestore,
         partition}).
