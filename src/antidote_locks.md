Antidote Locks
==============

This document describes the implementation of locks in Antidote.

Interface and Consistency Model
-------------------------------

When starting a transaction (or a single read- or update operation), users can specify a set of shared and exclusive locks for the transaction.
A lock is represented by an arbitrary Erlang term (when used via the protocol buffer interface, this is always a binary).

For example the following called would acquire lock1 exclusively and lock2 and lock3 as a shared lock:

    antidote:start_transaction(ignore, [
        {exclusive, [<<"lock1">>]}, {shared, [<<"lock2">>, <<"lock3">>]}])

Two transactions are conflicting if they both include a request for the same lock and at least one of the requests is for exclusive access.
For two conflicting transactions `tx1` and `tx2`, Antidote guarantees serializable consistency. 
This means that either `commit_timestamp(tx1) <= snapshot(tx2)` or vice versa (`commit_timestamp(tx2) <= snapshot(tx1)`). 


The call to `start_transaction` may fail if Antidote is unable to acquire the lock.


Implementation
--------------

The implementation works on two levels: Lock management within a DC and across DCs. 
Within each DC there is a single process running on one of the nodes, which is responsible for handling locks on the DC.
To handle locks across DCs, we store the current lock values in Antidotes replicated CRDT storage.


Each lock consists of N parts: one for each datacenter.
Initially, each DC holds its own part of the lock.
To execute a transaction with an exclusive lock, the DC must hold all N lock parts.
To execute a transaction with a shared lock, the DC must hold its own part of the lock.

The current state of each lock is stored in a map CRDT, storing the DC currently holding the lock for each lock part.
If a lock part DC_i has no entry in the CRDT, it is defined to be DC_i.

When locks are requested at a datacenter the following strategy is used for acquiring the locks:

1. Read the current state of all locks.
2. Check if we have all required locks.
    - If lock is missing, send a request to all other DCs and wait for responses
3. Once all locks are locally available: 
4. Wait until there are no other known requests with conflicting locks that should be served first (or already hold a lock locally)
5. Acquire the locks, reply with the current snapshot time.


When receiving a lock request from a different DC:

- For each requested lock:
    1. Wait until there are no other known requests with conflicting locks that should be served first (or already hold a lock locally)
    2. Update the CRDT state of the locks to transfer
    3. Send an inter-dc message to the requesting DC

### Ordering:

To guarantee liveness and fairness, we use the following ordering on requests:

- Each lock request receives a timestamp that is used for ordering.
    - If all locks are available locally, this is the current system time.
    - Otherwise it is the current system time plus the value of `INTER_DC_LOCK_REQUEST_DELAY`.
        This is the expected time we have to wait to get locks from other DCs, so we add this time such that local requests can still use the locks for some time.
- Lock requests are ordered lexicographically by the pair `{RequestTime, RequestingDc}` (i.e. the DC identifier is used to arbitrate in case of equal request times)

### Performance

Since it is quite expensive to move from one DC to another (the other DC has to catch up to the same snapshot time), we want to avoid sending a lock to a different DC if it is still required locally.
However, it should eventually be sent to the other DC to guarantee liveness/fairness.

To achieve this goal, we use the following strategy:

Always send the lock to the requesting DC if it has been held locally for more than `?MAX_LOCK_HOLD_DURATION`.
Otherwise, only send the lock if it has not been used in the last `?MIN_EXCLUSIVE_LOCK_DURATION` (since it is likely to be used again).
 



### Code Structure

- clocksi_interactive_coord:
    - Locks are acquired in `start_tx_internal`.
    - Locks are released in `before_commit_checks`.
- antidote_locks: 
    - Provides the internal Lock API
- antidote_lock_server:
    - A gen_server with one instance per DC (the server is registered using the global name `antidote_lock_server`).
    - Responsible for 
- antidote_lock_server_state:
    - Encapsulates the state and logic of the `antidote_lock_server`.
    - This module is purely functional, so that it is easier to test.
- antidote_lock_server_sup:
    - Supervisor for the antidote_lock_server
    



Future Work
-----------

- Dynamic Membership: Adding and removing DCs is currently not supported.
- Fault tolerance: In the following scenarios the current implementation would not work correctly:
    - CRDT state cannot be updated locally: Implementation will wait for the update indefinitely.
    - A datacenter crashes: Locks currently held by the crashing datacenter are never released, so no other DC can acquire the locks even when the DC is down for a long time.
        This could be partially solved by supporting dynamic membership and removing a DC that has been down for a long time. 
        However, this cannot be done automatically, since we cannot distinguish a network partition from a crashed DC.
        
        An alternative approach would be to use quorums such that some DC failures can be tolerated.  
- Performance: 
    - The main performance bottleneck at the moment is the time it takes to wait for a particular snapshot time.
        This can be improved by changing the following constants in antidote.hrl:    
        - `VECTORCLOCK_UPDATE_PERIOD`: How often vectorclocks are exchanged between shards in the same DC.
        - `META_DATA_SLEEP`: Sleep between sending meta data to other DCs
        
        A better option might be to send updates to other DCs as soon as they are available.
        When waiting for a clock, we could actively ask other shards instead of waiting for them to tell us after the timeout.
            
    - The `antidote_lock_server_state` module is not implemented efficiently in terms of CPU usage. The computation could be done incrementally to save cycles.
