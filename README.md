# Erlang client for Antiodote
[![Build Status](https://travis-ci.org/SyncFree/antidote_pb.svg?branch=master)](https://travis-ci.org/SyncFree/antidote_pb)

## Interface for transactions
     
* start\_transaction (Pid, timestamp, properties) --> transaction\_descriptor
  
  starts a new transaction and returns a transaction_descriptor which is a transaction identifier to be used with further operations of the transaction. timestamp provides the causality information. properties is a list of configurable parameters for the transaction. Currently only property supported which specifies to use static or interactive transactions.
Example:

        Clock = term_to_binary(ignore), %% First time there is no clock information
        {ok, TxId} = start_transaction(Pid, Clock, [{static=true}]). %% Use static transactions

* read\_objects (Pid, [bound\_object], transaction\_descriptor) --> {ok, Vals}

  reads a set of keys.
  Example:

        {ok, [Val1, Val2]} = ([O1, O2], TxId),
        Value = antidotec_counter:value(Val1). %% Assuming O1 is of type counter

* update\_objects (Pid, [{bound\_object, operation, op_parameters}], transaction\_descriptor) -> ok

  update a set of object with the specified operations. operation is any allowed (upstream) operation on the crdt type for bound\_object, op\_parameters are parameters to the operation. 
  Example: 

           Obj = antidotec_counter:increment(1, antidotec_counter:new()),
           ok = antidotec_pb:update_objects(Pid,
                                        antidotec_counter:to_ops(BObj, Obj),
                                        TxId).

* abort\_transaction (Pid, transaction_descriptor)
  aborts a transaction

* commit\_transaction (Pid, transaction_descriptor) --> {ok,timestamp} OR aborted


## Example
    
    %% Starts pb socket
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),

    BObj = {Key, riak_dt_pncounter, Bucket},
    Obj = antidotec_counter:increment(Amount, antidotec_counter:new()),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_counter:to_ops(BObj, Obj),
                                     TxId),
    {ok, TimeStamp} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Use TimeStamp for subsequent transactions if required
    {ok, TxId2} = antidotec_pb:start_transaction(Pid, TimeStamp, [{static=true}]),
    ...
    ...

    %% Close pb socket
    _Disconnected = antidotec_pb_socket:stop(Pid),
