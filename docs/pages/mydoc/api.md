---
title: API
last_updated: August 25, 2016
tags: [api]
summary: ""
sidebar: mydoc_sidebar
permalink: api.html
folder: mydoc
---

Client applications can access Antidote using [protocol buffer](https://developers.google.com/protocol-buffers/) interface.

Here we describe the Erlang client for Antidote's protocol buffer interface. To use the client library, add [antidote_pb](https://github.com/SyncFree/antidote_pb) to your application's rebar dependencies.

Antidote API
-----------

A unit of operation in Antidote is a transaction. A client should first start a transaction, then read and/or update multiple objects, and then commit the transaction.

### Interface for transactions ###

* start\_transaction (Pid, timestamp, properties) --> transaction\_descriptor

  starts a new transaction and returns a transaction_descriptor which is a transaction identifier to be used with further operations of the transaction. timestamp provides the causality information. properties is a list of configurable parameters for the transaction. Currently only property supported which specifies to use static or interactive transactions.
Example:

        Clock = term_to_binary(ignore), %% First time there is no clock information
        {ok, TxId} = antidotec_pb:start_transaction(Pid, Clock, [{static=true}]). %% Use static transactions

* read\_objects (Pid, [bound\_object], transaction\_descriptor) --> {ok, Vals}

  reads a set of keys.
  Example:

        {ok, [Val1, Val2]} = antidotec_pb:read_objects(Pid, [O1, O2], TxId),
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


Example
-------

    %% Starts pb socket
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),

    Counter1 = {Key1, antidote_crdt_counter, Bucket},
    Counter2 = {Key2, antidote_crdt_counter, Bucket},
    Obj = antidotec_counter:increment(Amount, antidotec_counter:new()),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_counter:to_ops(Counter1, Obj),
                                     TxId),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_counter:to_ops(Counter2, Obj),
                                     TxId),
    {ok, TimeStamp} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Use TimeStamp for subsequent transactions if required
    {ok, TxId2} = antidotec_pb:start_transaction(Pid, TimeStamp, [{static=true}]),
    ...
    ...

    %% Close pb socket
    _Disconnected = antidotec_pb_socket:stop(Pid),

Data Types
----------
Antidote support many high level replicated data types. Protocol buffer interface currently supports counter and sets. There are more data types supported by Antidote. (See [antidote_crdts](https://github.com/deepthidevaki/antidote_crdt)).

### antidotec_counter ###

This is the client side representation of replicated counter.
It provides following interface:

* new
* increment
* decrement
* to_ops

  The local operations (increment/decrement) must be converted to right format using this method before sending it to Antidote via antidotec_pb:update_object/3.

* value

  Returns an integer representing the current value of the counter.

### antidotec_set ###

This is the client side representation of replicated OR-set. It provides following interface:

* new
* add
* remove
* to_ops
* value

  Returns a list of elements which are in the set.
