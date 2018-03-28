---
title: Protocol Buffer API
last_updated: August 25, 2016
tags: [api]
sidebar: mydoc_sidebar
permalink: api.html
folder: mydoc
---

Clients can interact with the Antidote data store using a [protocol buffer](https://developers.google.com/protocol-buffers/) interface.
Here we describe the Erlang client for Antidote's protocol buffer interface. 
{% include note.html content="If you want to use this interface, you need to add [antidote_pb](https://github.com/SyncFree/antidote_pb) to your application's rebar dependencies." %}


## Transactions

A unit of operation in Antidote is a transaction. 
A client should first start a transaction, then read and/or update multiple objects, and finally commit the transaction.

All transaction functions take as first parameter the process identifier (pid) of the local Antidote proxy. 
Calling `antidotec_pb_socket:start(?ADDRESS, ?PORT)` starts this proxy and returns its pid.

### Start a transaction

  * `start_transaction(Pid::term(), Timestamp::term(), TxnProperties::term())
        -> {ok, TxnId::term()} | {error, Reason::term()}`

  This function starts a new transaction and returns a transaction identifier.
  This transaction identifier can be used to mark all further operations of this transaction. 
  The `Timestamp` provides the causality information, that is, the dependency information regarding other transactions. 
  Via `TxnProperties` you can pass a list of configuration parameters. 
  Currently, only one property is supported:  `static = true` starts a static transaction, 
  while `static = false` initiates an interactive transaction (default).

#### Example

```erlang
    %% If there is no dependency information available or required, 
    %% pass ignore as clock value.
    Clock = term_to_binary(ignore),
    %% Initiate a static transaction
    {ok, TxId} = antidotec_pb:start_transaction(Pid, Clock, [{static=true}]). 
```

### Reading and updating objects
  * `read_objects(Pid::term(), Objects::[term()], TxId::term()) -> {ok, [term()]}  | {error, term()}` reads a set of keys.
  
  * `update_objects(Pid::term(), Updates::[{term(), term(), term()}], TxId::term()) -> ok | {error, term()}` 
   takes a set of object with the operations and corresponding parameters as list of triples. 
   More on data types and operations can be found [here](#pb_datatypes).
  
####  Example

```erlang
    %% Information on key, type, and bucket
    KeyInfo = {Key, antidote_crdt_counter_pn, <<"bucket">>},
    %% Create a new counter update proxy locally
    Cntr = antidotec_counter:new(0),
    %% Increment the counter by 1
    Obj = antidotec_counter:increment(1, Cntr),
    ok = antidotec_pb:update_objects(Pid, antidotec_counter:to_ops(KeyInfor, Obj), TxId).

    Obj1 = {Key1, antidote_crdt_counter_pn, <<"bucket">>},
    Obj2 = {Key2, antidote_crdt_counter_pn, <<"bucket">>},
    %% Read values of two objects
    {ok, [Val1, Val2]} = antidotec_pb:read_objects(Pid, [Obj1, Obj2], TxId),
    Value = antidotec_counter:value(Val1). %% assuming Obj1 is of type counter
```

### Finalizing a transaction

  * `commit_transaction(Pid::term(), TxId::term()}) -> {ok, term()} | {error, term()}`
  
  To end a transaction, it has to be committed.
  All updates then performed against the stored data.
  These modifications are observable by later transactions that are (transitively) dependent on this transaction.

  * `abort_transaction(Pid::term(), TxId::term()) -> ok`

  Transactions can be stopped and canceled by calling `abort_transaction`.
  All updates for this transaction are then revoked.


#### Example
The following code snippet increments two counters atomically.

```erlang
    %% Starts pb socket
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),

    Counter1 = {Key1, antidote_crdt_counter_pn, Bucket},
    Counter2 = {Key2, antidote_crdt_counter_pn, Bucket},
    LocalObj = antidotec_counter:increment(Amount, antidotec_counter:new(0)),

    {ok, TxId} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    ok = antidotec_pb:update_objects(Pid, antidotec_counter:to_ops(Counter1, LocalObj),TxId),
    ok = antidotec_pb:update_objects(Pid, antidotec_counter:to_ops(Counter2, LocalObj),TxId),
    {ok, TimeStamp} = antidotec_pb:commit_transaction(Pid, TxId),
    
    %% Use TimeStamp for subsequent transactions if required
    {ok, TxId2} = antidotec_pb:start_transaction(Pid, TimeStamp, {}),
    ...
    ...

    %% Close pb socket
    _Disconnected = antidotec_pb_socket:stop(Pid),
```

## Data Types {#pb_datatypes}

Antidote supports several replicated data types (more information at [antidote_crdts](https://github.com/SyncFree/antidote_crdt)).
However, the protocol buffer interface currently supports only counters and sets.

### Counter

The client side representation of replicated counter `antidote_counter` provides the following interface:

* `new(integer()) -> antidotec_counter()` creates a local proxy (with an initial value). 
* `increment(integer(), antidotec_counter()) -> antidotec_counter()` increments the local proxy by the specified value.
* `decrement(integer(), antidotec_counter()) -> antidotec_counter()` decrements the local proxy by the specified value.
* `to_ops(term(), antidotec_counter()) -> [term()]` converts the local operations to right format for sending it to Antidote via `antidotec_pb:update_object/3`.
* `value(antidotec_counter()) -> integer()` returns an integer representing the current local value of the counter.

### Set

Similar to the counter, we have a client side representation of an replicated OR-set. The `antidotec_set` provides following interface:

* `new/1` creates a local proxy with some initial value.
* `add/2, remove/2` insert and remove elements from the set.
* `to_ops/2` converts the local operations to right format for sending it to Antidote via `antidotec_pb:update_object/3`.
* `value/1` returns a set representing the current local value of the replicated set, that is a list of elements which are in the set.
