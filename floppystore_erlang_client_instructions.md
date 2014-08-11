# FloppyStore erlang client

This is a repository for an erlang FloppyStore client.  This simple
client provides a non-transactional put/get interface with the storage.

## Current progress
We are still discussing the API of the system, this simple version
allows to execute operations over a counter.

Each CRDT that the client supports must have a client-side container
that stores the payload of the CRDT and can extract the operations to be
appended to the log.

Most features of the system are not supported yet.

Please note that this version may not handle timeouts.

## Installing
Build the project as you would, following the instructions on:

```
https://github.com/SyncFree/floppystore/
```

## Testing the API

Start an erlang console with the required dependencies:

  erl -pa floppystore/deps/*/ebin/ floppystore/ebin/

Connect to the database

  {ok, Pid} = floppyc_pb_socket:start("localhost", 8087).

Read or create a new key with a counter data-type:

  Obj = floppyc_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid).

Increment and read the value of the counter:

  Obj2 = floppyc_counter:increment(Obj).
  floppyc_counter:dirty_value(Obj2).

Store the updated object:

  floppyc_pb_socket:store_crdt(Obj2, Pid).

