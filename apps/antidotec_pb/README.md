# Antidote Erlang Client Library

## Connecting to Antidote
This section assumes that you have at least one AntidoteDB node running. Check the documentation in the [main repository][antidote-repo] for instructions on how to launch AntidoteDB instances.

The `antidotec_pb_socket` has several connection functions, but the simplest is `antidotec_pb_socket:start_link/2`, which takes in an address and port number and returns a connection wrapped in an Erlang Pid:

```erl
{ok, Pid} = antidotec_pb_socket:start_link("127.0.0.1", 8087).
```

If you are running a local instance of AntidoteDB, the default protocol buffers port number is `8087`.

Once you have acquired a `Pid` connection to an AntidoteDB node, you are ready to run transactions.

## Management Interface

For coordinating Antidote instances, you can use the following operations via the Erlang client:

```erl
{ok, Pid} = antidotec_pb_socket:start_link("127.0.0.1", 8087).
%%
ok = antidotec_pb_management:create_dc(Pid, [Node1, Node2]).
{ok, Descriptor} = antidotec_pb_management:get_connection_descriptor(Pid).
ok = antidotec_pb_management:connect_to_dcs(Pid, [Descriptor]).
```

## Transactional Interface

AntidoteDB has a simple transactional interface:

- `start_transaction/2`
- `read_objects/3`
- `update_objects/3`
- `commit_transaction/2`
- `abort_transaction/2`

In the following, you find a short description of the API.
A more elaborate version with examples can be found in the [AntidoteDB Documentation](https://antidotedb.gitbook.io/documentation/api/protocol-buffer-api).

#### Starting a transaction

The function `start_transaction(Pid, Timestamp) -> {ok, TxId} | {error, Reason}` starts a new transaction and returns a transaction identifier, to be used with further operations in the same transaction. Antidote transactions can be of 2 types: _interactive_ or _static_.

**Static transactions** can be seen as one-shot transactions for executing a set of updates or a read operation. Static transactions do not have to be committed or closed and are mainly handled by the Antidote server.

**Interactive transactions** can combine multiple read and update operations into an atomic transaction. Updates issued in the context of an interactive transaction are visible to read operations issued in the same context after the updates. Interactive transactions have to be committed in order to make the updates visible to subsequent transactions.

The default transaction type is `interactive`. Currently you can create `static` transactions by using the alternative `start_transaction/3` callback, that accepts an additional list of transaction properties (you can pass in `[{static, true}]` to the third parameter in order to get static transactions).

Here is a simple example:

```erl
{ok, Pid}  = antidotec_pb_socket:start_link("127.0.0.1", 8087).
%% ignore leaves out the clock parameter in the following calls
{ok, TxId1} = antidotec_pb:start_transaction(Pid, ignore). %% interactive transaction
{ok, TxId2} = antidotec_pb:start_transaction(Pid, ignore, [{static, true}]). %% static transaction
```

#### Reading objects

This callback is used to read multiple objects from Antidote. The parameters are the Pid we already created before, a list of bound objects and finally the transactional identifier. Bound objects are
tuples that identify the key, its type and the bucket it is being read from.

Here is an example of a read-only static transaction:

```erl
{ok, Pid} = antidotec_pb_socket:start_link("127.0.0.1", 8087).
CounterBoundObj = {<<"my_antidote_counter">>, antidote_crdt_counter_pn, <<"my_bucket">>}.
RegisterBoundObj = {<<"my_antidote_register">>, antidote_crdt_register_lww, <<"my_bucket">>}.
%% start a static transaction
{ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{static, true}]).
%% read values from antidote
{ok, [Counter, Register]} = antidotec_pb:read_objects(Pid, [CounterBoundObj, RegisterBoundObj], TxId).
%% get the actual values out of the CRDTs
CounterVal = antidotec_counter:value(Counter).
RegisterVal = antidotec_reg:value(Register).
```

#### Updating objects

Updates a set of objects with the specified operations. The supplied `ListBoundOjectUpdates` value must
include operations allowed on the CRDT type for the bound object. To clarify this concept, we provide an
example below:

```erl
{ok, Pid} = antidotec_pb_socket:start_link("127.0.0.1", 8087).
CounterBoundObj = {<<"my_antidote_counter">>, antidote_crdt_counter_pn, <<"my_bucket">>}.
RegisterBoundObj = {<<"my_antidote_register">>, antidote_crdt_register_lww, <<"my_bucket">>}.
%% start a static transaction
{ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{static, true}]).
%% Perform local updates
%% Get a new counter object and increment its value by 5
UpdatedCounter = antidotec_counter:increment(5, antidotec_counter:new()).
%% Get a new register object and assign it to some value
UpdatedRegister = antidotec_reg:assign(antidotec_reg:new(), "Antidote rules!").
%% convert updated values into operations to be performed in the database
CounterUpdateOps = antidotec_counter:to_ops(CounterBoundObj, UpdatedCounter).
RegisterUpdateOps = antidotec_reg:to_ops(RegisterBoundObj, UpdatedRegister).
%% write values to antidote
antidotec_pb:update_objects(Pid, CounterUpdateOps, TxId).
antidotec_pb:update_objects(Pid, RegisterUpdateOps, TxId).
%% get the actual values out of the CRDTs
{ok, [Counter, Register]} = antidotec_pb:read_objects(Pid, [CounterBoundObj, RegisterBoundObj], TxId).
CounterVal = antidotec_counter:value(Counter).
RegisterVal = antidotec_reg:value(Register).
```

#### Aborting transactions

Aborts an ongoing transaction (currently not implemented!).
This operation can fail if transaction was already committed or by request time out.

#### Committing a transaction

Commits an ongoing transaction. When multiple transactions attempt to write to the same key at a data center concurrently, only one is guaranteed to succeed in the commit operation, if current commits are disable; this will result in error messages.

[antidote-repo]: https://github.com/AntidoteDB/antidote
