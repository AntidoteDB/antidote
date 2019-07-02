# Antidote Erlang Client Library
[![Build Status](https://travis-ci.org/AntidoteDB/antidote-erlang-client.svg?branch=master)](https://travis-ci.org/AntidoteDB/antidote-erlang-client)
[![Coverage Status](https://coveralls.io/repos/github/AntidoteDB/antidote-erlang-client/badge.svg?branch=master)](https://coveralls.io/github/AntidoteDB/antidote-erlang-client?branch=master)

## Connecting to Antidote
This section assumes that you have at least one AntidoteDB node running. Check the documentation in the [main repository][antidote-repo] for instructions on how to launch AntidoteDB instances.

The `antidotec_pb_socket` has several connection functions, but the simplest is `antidotec_pb_socket:start_link/2`, which takes in an address and port number and returns a connection wrapped in an Erlang Pid:

```erl-sh
Pid = antidotec_pb_socket:start_link("127.0.0.1", 8087).
```

If you are running a local instance of AntidoteDB, the default protocol buffers port number is `8087`.

Once you have acquired a `Pid` connection to an AntidoteDB node, you are ready to run transactions.

## Transactional Interface

AntidoteDB has a simple transactional interface:

- `start_transaction/2`
- `read_objects/3`
- `update_objects/3`
- `commit_transaction/2`
- `abort_transaction/2`

Here is a detailed interface explanation:

#### start_transaction(Pid, Timestamp) -> {ok, TxId} | {error, Reason}.

This function starts a new transaction and returns a transaction identifier, to be used with further operations in the same transaction. Antidote transactions can be of 2 types: _interactive_ or _static_.

**Static transactions** can be seen as one-shot transactions for executing a set of updates or a read operation. Static transactions do not have to be committed or closed and are mainly handled by the Antidote server.

**Interactive transactions** can combine multiple read and update operations into an atomic transaction. Updates issued in the context of an interactive transaction are visible to read operations issued in the same context after the updates. Interactive transactions have to be committed in order to make the updates visible to subsequent transactions.

The default transaction type is `interactive`. Currently you can create `static` transactions by using the alternative `start_transaction/3` callback, that accepts an additional list of transaction properties (you can pass in `[{static, true}]` to the third parameter in order to get static transactions).

Here is a simple example:

```erl-sh
Pid = antidotec_pb_socket:start_link("127.0.0.1", 8087).
%% ignore leaves out the clock parameter in the following calls
{ok, TxId1} = antidotec_pb:start_transaction(Pid, ignore). %% interactive transaction
{ok, TxId2} = antidotec_pb:start_transaction(Pid, ignore, [{static, true}]). %% static transaction
```

#### read_objects(Pid, ListBoundOjects, TxId) -> {ok, Vals} | {error, Reason}.

This callback is used to read multiple objects from Antidote. The parameters are the Pid we already created before, a list of bound objects and finally the transactional identifier. Bound objects are
tuples that identify the key, its type and the bucket it is being read from.

Here is an example of a read-only static transaction:

```erl-sh
Pid = antidotec_pb_socket:start_link("127.0.0.1", 8087).
CounterBoundObj = {<<"my_antidote_counter">>, antidote_crdt_counter_pn, <<"my_bucket">>}.
RegisterBoundObj = {<<"my_antidote_register">>, antidote_crdt_register_mv, <<"my_bucket">>}.
%% start a static transaction
{ok, TxId} = start_transaction(Pid, Clock, [{static, true}]).
%% read values from antidote
{ok, [Counter, Register]} = antidotec_pb:read_objects(Pid, [CounterBoundObj, RegisterBoundObj], TxId).
%% get the actual values out of the CRDTs
CounterVal = antidotec_counter:value(Counter).
RegisterVal = antidotec_reg:value(Register).
```

#### update_objects(Pid, ListBoundOjectUpdates, TxId) -> ok | {error, Reason}.

Updates a set of objects with the specified operations. The supplied `ListBoundOjectUpdates` value must
include operations allowed on the CRDT type for the bound object. To clarify this concept, we provide an
example below:

```erl-sh
Pid = antidotec_pb_socket:start_link("127.0.0.1", 8087).
CounterBoundObj = {<<"my_antidote_counter">>, antidote_crdt_counter_pn, <<"my_bucket">>}.
RegisterBoundObj = {<<"my_antidote_register">>, antidote_crdt_register_mv, <<"my_bucket">>}.
%% start a static transaction
{ok, TxId} = start_transaction(Pid, Clock, [{static, true}]).
%% Perform local updates
%% Get a new counter object and increment its value by 5
UpdatedCounter = antidotec_counter:increment(5, antidote_crdt_counter:new())
%% Get a new register object and assign it to some value
UpdatedRegister = antidotec_reg:assign(antidote_crdt_reg:new(), "Antidote rules!")
%% convert updated values into operations to be performed in the database
CounterUpdateOps = antidotec_counter:to_ops(CounterBoundObj, UpdatedCounter),
RegisterUpdateOps = antidotec_reg:to_ops(RegisterBoundObj, UpdatedRegister),
%% write values to antidote
antidotec_pb:read_objects(Pid, [CounterUpdateOps, RegisterUpdateOps], TxId).
%% get the actual values out of the CRDTs
CounterVal = antidotec_counter:value(Counter).
RegisterVal = antidotec_reg:value(Register).
```

#### abort_transaction(Pid, TxId) -> ok | {error, Reason}.

Aborts an ongoing transaction. Can fail if transaction was already committed or by request time out.

#### commit_transaction(Pid, TxId)  -> ok | {error, Reason}.

Commits an ongoing transaction. When multiple transactions attempt to write to the same key only one is guaranteed to succeed in the commit operation; the others will return error messages.

[antidote-repo]: https://github.com/AntidoteDB/antidote
