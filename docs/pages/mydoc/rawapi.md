---
title: Native Erlang API
last_updated: August 29, 2016
tags: [api]
sidebar: mydoc_sidebar
permalink: rawapi.html
folder: mydoc
---

This page describes the native Erlang API of Antidote. Clients can invoke these functions via RPC. 
A more convenient but restricted way for client applications to interact with Antidote is the [protocol buffer interface](/api.html).

## CRDTs

Antidote provides a library of CRDTs. 
The interface of these CRDTs specify the operations and the parameters that can be used for inspection and modification of shared objects. 
In the following, we specify the supported `{operation(), op_param()}` pair for each of the supported CRDTs. 
The first element in the tuple specifies the update operation, and the second item indicates the corresponding parameters.

##### antidote_crdt_counter_pn #####
    {increment, integer()}
    {decrement, integer()}

##### antidote_crdt_set_aw #####
    {add, term()}
    {remove, term()}
    {add_all, [term()]}
    {remove_all, [term()]}

##### antidote_crdt_gset #####
    {add, {term(), actor()}}
    {remove, {term(), actor()}}
    {add_all, {[term()], actor()}}
    {remove_all, {[term()], actor()}}

##### antidote_crdt_register_lww #####
    {assign, {term(), non_neg_integer()}}
    {assign, term()}.

##### antidote_crdt_map_rr #####
    {update, {[map_field_update() | map_field_op()], actorordot()}}.

    -type actorordot() :: riak_dt:actor() | riak_dt:dot().
    -type map_field_op() ::  {remove, field()}.
    -type map_field_update() :: {update, field(), crdt_op()}.
    -type crdt_op() :: term(). %% Valid riak_dt updates
    -type field() :: term()

##### antidote_crdt_register_mv #####
    {assign, {term(), non_neg_integer()}}
    {assign, term()}
    {propagate, {term(), non_neg_integer()}}

##### antidote_crdt_rga #####
    {addRight, {any(), non_neg_integer()}}
    {remove, non_neg_integer()}

## Transactions

A unit of operation in Antidote is a transaction. 
A client should first start a transaction, then read and/or update several objects, and finally commit the transaction.

There are two types of transactions: interactive transactions and static transactions.

### Interactive transactions ###

With an interactive transaction, a client can execute several updates and reads before committing the transactions. 
The interface of interactive transaction is:

```erlang
    type bound_object() = {key(), crdt_type(), bucket()}.
    type snapshot_time() = vectorclock() | ignore.
    type reason() = term().

    start_transation(snapshot_time(), properties()) -> {ok, txid()} | {error, reason()}.

    update_objects([{bound_object(), operation(), op_param()}], txid()) -> ok | {error, reason()}.

    read_objects([bound_object()], TxId) -> {ok, [term()]}.

    commit_transaction(txid()) -> {ok, vectorclock()} | {error, reason()}.
```

#### Example ####

```erlang
     CounterObj = {my_counter, antidote_crdt_counter_pn, my_bucket},

     %% Read and increment counter by 1
     {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
     {ok, [CounterVal]} = rpc:call(Node, antidote, read_objects, [[CounterObj], TxId]),
     ok = rpc:call(Node, antidote, update_objects, [[{CounterObj, increment, 1}], TxId]),
     {ok, CT} = rpc:call(Node, antidote, commit_transaction, [TxId]),
     
     %% Start a new transaction
     {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [CT, []]),
```

### Static transactions ###
Static transactions consist of a single bulk operation. There are two different types:
* A client can issue a single call to update multiple objects atomically. 

```erlang
     type bound_object() = {key(), crdt_type(), bucket()}.
     type snapshot_time() = vectorclock() | ignore.

     update_objects(snapshot_time(), properties(), [{bound_object(), operation(), op_param()}]) ->
                {ok, vectorclock()} | {error, reason()}.

```
* A client can issue a single call to read to multiple objects from the same consistent snapshot.  

```erlang
    read_objects(snapshot_time(), properties(), [bound_object()]) -> {ok, [term()], vectorclock()}.
```

It is not possible to read and update in the same transaction.

#### Example ####

```erlang
    CounterObj = {my_counter, antidote_crdt_counter_pn, my_bucket},
    SetObj = {my_set, antidote_crdt_set_aw, my_bucket},
    {ok, CT1} = rpc:call(Node, antidote, update_objects, [ignore, [], [{CounterObj, increment, 1}]]),
    {ok, Result, CT2} = rpc:call(Node, antidote, read_objects, [CT1, [], [CounterObj, SetObj]]),
    [CounterVal, SetVal] = Result.
```
