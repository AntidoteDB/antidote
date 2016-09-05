---
title: Getting started with Antidote
keywords: sample homepage
tags: [getting_started]
sidebar: mydoc_sidebar
permalink: index.html
summary: These brief overview will give you a first taste of Antidote.
---

Antidote
============

Welcome to the Antidote repository, the reference platform of the [SyncFree European Project](https://syncfree.lip6.fr/)

About Antidote
-----------------
### Purpose ###
Antidote is an in-development distributed CRDT key-value store written in Erlang with [Riak Core](https://github.com/basho/riak_core) that is intended to provide the following features:

* Partitioning
* Inter-DC replication
* Support for transactions
* Flexible layered architecture so features can be smoothly added or removed

### Architecture ###

Information about Antidote's layered design can be found in the following [Google Doc](https://docs.google.com/document/d/1SNnmAtx5FrcNgEMdNQkKlfzYc1tqziaV2lQ6g9IQyzs/edit#heading=h.ze32da2pga2f)

### Current state ###

Not all features are available in the master branch.

* Partitioned
* OP-based CRDT support
* Transactional Causal+ Consistency using CURE protocol
* Replication across DCs with causal ordering
* Partial Replication

Using Antidote
--------------

### Prerequisites ###

* An UNIX-like OS
* [Erlang R16B02](https://github.com/SyncFree/crdtdb/blob/master/tutorial/1-get-started.md#get-an-erlang)

### Getting Antidote ###

    git clone git@github.com:SyncFree/antidote.git

### Building Antidote ###

#### Single Node Cluster ###

    make rel

Rebar will now pull all the dependencies it needs from github, and build
the application, and make an erlang "release" of a single node.  If all
went well (if it didn't, send an email to the SyncFree tech mailing
list), then you should be able to start a node of `antidote`.

    15:55:05:antidote $ _build/default/rel/antidote/bin/antidote console
    (elided)
    Eshell V5.10.3  (abort with ^G)
    (antidote@127.0.0.1)1>

Again `Ctrl-g` and `q` to quit the shell and stop the node.

<!-- #### Multi-Node Cluster TODO:

To generate 6 nodes of `antidote` on your local machine, in
`./dev`:

    make devrel

When that is done, we should start them all up:

    for d in dev/dev*; do $d/bin/antidote start; done

And check that they're working:

    for d in dev/dev*; do $d/bin/antidote ping; done
    pong
    pong
    pong
    pong


At this point you have 6 single node clusters running. We need to
join them together in a cluster:

    for d in dev/dev{2,3,4,5,6}; do $d/bin/antidote-admin cluster join 'dev1@127.0.0.1'; done
    Success: staged join request for 'dev1@127.0.0.1' to 'dev2@127.0.0.1'
    Success: staged join request for 'dev1@127.0.0.1' to 'dev3@127.0.0.1'
    Success: staged join request for 'dev1@127.0.0.1' to 'dev4@127.0.0.1'
    Success: staged join request for 'dev1@127.0.0.1' to 'dev5@127.0.0.1'
    Success: staged join request for 'dev1@127.0.0.1' to 'dev6@127.0.0.1'

Sends the requests to node1, which we can now tell to build the cluster:

     dev/dev1/bin/antidote-admin cluster plan
     ...
     dev/dev1/bin/antidote-admin cluster commit

Have a look at the `member-status` to see that the cluster is balancing:

    dev/dev1/bin/antidote-admin member-status
    ================================= Membership ==================================
    Status     Ring    Pending    Node
    -------------------------------------------------------------------------------
    valid     100.0%     16.6%    'dev1@127.0.0.1'
    valid       0.0%     16.6%    'dev2@127.0.0.1'
    valid       0.0%     16.6%    'dev3@127.0.0.1'
    valid       0.0%     16.6%    'dev4@127.0.0.1'
    valid       0.0%     16.7%    'dev5@127.0.0.1'
    valid       0.0%     16.7%    'dev6@127.0.0.1'
    -------------------------------------------------------------------------------
    Valid:6 / Leaving:0 / Exiting:0 / Joining:0 / Down:0


Wait a while, and look again, and you should see a fully balanced
cluster:

    dev/dev1/bin/antidote-admin member-status
    ================================= Membership ==================================
    Status     Ring    Pending    Node
    -------------------------------------------------------------------------------
    valid      16.6%      --    'dev1@127.0.0.1'
    valid      16.6%      --    'dev2@127.0.0.1'
    valid      16.6%      --    'dev3@127.0.0.1'
    valid      16.6%      --    'dev4@127.0.0.1'
    valid      16.6%      --    'dev5@127.0.0.1'
    valid      16.6%      --    'dev6@127.0.0.1'
    -------------------------------------------------------------------------------
    Valid:6 / Leaving:0 / Exiting:0 / Joining:0 / Down:0

##### Remote calls

We don't have a client, or an API, but we can still call into the
cluster using distributed erlang.

Let's start a node:

    dev/dev1/bin/antidote console

First check that we can connect to the cluster:

    (dev1@127.0.0.1)1> net_adm:ping('dev3@127.0.0.1').
    pong

And you can shut down your cluster:

    for d in dev/dev*; do $d/bin/antidote stop; done -->

### Reading from and writing to a CRDT object stored in antidote:

Antidote can store and manage different [crdts](https://github.com/SyncFree/antidote_crdt).
Here we see how to update and read a counter.

Start a node (if you haven't done it yet):

    _build/default/rel/antidote/bin/antidote console

#### Writing

Perform a write operation (example):

        CounterObj = {my_counter, antidote_crdt_counter, my_bucket}.
        {ok, TxId} = antidote:start_transaction(ignore, []).
        ok = antidote:update_objects([{CounterObj, increment, 1}], TxId).
        {ok, _CommitTime} = antidote:commit_transaction(TxId1).

You can also update objects with a single call as follows:

      CounterObj = {my_counter, antidote_crdt_counter, my_bucket}.
      {ok, _CommitTime} = antidote:update_objects(ignore, [], [{CounterObj, increment, 1}]).

#### Reading

Perform a read operation (example):

      CounterObj = {my_counter, antidote_crdt_counter, my_bucket}.
      {ok, TxId} = antidote:start_transaction(ignore, []).
      {ok, [CounterVal]} = antidote:read_objects([CounterObj], TxId).
      {ok, _CommitTime3} = antidote:commit_transaction(TxId).


Or in a single call:

    CounterObj = {my_counter, antidote_crdt_counter, my_bucket}.
    {ok, Res, _CommitTime} = antidote:read_objects(ignore, [], [CounterObj]).
    [CounterVal] = Res.

More about the API is described [here](/rawapi.html).

Running Tests
-------------

### Run all tests ###

1. `make systests`

### Running specific tests ###

Tests are located in $ANTIDOTE/test/

1. `make systests SUITE=xxx_SUITE`


{% include links.html %}
