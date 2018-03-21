---
title: Getting started with Antidote
keywords: setup, installation
tags: [getting_started]
sidebar: mydoc_sidebar
permalink: setup.html
---

# Quickstart with Docker

## Prerequisites

- Working recent version of Docker (1.12 and up recommended)

## Starting a local node

Start a local node with the command

```
docker run -d --name antidote -p "8087:8087" antidotedb/antidote
```

This should fetch the Antidote image automatically. For updating to the latest version use the command `docker pull antidotedb/antidote`.

Wait until Antidote is ready. The current log can be inspected with `docker logs antidote`.
Wait until the log message `Application antidote started on node 'antidote@127.0.0.1'` appears.

Antidote should now be running on port 8087 on localhost.


# Building Antidote releases

### Prerequisites ###

*  An UNIX-like OS
*  Erlang 18.3 or later

### Getting Antidote ###

The source code is available open-source at [Github](https://github.com/SyncFree/antidote).

You can clone it to your machine via

    git clone git@github.com:SyncFree/antidote.git

### Building a single node cluster ###

Antidote uses rebar3 for building releases.

    make rel

Rebar3 will now pull all the dependencies it needs from Github, and build
the application, and make an Erlang "release" of a single node.  If all
went well (if it didn't, contact us at [info@antidotedb.com](mailto:info@antidotedb.com)), then you should be able to start a node:

    15:55:05:antidote $ make console
    (elided)
    Eshell V5.10.3  (abort with ^G)
    (antidote@127.0.0.1)1>

Type `Ctrl-g` and `q` to quit the shell and stop the node.

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

    make console

#### Writing

Perform a write operation (example):

        CounterObj = {my_counter, antidote_crdt_counter_pn, my_bucket}.
        {ok, TxId} = antidote:start_transaction(ignore, []).
        ok = antidote:update_objects([{CounterObj, increment, 1}], TxId).
        {ok, _CommitTime} = antidote:commit_transaction(TxId).

You can also update objects with a single call as follows:

      CounterObj = {my_counter, antidote_crdt_counter_pn, my_bucket}.
      {ok, _CommitTime} = antidote:update_objects(ignore, [], [{CounterObj, increment, 1}]).

#### Reading

Perform a read operation (example):

      CounterObj = {my_counter, antidote_crdt_counter_pn, my_bucket}.
      {ok, TxId} = antidote:start_transaction(ignore, []).
      {ok, [CounterVal]} = antidote:read_objects([CounterObj], TxId).
      {ok, _CommitTime3} = antidote:commit_transaction(TxId).


Or in a single call:

    CounterObj = {my_counter, antidote_crdt_counter_pn, my_bucket}.
    {ok, Res, _CommitTime} = antidote:read_objects(ignore, [], [CounterObj]).
    [CounterVal] = Res.

More about the API is described [here](/antidote/rawapi.html).

Running Tests
-------------

### Run all tests ###

1. `make systests`

### Running specific tests ###

Tests are located in $ANTIDOTE/test/

1. `make systests SUITE=xxx_SUITE`


{% include links.html %}
