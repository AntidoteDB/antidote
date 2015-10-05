Antidote
============

Welcome to the Antidote repository, the reference platform of the [SyncFree European Project](https://syncfree.lip6.fr/)

About Antidote
-----------------
### Purpose ###
Antidote is an in-development distributed CRDT key-value store written in Erlang with [Riak Core](https://github.com/basho/riak_core) that is intended to provide the following features:

* Partitioning
* Intra-DC replication
* Inter-DC replication
* Support for atomic write transactions
* Flexible layered architecture so features can be smoothly added or removed

### Architecture ###

Information about Antidote's layered design can be found in the following [Google Doc](https://docs.google.com/document/d/1SNnmAtx5FrcNgEMdNQkKlfzYc1tqziaV2lQ6g9IQyzs/edit#heading=h.ze32da2pga2f)

### Current state ###

Not all features are available in the master branch.

* Partitioned
* Replicated within a datacenter
* State-based CRDT support, as it uses the [Riak DT library](https://github.com/basho/riak_dt)
* Provides snapshot isolation
* Replication across DCs with causal ordering

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

    15:55:05:antidote $ rel/antidote/bin/antidote console
    (elided)
    Eshell V5.10.3  (abort with ^G)
    (antidote@127.0.0.1)1>

Again `Ctrl-g` and `q` to quit the shell and stop the node.

#### Multi-Node Cluster

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

    for d in dev/dev*; do $d/bin/antidote stop; done

### Reading from and writing to a CRDT object stored in antidote:

#### Writing

Start a node (if you haven't done it yet):

    dev/dev1/bin/antidote console

Perform a write operation (example):

    (dev1@127.0.0.1)1> antidote:append(myKey, {increment, 4}).
    {ok,{1,'dev1@127.0.0.1'}}

The above rpc calls the function append from the module antidote:

    append(Key, {OpParam, Actor})

where

* `Key` = the key to write to
* `OpParam` = the parameters of the update operation
* `Actor` = the actor of the update

In the particular call we have just used as an example:

* `myKey` = the key to write to.
* `{increment,4}` = the parameters of the update

#### Reading

Start a node (if you haven't done it yet):

    dev/dev1/bin/antidote console

Perform a read operation (example):

    (dev1@127.0.0.1)1> antidote:read(myKey, riak_dt_gcounter).
    1

The above calls the function read from the module antidote:

    read(Key, Type)

where:

* `Key` = the key to read from.
* `Type` = the type of CRDT.

In the particular call we have just used as an example,

* `myKey` = the key to read from.
* `riak_dt_gcounter` = the CRDT type, a gcounter

The read operation will materialise (i.e., apply the operations that have been stored since the last materialisation, if any) the CRDT and return the result as an {ok, Result} tuple.

Running Tests
-------------

### Setup testing framework ###

1. Clone [Riak Test](https://github.com/basho/riak_test) from GitHub
2. `cd riak_test && make`

### Building antidote for testing ###

1. Go to antidote directory
2. `make stagedevrel`
3. `./riak_test/bin/antidote-setup.sh` (only for the first time)
4. `./riak_test/bin/antidote-current.sh`

### Run all tests ###

1. `make riak-test` (assumes `riak_test` is located `../riak\_test`)

### Running tests ###

1. `cd riak_test`
2. `./riak_test -v -c antidote -t $TEST_TO_RUN`
