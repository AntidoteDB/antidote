FloppyStore
============
Welcome to the Floppystore repository, the reference platform of the SyncFree european project (https://syncfree.lip6.fr/).

About FloppyStore
-----------------
### Purpose ###
FloppyStore is an in-development distributed CRDT key-value store written in Erlang and riak_core based (https://github.com/basho/riak_core)
that is intended to provide the following features:
* Partitioning,
* Intra-DC replication,
* Inter-DC replication,
* Support for atomic write transactions,
* A flexible layered architecture so features can be smoothly added or removed. 
 
### Architecture ###
Information about Floppystore's layered design can be found in the following google doc: https://docs.google.com/document/d/1SNnmAtx5FrcNgEMdNQkKlfzYc1tqziaV2lQ6g9IQyzs/edit#heading=h.ze32da2pga2f

### Current state ###
	NOTE: not all features are available in the master branch.
* Partitioned (built on top of floppyStore),
* Replicated within a datacenter.
* State-based CRDT support, as it uses the riak_dt library.
* Provides Snapshot Isolation. it implements Clock-SI (available in the clock-SI_v2 branch), which is currently non-stable.
* Replication Across DCs with causal ordering (available in the causality branch).

### Future features ###
* Operation-based CRDT support.
* Support for "red-blue" transactions.
	
Using FloppyStore
-----------------
### Prerequisites ###
1. An unix-based OS.
2. Erlang R16B02 (read https://github.com/SyncFree/crdtdb/blob/master/tutorial/1-get-started.md#get-an-erlang).
	
	NOTE: use this Erlang version in order not to have problems.
	
### Getting floppystore ###
1. From your shell, run: `git clone http://github.com/SyncFree/floppystore`

### Building floppystore ###
#### Single Node Cluster 
Go to the floppystore directory (the one that you've just cloned using git) and:
	make rel
Rebar will now pull all the dependencies it needs from github, and
build the application, and make an erlang "release" of a single node.
If all went well (if it didn't, send an email to the SyncFree
tech mailing list), then you should be able to start
a node of `floppystore`.

    15:55:05:floppystore $ rel/floppy/bin/floppy console
    (elided)
    Eshell V5.10.3  (abort with ^G)
    (floppy@127.0.0.1)1> floppy:ping().
    {pong,1118962191081472546749696200048404186924073353216}
    (floppy@127.0.0.1)3>

What you should see is a `pong` response, followed by a big
number. The number is the partition that responded to the `ping`
request. Try it a few more times, different partitions will respond.

Again Ctrl-g` and `q` to quit the shell and stop the node.


#### Multi-Node Cluster

    make devrel

Will generate 6 nodes of `floppy` on your local machine, in
`./dev`. When that is done, we should start them all up.

    for d in dev/dev*; do $d/bin/floppy start; done

And check that they're working:

    for d in dev/dev*; do $d/bin/floppy ping; done
    pong
    pong
    pong
    pong


At this point you have 4 single node applications running. We need to
join them together in a cluster:

    for d in dev/dev{2,3,4,5,6}; do $d/bin/floppy-admin cluster join 'dev1@127.0.0.1'; done
    Success: staged join request for 'floppy2@127.0.0.1' to 'dev2@127.0.0.1'
    Success: staged join request for 'floppy3@127.0.0.1' to 'dev3@127.0.0.1'
    Success: staged join request for 'floppy4@127.0.0.1' to 'dev4@127.0.0.1'
    Success: staged join request for 'floppy4@127.0.0.1' to 'dev5@127.0.0.1'
    Success: staged join request for 'floppy4@127.0.0.1' to 'dev6@127.0.0.1'

Sends the requests to node1, which we can now tell to build the cluster:

     dev/dev1/bin/floppy-admin cluster plan
     ...
     dev/dev1/bin/floppy-admin cluster commit

Have a look at the `member-status` to see that the cluster is balancing.

    dev/dev1/bin/floppy-admin member-status
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
cluster.

    dev/dev1/bin/floppy-admin member-status
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

    erl -name 'client@127.0.0.1' -setcookie floppy

First check that we can connect to the cluster:

     (cli@127.0.0.1)1> net_adm:ping('dev3@127.0.0.1').
     pong

Then we can rpc onto any of the nodes and call `ping`:

    (cli@127.0.0.1)2> rpc:call('dev1@127.0.0.1', floppy, ping, []).
    {pong,662242929415565384811044689824565743281594433536}
    (cli@127.0.0.1)3>

And you can shut down your cluster like

    for d in dev/dev*; do $d/bin/floppy stop; done

When you start it up again, it will still be a cluster.
    
### Reading from and writing to a CRDT object stored in floppystore:

#### Writing

Start a node (if you haven't done it yet):

	erl -name 'client@127.0.0.1' -setcookie floppy

Perform a write operation (example):

    (client@127.0.0.1)1> rpc:call('dev1@127.0.0.1', floppy, append, [myKey, {increment, 4}]).
    {ok,{1,'dev1@127.0.0.1'}}

The above rpc calls the function append from the module floppy:

	append(Key, {OpParam, Actor})

where 

* `Key` = the key to write to.
* `OpParam` = the parameters of the update operation.
* `Actor` = the actor of the update (as needed by riak_dt, basho's state-based CRDT implementation)

In the particular call we have just used as an example, 

* `myKey` = the key to write to.
* `{increment,4}` = the parameters of the update:
	* `increment` = an operation type, as defined in the riak_dt definition of the data type that is being written (in this case a gcounter), and
	* `4` = the operation's actor id. 
	

	IMPORTANT: the update operation will execute no operation on the CRDT, will just store the operation in floppystore. The execution of operations to a key occur when the CRDT is read.


#### Reading

Start a node (if you haven't done it yet):

	erl -name 'client@127.0.0.1' -setcookie floppy

Perform a read operation (example):

	(client@127.0.0.1)1> rpc:call('dev1@127.0.0.1', floppy, read, [myKey, riak_dt_gcounter]).
    1
    
The above rpc calls the function read from the module floppy:

	read(Key, Type)

where 

* `Key` = the key to read from.
* `Type` = the type of CRDT.

In the particular call we have just used as an example, 

* `myKey` = the key to read from.
* `riak_dt_gcounter` = the CRDT type, a gcounter

The read operation will materialise (i.e., apply the operations that have been stored since the last materialisation, if any) the CRDT and return the result as an {ok, Result} tuple.

		

Application Structure
---------------------

This is a blank riak core application. To get started, you'll want to edit the
following files:

* `src/riak_floppy_vnode.erl`
  * Implementation of the riak_core_vnode behaviour
* `src/floppy.erl`
  * Public API for interacting with your vnode


Running Tests 
-------------

### Setup riak_test ###

1. Clone [https://github.com/SyncFree/riak\_test](https://github.com/SyncFree/riak_test) .Lets call it RIAK_TEST
2. cd RIAK_TEST and run commands
	* git checkout features/csm/floppystore	
	* make
5. Download and put `.riak_test.config` in home directory from following link
	https://gist.github.com/cmeiklejohn/d96d7167e1a8ed4b128d
6. Create directory `rt` in home
7. Modify  `.riak_test.config` 
	* Change rtdev\_path (line number 106) to point to your HOME/rt/floppystore
	* Add line `{test_paths, ["PATH TO FLOPPYSTORE/riak_test/ebin"]}`

### Building floppystore for testing ###

1. Go to floppystore directory
2. make stagedevrel
3. ./riak\_test/bin/floppystore-setup.sh (Only for the first time) 
    
	./riak\_test/bin/floppystore-current.sh

### Running test ###

1. Go to RIAK_TEST directory
2. ./riak\_test -v -c floppystore -v -t "TEST\_TO\_RUN"

	TEST\_TO\_RUN is any test module in floppystore/riak_test/

	eg:- ./riak_test -v -c floppystore -v -t floppystore


