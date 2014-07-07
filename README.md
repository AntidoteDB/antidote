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

### Current state ###
	NOTE: not all features are available in the master branch.'
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
2. Erlang R16B02 (read https://github.com/SyncFree/floppy/blob/master/tutorial/1-get-started.md#get-an-erlang).
	
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

    for d in dev/dev{2,3,4,5,6}; do $d/bin/floppy-admin cluster join 'floppy1@127.0.0.1'; done
    Success: staged join request for 'floppy2@127.0.0.1' to 'floppy1@127.0.0.1'
    Success: staged join request for 'floppy3@127.0.0.1' to 'floppy1@127.0.0.1'
    Success: staged join request for 'floppy4@127.0.0.1' to 'floppy1@127.0.0.1'

Sends the requests to node1, which we can now tell to build the cluster:

     dev/dev1/bin/floppy-admin cluster plan
     ...
     dev/dev1/bin/floppy-admin cluster commit

Have a look at the `member-status` to see that the cluster is balancing.

    dev/dev1/bin/floppy-admin member-status
    ================================= Membership ==================================
    Status     Ring    Pending    Node
    -------------------------------------------------------------------------------
    valid     100.0%     25.0%    'floppy1@127.0.0.1'
    valid       0.0%     25.0%    'floppy2@127.0.0.1'
    valid       0.0%     25.0%    'floppy3@127.0.0.1'
    valid       0.0%     25.0%    'floppy4@127.0.0.1'
    -------------------------------------------------------------------------------
    Valid:4 / Leaving:0 / Exiting:0 / Joining:0 / Down:0


Wait a while, and look again, and you should see a fully balanced
cluster.

    dev/dev1/bin/floppy-admin member-status
    ================================= Membership ==================================
    Status     Ring    Pending    Node
    -------------------------------------------------------------------------------
    valid      25.0%      --      'floppy1@127.0.0.1'
    valid      25.0%      --      'floppy2@127.0.0.1'
    valid      25.0%      --      'floppy3@127.0.0.1'
    valid      25.0%      --      'floppy4@127.0.0.1'
    -------------------------------------------------------------------------------
    Valid:4 / Leaving:0 / Exiting:0 / Joining:0 / Down:0


##### Remote calls

We don't have a client, or an API, but we can still call into the
cluster using distributed erlang.

Let's start a node:

    erl -name 'client@127.0.0.1' -setcookie floppy

First check that we can connect to the cluster:

     (cli@127.0.0.1)1> net_adm:ping('floppy3@127.0.0.1').
     pong

Then we can rpc onto any of the nodes and call `ping`:

    (cli@127.0.0.1)2> rpc:call('floppy1@127.0.0.1', floppy, ping, []).
    {pong,662242929415565384811044689824565743281594433536}
    (cli@127.0.0.1)3>

And you can shut down your cluster like

    for d in dev/dev*; do $d/bin/floppy stop; done
    
When you start it up again, it will still be a cluster.
make devrel, if you want different nodes (6 by default) to be created. See https://github.com/SyncFree/floppy/blob/rdb/3-coordinator/tutorial/1-get-started.md#make-a-cluster
on how to join all these nodes into a single cluster. Important: in every command there explained, replace "floppy" by "floppy" (when aplicable).
		

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


