Antidote
============
Welcome to the Antidote repository, the reference platform of the SyncFree european project (https://syncfree.lip6.fr/).

About Antidote
-----------------
### Purpose ###
Antidote is an in-development distributed CRDT key-value store written in Erlang and riak_core based (https://github.com/basho/riak_core)
that is intended to provide the following features:
* Partitioning,
* Intra-DC replication,
* Inter-DC replication,
* Support for atomic write transactions,
* A flexible layered architecture so features can be smoothly added or removed. 
 
### Architecture ###
Information about Antidote's layered design can be found in the following google doc: https://docs.google.com/document/d/1SNnmAtx5FrcNgEMdNQkKlfzYc1tqziaV2lQ6g9IQyzs/edit#heading=h.ze32da2pga2f

### Current state ###
	NOTE: not all features are available in the master branch.
* Partitioned (built on top of Antidote),
* Replicated within a datacenter.
* State-based CRDT support, as it uses the riak_dt library.
* Provides Snapshot Isolation. it implements Clock-SI (available in the clock-SI_v2 branch), which is currently non-stable.
* Replication Across DCs with causal ordering (available in the causality branch).

### Future features ###
* Operation-based CRDT support.
* Support for "red-blue" transactions.
	
Using Antidote
-----------------
### Prerequisites ###
1. An unix-based OS.
2. Erlang R16B02 (read https://github.com/SyncFree/crdtdb/blob/master/tutorial/1-get-started.md#get-an-erlang).
	
	NOTE: use this Erlang version in order not to have problems.
	
### Getting Antidote ###
1. From your shell, run: `git clone http://github.com/SyncFree/antidote`

### Building Antidote ###
#### Single Node Cluster 
Go to the Antidote directory (the one that you've just cloned using git) and:

	make rel
	
Rebar will now pull all the dependencies it needs from github, and
build the application, and make an erlang "release" of a single node.
If all went well (if it didn't, send an email to the SyncFree
tech mailing list), then you should be able to start
a node of `Antidote`.

    15:55:05:antidote $ rel/antidote/bin/antidote console
    (elided)
    Eshell V5.10.3  (abort with ^G)
    (antidote@127.0.0.1)1> antidote:ping().
    {pong,1118962191081472546749696200048404186924073353216}
    (antidote@127.0.0.1)3>

What you should see is a `pong` response, followed by a big
number. The number is the partition that responded to the `ping`
request. Try it a few more times, different partitions will respond.

Again Ctrl-g` and `q` to quit the shell and stop the node.


#### Multi-Node Cluster

    make devrel

Will generate 6 nodes of `antidote` on your local machine, in
`./dev`. When that is done, we should start them all up.

    for d in dev/dev*; do $d/bin/antidote start; done

And check that they're working:

    for d in dev/dev*; do $d/bin/antidote ping; done
    pong
    pong
    pong
    pong


At this point you have 4 single node applications running. We need to
join them together in a cluster:

    for d in dev/dev{2,3,4,5,6}; do $d/bin/antidote-admin cluster join 'dev1@127.0.0.1'; done
    Success: staged join request for 'antidote@127.0.0.1' to 'dev2@127.0.0.1'
    Success: staged join request for 'antidote@127.0.0.1' to 'dev3@127.0.0.1'
    Success: staged join request for 'antidote@127.0.0.1' to 'dev4@127.0.0.1'
    Success: staged join request for 'antidote@127.0.0.1' to 'dev5@127.0.0.1'
    Success: staged join request for 'antidote@127.0.0.1' to 'dev6@127.0.0.1'

Sends the requests to node1, which we can now tell to build the cluster:

     dev/dev1/bin/antidote-admin cluster plan
     ...
     dev/dev1/bin/antidote-admin cluster commit

Have a look at the `member-status` to see that the cluster is balancing.

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
cluster.

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

    erl -name 'client@127.0.0.1' -setcookie antidote

First check that we can connect to the cluster:

     (cli@127.0.0.1)1> net_adm:ping('dev3@127.0.0.1').
     pong

Then we can rpc onto any of the nodes and call `ping`:

    (cli@127.0.0.1)2> rpc:call('dev1@127.0.0.1', antidote, ping, []).
    {pong,662242929415565384811044689824565743281594433536}
    (cli@127.0.0.1)3>

And you can shut down your cluster like

    for d in dev/dev*; do $d/bin/antidote stop; done

When you start it up again, it will still be a cluster.
    
### Reading from and writing to a CRDT object stored in antidote:

#### Writing

Start a node (if you haven't done it yet):

	erl -name 'client@127.0.0.1' -setcookie antidote

Perform a write operation (example):

    (client@127.0.0.1)1> rpc:call('dev1@127.0.0.1', antidote, append, [myKey, {increment, 4}]).
    {ok,{1,'dev1@127.0.0.1'}}

The above rpc calls the function append from the module antidote:

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
	

	IMPORTANT: the update operation will execute no operation on the CRDT, will just store the operation in antidote. The execution of operations to a key occur when the CRDT is read.


#### Reading

Start a node (if you haven't done it yet):

	erl -name 'client@127.0.0.1' -setcookie antidote

Perform a read operation (example):

	(client@127.0.0.1)1> rpc:call('dev1@127.0.0.1', antidote, read, [myKey, riak_dt_gcounter]).
    1
    
The above rpc calls the function read from the module antidote:

	read(Key, Type)

where 

* `Key` = the key to read from.
* `Type` = the type of CRDT.

In the particular call we have just used as an example, 

* `myKey` = the key to read from.
* `riak_dt_gcounter` = the CRDT type, a gcounter

The read operation will materialise (i.e., apply the operations that have been stored since the last materialisation, if any) the CRDT and return the result as an {ok, Result} tuple.

		

Running Tests 
-------------

### Setup riak_test ###

1. Clone [https://github.com/SyncFree/riak\_test](https://github.com/SyncFree/riak_test) .Lets call it RIAK_TEST
2. Switch to the features/csm/floppystore branch:
	* git checkout features/csm/floppystore
2. cd RIAK_TEST and run commands
	* make

### Building antidote for testing ###

1. Go to antidote directory
2. make stagedevrel
3. ./riak\_test/bin/antidote-setup.sh (Only for the first time) 
    
	./riak\_test/bin/antidote-current.sh

### Running test ###

1. Go to RIAK_TEST directory
2. ./riak\_test -v -c antidote -t "TEST\_TO\_RUN"

	TEST\_TO\_RUN is any test module in antidote/riak_test/

	eg:- ./riak_test -v -c antidote -t clocksi_test


