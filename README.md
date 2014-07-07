floppy: A Riak Core Application
======================================

Welcome to the Floppystore repository, the reference platform of the SyncFree european project (https://syncfree.lip6.fr/).


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


