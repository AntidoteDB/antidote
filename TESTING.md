Antidote Testing
================

* [Unit Tests](#unit-tests)
* [System Tests](#system-tests)
* [Release Tests](#release-tests)
* [Test Logging](#test-logging)
* [Utility Functions](#utility-functions)

Unit Tests
----------

EUnit is used for unit testing. 
Unit tests reside in the module they are testing and test the module in isolation. 
The basic test skeleton should be encapsulated in a TEST block.


```
-ifdef(TEST).
main_test_() ->
    {foreach,
        fun setup/0, fun cleanup/1,
        [ fun my_test/1, ... ]
    }.

% Setup and Cleanup
setup() -> io:format(user,"setup",[]), ...

cleanup(Args_from_setup_return) -> ...

...
-endif.
```

* All tests can then be executed via `make test`.
* A single module can be tested via `rebar3 eunit --module=MODULE_NAME`
* Log output is consumed; use `io:format/3` for debugging purposes


System Tests
----------------------------------

System test suite tests a certain aspect of the system and can be either a single data center or a multi-datacenter suite.

A suite should be name `..._SUITE.erl` by convention.

To execute system tests, execute one of the following commands

    make singledc 
    make singledc SUITE=...
    make multidc
    make multidc SUITE=...
    make systests


#### Bucket Namespace

Every system test should have its own bucket namespace defined at the top of the suite. 
Unique bucket namespaces ensure that test suites do not interfere with each other. 
The convention is filename with SUITE replaced by bucket.
E.g., the bucket for `antidote_SUITE.erl` should be defined as

    -define(BUCKET, antidote_bucket).

and the macro should be used *once* at the start of a test case, defining the bucket to be used for that test case. 
If a unique bucket *per test case* is needed (e.g. for parallel tests), then

    -define(BUCKET, test_utils:bucket(antidote_bucket)).

can be used instead.


#### Multiple Data Center System Test Setup

System tests which use multiple data centers are located in the test/multidc folder.
The fixed test setup is as follows:

* 3 interconnected Data center
* Data center one has two physical nodes



Release Tests
-------------

`make reltest` tests the antidote release executable.


Test Logging
-------------

To log messages in a test suite, use the function `ct:log`. If a message should be visibile in the printed log (e.g. in travis), then `ct:pal` or `ct:print` can be used. `lager` should not be used for logging in system tests.


Utility Functions
-----------------

There are multiple help functions already implemented for common functionality in the `test/utils` folder. They are separated by functionality into the following files:

* `antidote_utils`
  * Functions to manipulate antidote data types
* `riak_utils`
  * Riak cluster management. Useful for multi-dc tests.
* `time_utils`
  * Time-based helper functions
* `test_utils`
  * Test initialization and other helper functions which do not fit into the other categories


