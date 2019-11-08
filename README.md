Antidote
============
[![Build Status](https://travis-ci.org/AntidoteDB/antidote.svg?branch=master)](https://travis-ci.org/AntidoteDB/antidote)
[![Coverage Status](https://coveralls.io/repos/github/AntidoteDB/antidote/badge.svg?branch=master)](https://coveralls.io/github/AntidoteDB/antidote?branch=master)

Welcome to the Antidote repository, the reference platform of the [SyncFree European Project](https://syncfree.lip6.fr/) and the [LightKone European Project](https://www.lightkone.eu/)

You will find all information in the documentation at [http://antidotedb.eu](http://antidotedb.eu).

For benchmarking Antidote deployments, checkout the [Antidote Benchmarks](https://github.com/AntidoteDB/Benchmarks) repository.

For monitoring Antidote deployments, checkout the [Antidote Stats repository](https://github.com/AntidoteDB/antidote_stats) repository.



Development
-----------

Antidote requires Erlang 21 or greater.

Use the following `Makefile` targets to build and test antidote:

	# compile the project:
	make compile

	# run the unit tests:
	make test

	# run the system tests:
	make systests
	
	# run the release test:
	make reltest

	# run dialyzer to check types:
	make dialyzer
	
	# run linter
	make lint

	# open a shell:
	make shell

	# build a release:
	make rel
	
Using a Release
-----------

After building a release with `make rel`, Antidote can be started with the generated executable file.
The release executable is located at `_build/default/bin/rel/antidote/releases/bin/antidote`.
It should be started with the node name and cookie configured. 
Setting the cookie is optional, setting the node name is mandatory.

	# start antidote with given short node name as a foreground process
	NODE_NAME=testing ./antidote foreground
	
	# start antidote with given long node name with an erlang interactive console attached
	NODE_NAME=testing ./antidote console
	
	
Dynamic Release Configuration
-----------

The release executable is dynamically configurable with OS environment variables.
The default configuration can be found in the config file folder `config`.
Only `NODE_NAME`, `COOKIE`, `ROOT_DIR_PREFIX`, `LOGGER_DIR_PREFIX` and `DATA_DIR_PREFIX` are expected to be provided via OS environment variables.
All other configuration has to be set via [erlang application environment arguments](http://erlang.org/doc/man/erl.html#flags) `-Application Par Val`, 
where variable expansion can be used in the `Val` part.

Example:

```shell script
# start antidote with a unified directory, set logging level to warning
# assume NODE_NAME, ROOT_PREFIX_DIR (`antidote/`), P1, P2, P3, P4, and P5 OS variables 
# have been exported before
# modify appliation configurations (see sys.config.src and other .config files for options)
# logging level and all ports
# disable certification checks for concurrent transactions
./antidote foreground \
-kernel logging_level warning \
-riak_core handoff_port $P1 \
-ranch pb_port $P2 \
-antidote pubsub_port $P3 \
-antidote logreader_port $P4 \
-antidote_stats metrics_port $P5 \
-antidote txn_check false
```

Code style
-----------

Before commiting code run `make lint` to check the code style.

In addition there are the following rules which are not checked automatically:

- Indentation should use 4 spaces (no tabs)
- Exported functions must be documented and have a type specification

Working on dependencies
-----------

When working on dependencies of Antidote it can be helpful to use them as [Checkout Dependencies](https://www.rebar3.org/docs/dependencies#section-checkout-dependencies):

- Create a folder named `_checkouts` in your `antidote` folder (next to the `_build` folder)
- Clone or symlink the dependency into that folder. The folder name in `_checkouts` must be the name of the dependency in `rebar.config`.
- When running a rebar3 task on Antidote, it will always use the latest version from the dependencies. It will also recompile all other dependencies, which can be avoided [by patching rebar3](https://github.com/erlang/rebar3/issues/2152)

