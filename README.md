Antidote
============
[![Build Status](https://travis-ci.org/AntidoteDB/antidote.svg?branch=master)](https://travis-ci.org/AntidoteDB/antidote)
[![Coverage Status](https://coveralls.io/repos/github/AntidoteDB/antidote/badge.svg?branch=master)](https://coveralls.io/github/AntidoteDB/antidote?branch=master)

Welcome to the Antidote repository, the reference platform of the [SyncFree European Project](https://syncfree.lip6.fr/) and the [LightKone European Project](https://www.lightkone.eu/)

You will find all information in the documentation at [http://antidotedb.eu](http://antidotedb.eu).

For benchmarking Antidote deployments, checkout the [Antidote Benchmarks](https://github.com/AntidoteDB/Benchmarks) repository.



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

	# Run dialyzer to check types:
	make dialyzer

	# Open a shell:
	make shell

	# Build a release:
	make rel


### Code style

Before commiting code run `make lint` to check the code style.

In addition there are the following rules which are not checked automatically:

- Indentation should use 4 spaces (no tabs)
- Exported functions must be documented and have a type specification

### Working on dependencies

When working on dependencies of Antidote it can be helpful to use them as [Checkout Dependencies](https://www.rebar3.org/docs/dependencies#section-checkout-dependencies):

- Create a folder named `_checkouts` in your `antidote` folder (next to the `_build` folder)
- Clone the dependency into that folder. The folder name in `_checkouts` must be the name of the dependency in `rebar.config`.
   Note that symbolic links in the `_checkouts` folder are ignored. 
- When running a rebar3 task on Antidote, it will always use the latest version from the dependencies. It will also recompile all other dependencies, which can be avoided [by patching rebar3](https://github.com/erlang/rebar3/issues/2152)

