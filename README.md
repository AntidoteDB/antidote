Antidote
============
[![Build Status](https://travis-ci.org/AntidoteDB/antidote.svg?branch=master)](https://travis-ci.org/AntidoteDB/antidote)

Welcome to the Antidote repository, the reference platform of the [SyncFree European Project](https://syncfree.lip6.fr/) and the [LightKone European Project](https://www.lightkone.eu/)

You will find all information in the documentation at [http://antidotedb.eu](http://antidotedb.eu).

For benchmarking Antidote deployments, we currently use [basho bench](https://github.com/SyncFree/basho_bench/tree/antidote_pb-rebar3-erlang19).



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


