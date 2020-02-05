AntidoteDB
============

[![Build Status](https://travis-ci.org/AntidoteDB/antidote.svg?branch=master)](https://travis-ci.org/AntidoteDB/antidote)
[![Coverage Status](https://coveralls.io/repos/github/AntidoteDB/antidote/badge.svg?branch=master)](https://coveralls.io/github/AntidoteDB/antidote?branch=master)

Welcome to the Antidote repository, the reference platform of the [SyncFree European Project](https://syncfree.lip6.fr/) and the [LightKone European Project](https://www.lightkone.eu/).

Description
===========

AntidoteDB is a highly available geo-replicated key-value database.
AntidoteDB provides features that help programmers to write correct applications while having the same performance and horizontal scalability as AP/NoSQL databases.
Furthermore, AntidoteDB operations are based on the principle of synchronization-free execution by using Conflict-free replicated datatypes (*CRDTs*).


Features
=========

**CRDTs**

High-level replicated data types that are designed to work correctly in the presence of concurrent updates and partial failures.

**Highly Available Transactions**

Traditional ACID transactions were built for single-machine deployments. 
On the one hand, it is expensive to implement ACID transactions in distributed deployments. 
On the other hand, highly-available transactions (HAT) provide strong consistency within a data center, 
but still perform well in geo-replicated deployments.

**Geo-replication**

Designed to run on multiple servers in locations distributed world-wide. 
It provides continuous functioning even when there are failures or network partition.


How to Use
==========

You will find all information on the [project website](http://antidotedb.eu) or the [usage documentation](https://antidotedb.gitbook.io/documentation/).

Small tutorials on how to use Antidote can be found for [Java](https://github.com/AntidoteDB/antidote-java-tutorial) 
and [Jupyter Notebook](https://github.com/AntidoteDB/antidote-jupyter-notebook).

Topics:

* [Configuring Features of Antidote](https://antidotedb.gitbook.io/documentation/architecture/configuration)
* [Benchmarking Antidote](https://github.com/AntidoteDB/Benchmarks)
* Deploying Antidote
  * [Natively](https://antidotedb.gitbook.io/documentation/deployment/native)
  * [Local Docker setup](https://antidotedb.gitbook.io/documentation/deployment/docker)
  * [Docker compose setups](https://antidotedb.gitbook.io/documentation/deployment/docker-compose-setup)
  * [Docker Swarm](https://antidotedb.gitbook.io/documentation/deployment/dockerswarm)
  * [Kubernetes](https://antidotedb.gitbook.io/documentation/deployment/kubernetes)
* [Monitoring an Antidote instance or data center](https://github.com/AntidoteDB/antidote_stats)
* [Protocol Buffer API](https://antidotedb.gitbook.io/documentation/api/protocol-buffer-api)
  * [Erlang Client Repository](https://github.com/AntidoteDB/antidote-erlang-client)
  * [Java Client Repository](https://github.com/AntidoteDB/antidote-java-client)
  * [JavaScript Client Repository](https://github.com/AntidoteDB/antidote_ts_client)
  * [Go Client Repository](https://github.com/AntidoteDB/antidote-go-client)
  * [Python Client Repository](https://github.com/AntidoteDB/antidote-python-client)
  * [REST Client Repository](https://github.com/LightKone/antidote-rest-server)

Applications that use AntidoteDB:

* [Calender App](https://github.com/AntidoteDB/calender-app)
* [Antidote Web Shell](https://github.com/AntidoteDB/antidote-web-shell)


Contributing & Development
==============

Antidote encourages open-source development.
If you want to contribute, you can find all necessary information in the [developer documentation](https://antidotedb.gitbook.io/documentation/development/setup)
To make yourself familiar with AntidoteDB, you can start by looking at [good first issues](https://github.com/AntidoteDB/antidote/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).
