![Vaxine logo](https://vaxine.io/id/vaxine-logo-dark.png#gh-dark-mode-only)
![Vaxine logo](https://vaxine.io/id/vaxine-logo-light.png#gh-light-mode-only)

Welcome to the Vaxine source code repository. Vaxine is a rich-CRDT database system based on AntidoteDB.

![Erlang CI](https://github.com/vaxine-io/vaxine/workflows/Erlang%20CI/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/vaxine-io/vaxine/badge.svg?branch=main)](https://coveralls.io/github/vaxine-io/vaxine?branch=main)

## About Vaxine

Vaxine is a rich-CRDT database system that extends Antidote with a relational-oriented data-model, the ability to maintain invariants, a query language and real-time subscriptions service. Vaxine enables developers to create applications that are scalable and maintain consistency, without making the typical availability sacrifices of CP systems.

Applications built on top of Vaxine offer:

* Geo-distributed deployments
* Multi-writer, low-latency writes
* Integrity constraints
* Real-time update subscriptions

More information:

- [Vaxine website](https://vaxine.io)
- [Vaxine documentation](https://vaxine.io/docs)
- [Example applications](https://vaxine.io/demos) ([source code](https://github.com/vaxine-io/examples))

## About Antidote

AntidoteDB is a highly available geo-replicated key-value database based on Conflict-free replicated datatypes (*CRDTs*) and Highly Available Transactions. CRDTs are high-level replicated data types that are designed to work correctly in the presence of concurrent updates and partial failures. Highly Available Transactions provide atomic transactional updates and strong consistency within a data center, whilst still performing well in geo-replicated deployments.

More information:

- [Antidote website](https://www.antidotedb.eu)
- [Antidote documentation](https://antidotedb.gitbook.io/documentation)

Antidote is the reference platform of the [SyncFree European Project](https://syncfree.lip6.fr/) and the [LightKone European Project](https://www.lightkone.eu/).

## Community Guidelines

This repo contains guidelines for participating in the Vaxine community:

* [Code of Conduct](./CODE_OF_CONDUCT.md)
* [Guide to Contributing](./CONTRIBUTING.md)
* [Contributor License Agreement](./CLA.md)

If you have any questions or concerns, please raise them on the [Vaxine community](https://vaxine.io/project/community) channels.
