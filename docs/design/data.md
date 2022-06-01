# Data

The Data component is further split into three components:

*  **Query language**: to express and execute queries
*  **Schemas**: to declare the database structure
*  **Migrations**: to modify the database schema

As a starting point we are creating an [Ecto](https://hexdocs.pm/ecto/) adapter for Antidote protobuf API. This strategy gives us a quick integration, so that developers can start building Elixir applications using our platform. We make the Vax layer contact Antidote directly, bypassing the Vaxine layer (refer [here](https://github.com/vaxine-io/internal/blob/main/design/index.md#architecture) for a complete architecture diagram).  When some of Vaxine's features becomes available, Vax will start talking to the Vaxine layer instead. Don't worry, when that day arrives, the only thing you will have to do is change your Ecto adapter and possibly some configurations, and we ensure your applications remains compatible out-of-the-box.



## Query language

Exposing a top-level query interface allows the decoupling between data access and the internal representation of data, providing a common API on which different clients can be built. However, the envisioned API departs from most traditional query language protocols in which queries are passed in a serialised form from the client to the server to be executed. We will leverage a more synergic relationship between the client and the server to enable surgical precise operations, that reduce unnecessary writes and avoid coordination whenever possible.



## Schema

A Schema defines the structure of a database and ensures that updates maintain its integrity. Schemas in Vaxine face two major challenges:

* Efficiently map the relational data model used in Vaxine to the more low-level key-CRDT data model provided by Antidote
* Maintain database integrity in the presence of concurrent updates



## Migrations

Migrations allow evolving database schemas in a reproducible way. Migrations in Vaxine might additionally include the creation or the deletion of reservation tokens, depending on the constraints defined in the database schema. For that reason the migration protocol requires tight integration with the reservation broker. Unlike migrations in SQL databases, the need to reclaim and creating new reservations might affect the success of a migration.



## TODO

- The mapping of relational data into the KV-CRDT model of Antidote
- The description of the query language supported
- A roadmap of query features that will be supported over time