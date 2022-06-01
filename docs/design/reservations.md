# Reservations

Reservations is an internal service that enables invariant preservation on top of Antidote.

Reservations are assigned to transactions when accessing invariant-sensitive data, for instance, when modifying an entity that has inbound or outbound references to other entities. Vaxine identifies what reservations are necessary for executing certain updates by analysing the schema of the database for the entities involved in the update.

The reservations service provides different types of reservation tokens that can be used to enforce different kinds of constraints. Reservations are managed by reservation brokers which are typically deployed one per region. Reservation brokers exchange reservations between themselves to allow safe uncoordinated writes in the wide-area. Reservation brokers can have internal redundancy to avoid that reservations in a region become inaccessible in case of failures.

When assigning reservation tokens, It is necessary to ensure that the clients executing a transaction and the local brokers are in the same causal context to ensure that the database transitions between consistent states. Yes, you are right! We can use Antidote as a backend for the reservations service to ensure consistency between reservation tokens and data.



## TODO

This doc provides the specification of the Reservations Service, including its API, protocols and supported reservation types.