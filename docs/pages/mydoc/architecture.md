---
title: Architecture
last_updated: September 16, 2016
tags: [architecture]
sidebar: mydoc_sidebar
permalink: architecture.html
folder: mydoc
---

To provide fast parallel access to different data, data is sharded among the servers
within a cluster using consistent hashing and organized in a ring. A read/write
request is served by the server hosting a copy of the data. A transaction that reads/
writes multiple objects contacts only those servers that have the objects accessed
by the transaction. This master-less design allows serving of requests even when
some servers fail.

Antidote employs Cure, a highly scalable protocol, to replicate the updates
from one cluster to other. The updates are replicated asynchronously to provide
high availability under network partitions. 

Cure provides causal consistency
which is the highest consistency model compatible with high availability. Causal
consistency guarantees that related events are made visible according to their order
of occurrence, while the unrelated events (events that occurred concurrently) can
be in different order in different replicas. 

For example, in a social networking
application a wall post has happened before a reply to it. Therefore no user must
see the reply before the post itself. Causal consistency provides these guarantees.

Cure also allows applications to pack reads and writes to multiple objects in a
transaction. The transactions together with causal consistency helps to read and
update more than one object in a consistent manner.

{% include image.html file="architecture.png" %}

Each partition in AntidoteDB mainly consists of the following four components:

### Log
This module implements a log-based persistent
  layer. Updates are stored in a log, which is persisted to disk for
  durability. The module also internally maintains a cache layer in order to make
  accesses to the log faster. Further details can be found [here](/antidote/log.html).

### Materializer
This module is responsible for generating and caching
  the object versions requested by
  clients. It is placed between the log and the 
    transaction manager modules. To avoid
   system degredation over time it
   incorporates some pruning mechanisms.

### Transaction Manager
This module implements the
  transaction protocol. It receives client's requests, executes
  and coordinates transactions, and replies back to clients. 

### InterDC Replication 
This module is in charge of fetching
  updates from the log, and propagating them to other data
  centers. Communication is done partition-wise.



{% include links.html %}
