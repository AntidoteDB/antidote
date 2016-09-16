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

Information about Antidote's layered design can be found in the following [Google Doc](https://docs.google.com/document/d/1SNnmAtx5FrcNgEMdNQkKlfzYc1tqziaV2lQ6g9IQyzs/edit#heading=h.ze32da2pga2f)


{% include links.html %}
