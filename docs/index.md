---
title: AntidoteDB - A cloud scale key-value database
keywords: homepage
tags: [index]
toc: false
sidebar: mydoc_sidebar
permalink: index.html
---

*  Your cloud-scale application must be highly available?
*  You need to serve millions of customers around the world with low latency responses?
*  You are tired of fixing inconsistencies in your key-value store?


## Why AntidoteDB? ##

Traditional databases provide
strong guarantees but are slow and unavailable under failures and network partition.
Hence, they are not suitable for geo-replication. The alternatives are NoSQL-style
databases which are fast and available even under network partition. They provide
a low-level key-value interface and expose data inconsistencies due to asynchronous
communication among the servers. It takes significant effort and expertise from
programmers to deal with these inconsistencies and develop correct applications on
top of these databases.

For example, consider that your application stores a counter which counts the
number of ads displayed to a user. For scalability, the database replicates all data in
different locations. What will be the value of the counter, when it is incremented at
two locations at the same time? As an application programmer, you have to detect
such concurrent updates and resolve conflicting modifications. AntidoteDB provides
features that aid programmers to write correct applications, while having the same
performance and horizontal scalability as AP/NoSQL, from a single machine to
geo-replicated deployments, with the added guarantees of Causal Highly-Available
Transactions, and provable absence of data corruption due to concurrency.


<div class="row">
        <div class="col-lg-12">
            <h2 class="page-header"></h2>
        </div>
        <div class="col-lg-12">

            <ul id="myTab" class="nav nav-tabs nav-justified">
                <li class="active"><a href="#service-one" data-toggle="tab"><i class="fa fa-tree"></i> CRDTs </a>
                </li>
                <li class=""><a href="#service-two" data-toggle="tab"><i class="fa fa-car"></i> Highly Available Transactions </a>
                </li>
                <li class=""><a href="#service-three" data-toggle="tab"><i class="fa fa-support"></i> Geo-replication </a>
                </li>
            </ul>

            <div id="myTabContent" class="tab-content">
                <div class="tab-pane fade active in" id="service-one">
                    <h4>CRDTs</h4>
                    <p>Antidote supports high-level replicated data types such as
                       counters, sets, maps, and sequences that are designed to work correctly in the
                       presence of concurrent updates and partial failures.
                    </p>
                </div>
                <div class="tab-pane fade" id="service-two">
                    <h4>Highly Available Transactions</h4>
                    <p>In some cases, the application needs to maintain
                    some relation between updates to different objects. For example, in a social
                    networking application, a reply to some post should be visible to a user only after
                    s/he observed the post. Antidote maintains such relations by providing causal
                    consistency across all replicas and atomic multi-object updates. Thus, programmers
                    can program their application on top of Antidote without worrying about the
                    inconsistencies arising due to concurrent updates in different replicas.</p>
                </div>
                <div class="tab-pane fade" id="service-three">
                    <h4>Geo-replication</h4>
                    <p>Antidote is designed to run on multiple servers in geo-distributed
                    locations. To provide fast responses to read and write requests, Antidote automatically
                    replicates data in different locations and serves the requests from the nearest
                    location without contacting a remote server. It provides continuous functioning
                    even when there are failures or network partition.</p>
                </div>
            </div>

        </div>
    </div>


## Where academia meets industry ##
AntidoteDB is an artifact of the [SyncFree Project](https://syncfree.lip6.fr/).
It incorporates cutting-edge research results on distributed systems and modern data stores, meeting for your needs!

AntidoteDB is the first to provide all of these features in one data store:

*  Transactional Causal+ Consistency
*  Replication across DCs
*  Horizontal scalability by partitioning of the key space
*  Operation-based CRDT support
*  Partial Replication

Hint: Some features are still experimental and are not available on the master branch.

## Architecture ##

Information about Antidote's layered design can be found in the following [Google Doc](https://docs.google.com/document/d/1SNnmAtx5FrcNgEMdNQkKlfzYc1tqziaV2lQ6g9IQyzs/edit#heading=h.ze32da2pga2f)


{% include links.html %}
