---
title:
keywords: homepage
tags: [index]
toc: false
sidebar: mydoc_sidebar
permalink: index.html
---

{% include image.html file="company_logo_big.png" %}

# A planet-scale, available, transactional database with strong semantics

*  Your cloud-scale application must be highly available?
*  You need to serve millions of customers around the world with low latency responses?
*  You are tired of fixing inconsistencies in your key-value store?


## Why Antidote? ##

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
such concurrent updates and resolve conflicting modifications. Antidote provides
features that aid programmers to write correct applications, while having the same
performance and horizontal scalability as AP/NoSQL, from a single machine to
geo-replicated deployments, with the added guarantees of Causal Highly-Available
Transactions, and provable absence of data corruption due to concurrency.


<div class="row">
         <div class="col-lg-12">
             <h2 class="page-header">Characteristics</h2>
         </div>
         <div class="col-md-3 col-sm-6">
             <div class="panel panel-default text-center">
                 <div class="panel-heading">
                     <span class="fa-stack fa-5x">
                           <i class="fa fa-circle fa-stack-2x text-primary"></i>
                           <i class="fa fa-database fa-stack-1x fa-inverse"></i>
                     </span>
                 </div>
                 <div class="panel-body">
                     <h4>CRDTs</h4>
                     <p>High-level replicated data types that are designed to work correctly in the
                       presence of concurrent updates and partial failures.</p>
                     <a href="rawapi.html" class="btn btn-primary">Learn More</a>
                 </div>
             </div>
         </div>
         <div class="col-md-3 col-sm-6">
             <div class="panel panel-default text-center">
                 <div class="panel-heading">
                     <span class="fa-stack fa-5x">
                           <i class="fa fa-circle fa-stack-2x text-primary"></i>
                           <i class="fa fa-graduation-cap fa-stack-1x fa-inverse"></i>
                     </span>
                 </div>
                 <div class="panel-body">
                     <h4>Highly Available Transactions (HATs)</h4>
                     <p>In some cases, the application needs to maintain
                    some relation between updates to different objects. Antidote provides causal
                    consistency across all replicas, snapshot reads and atomic multi-object updates.</p>
                     <a href="rawapi.html" class="btn btn-primary">Learn More</a>
                 </div>
             </div>
         </div>
         <div class="col-md-3 col-sm-6">
             <div class="panel panel-default text-center">
                 <div class="panel-heading">
                     <span class="fa-stack fa-5x">
                           <i class="fa fa-circle fa-stack-2x text-primary"></i>
                           <i class="fa fa-globe fa-stack-1x fa-inverse"></i>
                     </span>
                 </div>
                 <div class="panel-body">
                     <h4>Geo-replication</h4>
                     <p>Designed to run on multiple servers in
                    locations distributed world-wide. It provides continuous functioning
                    even when there are failures or network partition.</p>
                     <a href="architecture.html" class="btn btn-primary">Learn More</a>
                 </div>
             </div>
         </div>
</div>


## Where academia meets industry ##

Antidote is an artifact of the [SyncFree Project](https://syncfree.lip6.fr/).
It incorporates cutting-edge research results on distributed systems and modern data stores, meeting for your needs!

Antidote is the first to provide all of these features in one data store:

*  Transactional Causal+ Consistency
*  Replication across DCs
*  Horizontal scalability by partitioning of the key space
*  Operation-based CRDT support
*  Partial Replication

The [Whitepaper](https://syncfree.lip6.fr/index.php/white-papers) gives a comprehensive
overview of Antidote's design goals.

Hint: Some features are still experimental and are not available on the master branch.


{% include links.html %}
