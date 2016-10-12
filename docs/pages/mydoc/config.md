---
title: Configuration
last_updated: August 25, 2016
tags: [configuration]
sidebar: mydoc_sidebar
permalink: config.html
folder: mydoc
---

Antidote can be configured to use different transaction protocols and recovery strategies.

## Options for environmental variables

| Parameter | Value | Description |
| ----------|-------|------------ |
| txn_cert | true  | Write operations are certified during commit. The transaction is aborted if a write conflict is detected (i.e. snapshot isolation is ensured for the updates within a single DC, updates across DCs are not checked) (default). |
|           | false | Transactions perform no certification check and always commit (if they do not crash).|
| txn_prot | clocksi | Uses the [Cure protocol](https://pages.lip6.fr/Marc.Shapiro/papers/Cure-final-ICDCS16.pdf) to define snapshots and causal dependencies  (default). |
|          | gr | Uses the [Gentle-rain](https://infoscience.epfl.ch/record/202079) protocol to define snapshots and causal dependencies.|
| recover_from_log | true | When starting a node, it loads any operations stored on the disk log to the in memory cache of the key-value store (default). |
|       | false | When starting a node, the key-value store is empty.|
| recover_meta_data_on_start | true | Meta-data state is loaded from disk on restart including connection state between other DCs and node names and configurations. Nodes  automatically reconnect to other DCs on restart (default).
|        | false | meta-data concerning node names and connections to other DCs is not loaded on restart. |
| sync_log | true | Local transactions are stored in the log synchronously, i.e., when the reply is sent, the updates are guaranteed to have been                stored to disk (this is very slow in the current logging setup).
|        | false | All updates are sent to the operating system to be stored on disk (eventually), but are not guaranteed to be stored durably on disk                 when the reply is sent (default) |
