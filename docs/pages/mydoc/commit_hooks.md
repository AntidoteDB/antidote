---
title: Commit hooks
last_updated: September 5, 2016
tags: [features]
sidebar: mydoc_sidebar
permalink: commit_hooks.html
folder: mydoc
---

Commit hooks are executed when an object is updated in Antidote. Pre-Commit hooks are execute before the object is updated and post-commit hooks after the update is executed successfully.

Commit hooks are erlang functions which can be called with in Antidote. A commit hook function must take one argument of type update_object() and returns update_object().

    fun (update_object()) -> {ok, update_object()} | {error, Reason}.
    -type update_object() :: { {key(), bucket()}, crdt_type(), update_operation() }
    -type update_operation() :: {atom(), term()}

Commit hooks can be registered per bucket.

## Pre-Commit hooks

Pre-commit hooks are executed before an object is being updated. If pre-commit hook fails, the entire transaction is aborted.

Pre-commit hooks takes one argument which is the client issued update operation to the object. It can then return a modified operation. This modified operation will be used to update the object.

Following is a pre-commit hook which modifies single increment  to increment by two.

```erlang
my_increment_hook({ {Key, Bucket}, antidote_crdt_counter_pn, {increment, 1} }) ->
    {ok, { {Key, Bucket}, antidote_crdt_counter_pn, {increment, 2} }}.
```

## Post-commit hooks

Post commit hooks are executed after the transaction is successfully committed and before the reply is sent to the client. Currently if post commit hook is failed, it is ignored and transaction is still considered to be successfully committed.

Similar to pre-commit hooks, post-commit hooks receives an operation to the object. This operation is either

* client issued update operation, if there was no pre-commit hook executed
* operation returned by pre-commit hook, if there was a pre-commit hook

Following is a post commit hook which increments a commit-count for every update to a key.

```erlang
    my_commit_count_hook({ { Key, Bucket }, Type, OP }) ->
        _Result = antidote:update_objects(ignore, [],
                                          [{ {Key, antidote_crdt_counter_pn, commitcount}, increment, 1}]),
        {ok, { {Key, Bucket}, Type, OP} }.
```

Note that if post commit hook update objects, it may trigger more hooks and result in infinite cycles of hooks execution. Therefore, hook functions should be carefully written.

## Registering Commit hooks

A commit hook is registered per bucket. A bucket can have any number of pre and post commit hooks. However, the order of execution of these hooks (if more than one exist) cannot be specified.

```erlang
register_post_hook(bucket(), module_name(), function_name()) -> ok | {error, function_not_exported}.
register_pre_hook(bucket(), module_name(), function_name()) -> ok | {error, function_not_exported}.
unregister_hook(pre_commit | post_commit, bucket()) -> ok.
```
