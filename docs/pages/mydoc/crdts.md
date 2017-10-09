---
title: Datatypes in Antidote
tags: [crdts]
sidebar: mydoc_sidebar
permalink: crdts.html
folder: mydoc
---


Most highly available databases only provide last-writer-wins registers, which resolve concurrent updates using timestamps.
Antidote offers a variety of other datatypes with smarter strategies to resolve concurrent updates.
The available data types are described below.

All data types have in common, that their value only depends on the operations which have been delivered to the currently used Antidote data center.
There can always be operations which are not yet visible, for example because they happened concurrently on a different datacenter.

In the descriptions below the word "after" always means "causally after".
An operation which happens later in time can be concurrent, if it happened independently.

Many datatypes support a `reset` operation, which is used when nesting a datatype in a map-datatype.
Therefore the details of `reset` are explained with the map-datatypes below.



## Last-writer-wins Register (LWW-Register)

Reading a Last-writer-wins Register (LWW-Register) returns the latest written value based on timestamps.
The timestamps respect causality, so assigning a new value is guaranteed to overwrite previous values.

The initial value is the empty binary.

#### Operations

assign(value)
:	Assigns a value to the register.

## Multi-value Register (MV-Register)

Reading a Multi-value Register (MV-Register) returns a list of all concurrently assigned values, which have not been overridden by other assignments.
The returned list is ordered by the lexicographic order on the byte representation of the value.

In the initial state reading the register returns the empty list.


#### Operations

assign(value)
:	Assigns a value to the register.

reset()
:   Resets the register to the initial state (empty list).

## Number Types

### Counter

The counter datatype only allows incrementing and decrementing the value.
Reading the value returns the aggregated value from all received operations.

#### Operations

increment(v)
:	Increments the counter by `v`.

decrement(v)
:	Decrements the counter by `v`.


### Fat Counter

The fat counter offers the same functionality as the counter above but additionally supports the `reset` operation, which is useful for embedding the counter in a map datatype.

The cost of this additional feature is an increased storage size, which is linear in the number of operations since the last reset.


#### Operations

increment(v)
:	Increments the counter by `v`.

decrement(v)
:	Decrements the counter by `v`.

reset()
:   Resets the counter to the initial state (0), cancelling all observed increments and decrements.



### Integer

The integer extends the counter with an additional `set`-operation which allows to assign a value to the integer.

Intuitively concurrent updates are resolved by taking the maximum in the case of concurrent `set`-operations and by considering all increments and decrement operations which are not overwritten by the `set`-operation.

More precisely, the result of reading the value can be specified as follows:

- If there are no `set` operations, the semantics are equivalent to the counter datatype.
- Otherwise, let `S` be the set of all concurrent `set`-operations, which have not been overridden.
- For each operation `o` in `S` with argument `assign(v)`, take all increments and decrements concurrent to `o` and aggregate them onto `V`.
- Return the maximum of the values obtained from the previous step.

#### Operations

increment(v)
:	Increments the integer by `v`.

decrement(v)
:	Decrements the integer by `v`.

set(v)
:   Sets the value of the integer to `v`.

reset()
:   Same as `set(0)`.
	This is not a standard implementation of `reset` since  it might affect concurrent updates and does not reset the internal state to the initial state.

## Flags

Flags store a Boolean value (`true` or `false`).

Antidote has two variants for flags, which differ in how concurrent updates are resolved.

The **enable-wins flag** (EW-flag) returns true, if (and only if) there is an `enable`-operation among the latest operations.

The **disable-wins** (DW-flag) flag return true, if (and only if) is an `enable`-operation and no `disable` operation among the latest operations.

Here "latest operations" are all operations which have not been overridden by a later operation.


#### Operations

enable()
:	Sets the flag to `true`.

disable()
:   Sets the flag to `false`

reset()
:   Resets the flag to the initial state (`false`).


## Maps

Maps in Antidote can store values under keys.
The keys are simple (binary) values.
Values are again datatypes and updating nested values is done by sending operations to these embedded values.
Similarly, reading a value reads all the embedded values and returns them with their respective key.

The different variations of maps differ in how they handle `remove`-operations.

All implementations have in common, that they call `reset` on the removed value.
Intuitively, a reset-operation Should have the effect of removing all previous operations, so that the state is equivalent to the initial state, but operations concurrent to the `reset` are not lost.
However, not all datatypes support a `reset` and not all follow this principle (usually for performance reasons).




#### Operations

update(nestedOps)
:	Sends a list of nested operations to the

remove(keys)
:   Removes entries from the map based on keys.

reset()
:   Resets the map.


### Grow-only Map (G-Map)

The grow-only map does not support removing entries.
It can be used for representing objects/structs in Antidote, where fields are not dynamically added and removed.

### Add-wins Map (AW-Map)

A `remove` on an add-wins map resets the embedded datatype by calling its reset operation.
Moreover, it marks the entry as removed, so that it does not show up when reading the value of the map.

Concurrent updates on a key will overwrite the removed-marker (i.e. adds via update-operation win over concurrent removes, hence the name).

The drawback of this implementation is, that markers for removed elements might never be cleaned up, so it should only be used when the set of keys is limited.

### Remove-Resets Map (RR-Map)

Similar to the add-wins map, calling `remove` on a remove-resets map resets the embedded datatype by calling its reset operation.

If the internal state of an embedded datatype becomes equal to its initial state, it is removed from the map.
When calling `reset` this is the case for add-wins sets, multi-value registers, fat counters, flags and remove-reset maps which only contain the types in this sentence (read this as an inductive definition).
Therefore only these types should be used with remove-reset maps.

Since the internal state of data types determines whether an entry exists or not, it is not advisable to depend on the set of entries with this implementation.
It is also not possible to store an entry with an empty add-wins set in this map, since the empty set is equivalent to the initial state.


## Sets


There are several implementations of sets, which differ in how they handle concurrent operations.

Reading a set returns the values in the set ordered by the lexicographic order on the byte representation of the values.

#### Operations

add(element) / add_all(elements)
:	Add elements to the set.

remove(element) / remove_all(elements)
:   Removes elements from the set.

reset()
:   Resets the set.

### Grow-only Set (G-Set)

The grow-only set does not support removing elements or resetting the state.
Reading it simply returns all elements added to the set.

### Add-wins Set (AW-Set / OR-Set)

In the add-wins set (also called observed-remove set), add-operations win over concurrent remove-operations.
A remove-operation will only "overwrite" the add-operations that happened before it.

More precisely, an element is in the set, if there is an add-operation for the element which is not followed by a remove-operation for the same element (or a reset-operation).

A reset-operation has the effect of removing all preceeding operations.

### Remove-wins Set (RW-Set)

In the remove-wins set, remove-operations win over concurrent add-operations.
Here, an add-operation overwrites only the remove-operations that happened before it.

More precisely, an element is removed, if there is a remove-operation for the element which is not followed by an add-operation for the same element.
An element is in the set, if it has been added and not removed.


A reset-operation has the effect of removing all preceeding operations.
