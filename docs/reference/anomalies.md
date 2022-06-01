---
layout: docs
title: Anomalies
redirect_from: /docs/reference
---

# Anomalies

Multiplayer applications put extra pressure in achieving good user experience. Applications need to be fast and responsive, and at the same time provide a sensible combination of user actions coming from different sources that make interactions feel natural and enjoyable.

Many applications are built on top of strong consistency models, i.e., with operations originating from different replicas appearing to execute sequentially one after the other, with stricter or more relaxed time constraints. Under strong consistency, operation execution is typically coordinated across replicas to arbitrate the ordering of operations at every replica. It is common to divide workloads by regions to mitigate the effects of coordination over large distances.

Strong consistency provides a simple programming model that is convenient for developers. However, and despite best engineering efforts, strong consistency systems are difficult to adapt to multiplayer applications. There are two main problems that strong consistency systems can’t solve:

1. more geographically distant users result in global higher operation latencies (not just the perceived latency of remote operations)
1. entropy on writes results in higher rates of aborts, which is not good for collaborative experience

For these reasons, strong consistency is not suitable. Async operation execution under eventual consistency still difficult because many anomalies. Vaxine uses Rich-CRDTs and Transactional Causal+ Consistency to prevent much of these anomalies.

## Summary table

The following table summarises the anomalies that occur with different database technologies and illustrates how Rich-CRDTs with TCC+ both avoids aborts and flickering that are common with CP systems and prevents a range of anomalies possible under pure CRDT and OT systems:

<div class="table-responsive tbody-primary-strongs" markdown="1">

| Anomaly                         | OT             | CRDTs          | TCC+ & Rich-CRDTs | Serializability |
| ------------------------------- | -------------- | -------------- | ----------------- | --------------- |
| Causality violation             | Single object  | Single object  | **No**            | **No**          |
| User intention violation        | Some           | Some           | Some              | **No**          |
| Cumulative effects              | Yes            | Yes            | **No**            | **No**          |
| Flickering                      | Some           | Some           | **No**            | Yes (aborts)    |
| Broken referential integrity    | Yes            | Yes            | **No**            | **No**          |
| Ownership clash                 | Yes            | Yes            | **No**            | **No**          |
| Long-lived actions interference | Yes            | Yes            | **No**            | **No**          |
| Duplication of elements         | Yes            | Yes            | **No**            | **No**          |
| Tree cycles                     | Yes            | Yes            | **No**            | **No**          |
| Convergence inconsistencies     | Yes            | Yes            | Some              | Not applicable  |
| Ghost movements                 | Yes            | Yes            | Yes               | Yes             |
| Proximity bias                  | Not applicable | Not applicable | Randomised        | Yes             |
| Offline interactions            | Some           | Offline-first  | Online            | Online          |

</div>

## Causality violation

**Problem**:<br />Operations become visible before their dependencies are also made visible, influencing the user’s action. I.e.: the user acts on information that has already been superceeded.

**Example**:<br />Elements in a creativity application are organised by color; user changes the color of an element and arranges accordingly. Operations are delivered out of order. Another user sees that the element was positioned in the wrong group (according to the color) and moves the element to a different group.

**Example**:<br />User changes the access rights for some document, removing the participants that have lower clearance level. Subsequently modified the content of that document with sensitive data. If these two changes are delivered out of order, a user with lower clearance level might be able to see the sensitive information.

**Mitigation**:<br />Latency of operations is so low that the user observing the operations out of order doesn’t have enough time to act before the operation out of order is delivered.

**Solution**:<br />Transactional Causal Consistency+, the strongest consistency model under High Availability, which prevents reordering of causal-related operations.

<figure class="figure mt-2">
  <a href="../_assets/images/reference/causality-violation.gif"
      class="no-visual">
    <img src="../_assets/images/reference/causality-violation.gif"
        class="figure-img img-fluid"
    />
  </a>
  <figcaption class="figure-caption text-end">
    Illustration of a causality violation anomaly.
  </figcaption>
</figure>

## User intent

**Problem**:<br />The conflict-resolution strategy applied to solve conflicting updates leaves the object in a state that does not reflect the intention of individual changes.

**Example**:<br />A user inserts a word at the beginning or the end of a sentence, while another user changes the style of the same sentence. Merging the changes results in an inconsistent style for the sentence.

**Mitigation**:<br />Capturing user intention is a domain-specific problem. Many times arbitration can only go so far as preserving the intention of one of the users (if they are conflicting)

**Solution**:<br />develop data-types that have more complex internal representation of the data they are used to represent. Vaxine provides off the shelf rich data-types tailored for specific use cases, like collaborative text edition and other productivity tools.

<figure class="figure mt-2">
  <a href="../_assets/images/reference/user-intent.gif"
      class="no-visual">
    <img src="../_assets/images/reference/user-intent.gif"
        class="figure-img img-fluid"
    />
  </a>
  <figcaption class="figure-caption text-end">
    Illustration of a user-intent preservation anomaly.
  </figcaption>
</figure>

## Cumulative effects

**Problem**:<br />Cumulating the effects of individual operation might result in amplification of the intended changes. This is a particular case of failing to preserve user intention.

**Example**:<br />two users modify some object’s property concurrently, for example enlarging a shape, and the semantics of the merge is to sum the deltas. The shape size will end larger than each user intended. When trying to correct the issue, the reverse situation might happen again.

**Mitigation**:<br />Cumulative effects are acceptable when latency is low enough that users perceive that there are concurrent modifications to the same object.

**Solution**:<br />Prevent multiple users from modifying the same property of an object concurrently.

## Flickering

**Problem**:<br />Objects a user is working on are modified by other users concurrently. Changes are applied locally immediately, but some of them might conflict with changes made by other users. The issues arises when one of the client applies unconfirmed changes that will be later undone by conflict arbitration.

**Example**:<br />Two users set the color of the same object concurrently. They apply the change immediately in the local state and send the update to the server. A server receives the first update and propagates it to all all users. The clients receiving the update from the server apply it, possibly overwriting the color set locally. When the second operation arrives at the server, conflict resolution might chose this operation as the winner and propagate it to clients. This will result in flickering effect for the users that already applied the first update.

<figure class="figure mt-2">
  <a href="../_assets/images/reference/flickering.gif"
      class="no-visual">
    <img src="../_assets/images/reference/flickering.gif"
        class="figure-img img-fluid"
    />
  </a>
  <figcaption class="figure-caption text-end">
    Illustration of a flickering anomaly.
  </figcaption>
</figure>

**Example**:<br />Consider a collaborative kanban board application. Two users move the same card to different columns. While the system does not arbitrate the winning move, users might temporarily see the card in the position picked by the other user, before the system decides that the winning position is the other one, and move the card again.

**Mitigation**:<br />Delay applying conflicting operations until the server arbitrates the result.

**Solution**:<br />Vaxine doesn’t use a central authority to arbitrate conflict-resolution, instead every server is able to merge incoming operation in local state deterministically. CRDTs provide decentralised conflict resolution rules that ensure that the result of merging conflicting updates is independent of the order in which conflicting operations are applied (given that they respect causality in the case of operation-based CRDTs).

## Referential Integrity

**Problem**:<br />Concurrent operation might inadvertently destroy the target in one side of a relationship and the inconsistency is not detected until the element is dereferenced.

**Example**:<br />In a Kanban board, a user moves a card into a column and concurrently another user removes that column. After merging the changes, the card that was moved has no assigned column and can possibly be lost.

<figure class="figure mt-2">
  <a href="../_assets/images/reference/referential-integrity-cards.gif"
      class="no-visual">
    <img src="../_assets/images/reference/referential-integrity-cards.gif"
        class="figure-img img-fluid"
    />
  </a>
  <figcaption class="figure-caption text-end">
    Illustration of a referential integrity anomaly.
  </figcaption>
</figure>

**Example**:<br />In a role playing game ("RPG"), a user equips an item and concurrently sells it. The user may receive the proceeds of the sale while the item remains equipped but no longer exists in the inventory. It is possible to do an integrity check to unequip the item, but the developer has to provide the code for that.

<figure class="figure mt-2">
  <a href="../_assets/images/reference/referential-integrity-sale.gif"
      class="no-visual">
    <img src="../_assets/images/reference/referential-integrity-sale.gif"
        class="figure-img img-fluid"
    />
  </a>
  <figcaption class="figure-caption text-end">
    Illustration of a referential integrity anomaly.
  </figcaption>
</figure>

**Mitigation**:<br />Use a stronger consistency model that ensures sequential operation execution.

**Solution**:<br />Vaxine prevents referential integrity violation through reservations and/or compensations.

## Ownership clash

**Problem**:<br />Concurrent operations set the value of a field. Arbitration picks a single winning value, but during the fork different replicas allow operations based on the different values set.

**Example**:<br />two concurrent transactions set different owners for an entity. Divergence is only reconciled asynchronously, meanwhile the (two) owner(s) of the entity executed operations that should only be allowed by a single owner.

**Mitigation**:<br />coordinate concurrent operation execution for each record/attribute.

**Solution**:<br />Use Rich-CRDT to enforce uniqueness constraint efficiently.

## Cursor ghost movements

**Problem**:<br />The edition cursor position might change when incoming updates are applied and no longer match the input device position on screen.

**Example**:<br />A user select an entry to edit in a todo list by clicking on it. Meanwhile, another user adds a new entry to the list. When the new entry is added to the todo list of the first user, the element he is editing will change position, while the mouse pointer will remain in the same position he clicked.

**Example**:<br />A user wants to edit some paragraph in a text block and positions the screen on the beginning of that paragraph. Another user is adding text before that paragraph making the paragraph the first user focused to move down in the screen.

**Mitigation**:<br />Track the cursor position using hints from the context and reposition the screen when new changes affect the focus of the user

**Solution**:<br />Tracking cursor movements is a difficult problem because it crosses the boundaries of two layers, visualisation and data management. Mitigation is the only solution we are aware of.

## Long-lived actions

**Problem**:<br />Operations that require multiple user interactions might see their changes being altered by concurrent operations that were execute during the duration of the whole operation.

**Example**:<br />User executes a copy-paste operation but takes a few seconds between copying the elements and pasting them in another location. Some user deletes one of the copied element concurrently. The first user finally pastes the copied elements, but the copied elements no longer include the deleted element. Conversely, if the element is re-created it might also be confusing for the user that deleted it.

**Mitigation**:<br />Handle copy-and-paste in a transaction and abort one of the conflicting operations (i.e. the entire copy-and-paste or the conflicting update to the copy set)

**Solution**:<br />Vaxine allows the developer to select the conflict-resolution policy that suits the situation best, either prioritising the recreation of the concurrently-removed elements (add-wins), or ensuring that they are deleted (remove-wins).

## Duplication of elements

**Problem**:<br />Concurrent operations that modify the relations of some element result in the duplication of that element.

**Example**:<br />Two cut-and-paste operations of the same element result in two copies of that element, violating uniqueness.

**Example**:<br />Concurrent reparenting an element in a graph results in duplication of that element.

**Mitigation**:<br />Use locking to prevent concurrent operations on a single element.

**Solution**:<br />Rich CRDTs provide uniqueness invariant through the use of reservations, allowing operations that are safe to execute concurrently over a single object.

## No cycles

**Problem**:<br />moving elements in a tree might result in cycles.

**Example**:<br />A node A is moved under B, while B is moved under A, creating a cycle. The same problem can happen with a larger tree.

**Example**:<br />In a structured collaboration application, one container element might be moved inside another container and vice versa, concurrently.

**Mitigation**:<br />Lock the root of the tree to prevent concurrent moves.

**Solution**:<br />Use reservations to allow moves within subtrees without blocking operations on other parts of the tree. Requires a cycle-detection mechanism to issue reservations safely.

## Offline interaction

**Problem**:<br />working on a document offline for long periods might result in divergence difficult to reconcile.

**Example**:<br />Users use a spreadsheet to collect data points in one sheet and calculate summary info in another sheet. The template of the datapoints might be modified, affecting the columns that should be used to make the calculations. Meanwhile, the user working offline on the summary sheet might add new calculations referring to the old column positions.

**Solution**:<br />Minimize divergence by synching replicas frequently; give feedback about the status of the connection.

## Proximity bias

**Problem**:<br />The arbitration of concurrent operations over a competing resource favours clients that are nearer a server.

**Example**:<br />Two players in a competitive game try to collect the same item in the scenario. The user that is closer to the server systematically succeeds collecting the item.

**Mitigation**:<br />don’t use timestamp-based conflict-resolution strategies.

**Solution**:<br />Use Rich-CRDTs that introduce pseudo-randomness in the presence of conflicts. E.g. a reservations system that manages reservations across multiple regions; a CRDT with pseudo-random conflict arbitration.

## Conflict-resolution consistency

**Problem**:<br />Correctness of application depends on having effects of operations being applied atomically in a transaction. Conflict arbitration might break the implicit invariants enforced by a transaction.

**Example**:<br />consider a numerical equality, X = Y. A transaction sets the two values, for instance TxA: X = 1, Y = 1 and TxB: X = 2, Y = 2. The conflict resolution strategy for X and Y are set differently, suppose that one picks the maximum, while the other picks the minimum value. Being unable to ensure coherence between the conflict resolutions strategies applied to X and Y will break the invariant X = Y.

**Mitigation**:<br />can't ensure this invariant without atomic operations.

**Solution**:<br />Use transactions and carefully consider conflict resolution rules; declare invariant and let the system figure out the appropriate conflict-resolution rules to be applied for each value.

## Database anomalies

### Phantom reads

**Problem**:<br />Non-serialisable database anomaly where one transaction computes a collection of records, while another concurrently adds/removes a record to that collection and commits. A re-read of the computed collection in the same transaction will result in a different result.

**Example**:<br />A user counts all comments per user on a post and another user adds a new comment. If the first user tries to retrieve the comments for that user, the result set won’t match the size returned in the first read.

**Solution**:<br />Range locks, or similarly with reservations.

### Stickiness

**Problem**:<br />Reconnecting to a different server does not reflect last changes executed in the session.

**Example**:<br />A user makes some comments on a video-editing platform; The server where he is connected fails and the connection is handed over to a different server. After refreshing the page, the user doesn’t see his comments.

**Solution**:<br />Ensure selected server for failover includes latest client writes, or cache unstable operations on the client.

### Write-skew (short fork)

**Problem**:<br />Two transactions don’t have write-write conflict but are not serialisable.

**Example**:<br />T1 reads something modified by T2 and T2 reads something modified by T1, but the transactions don&#39;t conflict

**Solution**:<br />Issue is possible under SI and TCC+.

### Long fork

**Problem**:<br />Two concurrent transactions writing to different objects commit, subsequent transactions might see the effects of one of the transaction, but not the other.

**Example**:<br />See problem definition.
