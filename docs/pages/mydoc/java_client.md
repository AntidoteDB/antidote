---
title: Antidote Client
last_updated: May 2, 2017
tags: [quick_start]
sidebar: mydoc_sidebar
permalink: java_client.html
toc: true
---

The tutorial serves as a guide for different client interfaces of Antidote. As a running example, we use a collaborative editing application where multiple users can concurrently update a shared todo list. We discuss how Antidote handles conflicting updates by multiple users based on its CRDT data model.

## Requirements

* Build AntidoteDB using docker
* Clone the application from github

[Todolist application](https://github.com/shraddhabarke/antidote-todolist)

In this tutorial, you will learn the following concepts:

 * Model data of Java application
 * Update and read CRDT objects in AntidoteDB
 * Run two AntidoteDB nodes concurrently
 * Test the behaviour of CRDT objects under concurrent access

## Data Model

The collaborative todo list application consists of a board with multiple columns. Each column consists of a list of tasks.

As per the [application requirements](https://github.com/shraddhabarke/antidote-todolist/blob/master/concurrent_cases.md), the update operations get preference over the delete operations. Consequently, the **data model adheres to add wins semantics.**

The **Board** object is an add wins map with a unique board id that returns relevant information about the board using the getboard method. Each **Board** object has a name and a list of column ids associated with it.

```erlang
Map[board_id -> Register[name]]
Map[board_id -> Set[column_id]]
```

Similar to the **Board**, the **Column** object is an add wins map with column id as the key. The unique column id returns fields related to a particular column such as name, tasks inside the column and board_id associated with the column. To give preference to the latest update, the column name and board id are of type last writer wins register. The tasks field is an add wins Set.

```erlang
Map[column_id -> Register[name]]
Map[column_id -> Set[tasks]]
Map[column_id -> Register[board_id]]
```

Each **Column** object has one or more **Task** objects. **Task** object is modelled as an add wins map with task id as the unique key. The fields of a **Task** object consists of title, due date and the column id associated with the task.

```erlang
Map[task_id -> Register[title]]
Map[task_id -> Register[due_date]]
Map[task_id -> Register[column_id]]
```

## Updating objects in Antidote:

In Antidote each object is stored in a Bucket. To create a bucket use the static bucket method:

```erlang
Bucket bucket = Bucket.bucket("mybucket");
```
Objects in the AntidoteDB are addressed using a Key. To execute the update operation on a bucket, ```Bucket.update``` method is called.
For performing several updates simultaneously the ```Bucket.updates``` methods can be used which is explained in the next section.

In the boardMap method, [MapKey](https://www.javadoc.io/doc/eu.antidotedb/antidote-java-client/0.1.0) (for the Map data type) stored under the key ```board_id.getId()``` is returned. MapKey is used to update the contents of the Map boardMap in the database as described in the next method.

```erlang
public MapKey boardMap(BoardId board_id) {
  return map_aw(board_id.getId());
}
```

The code below illustrates a method to create a board in the application and rename it. The update method takes a transactional context as its first argument. The createboard method returns a unique id that is passed as an argument for the renameBoard method. Since the name is stored in a register data type, assign method is called to update the value.

```erlang
public BoardId createBoard(AntidoteClient client, String name) {
  BoardId board_id = BoardId.generateId();
  MapKey board = boardMap(board_id);
  cbucket.update(client.noTransaction(), board.update(namefield.assign(name)));
  return board_id;
}
```

```erlang
public void renameBoard(AntidoteClient client, BoardId board_id , String newName) {
  MapKey board = boardMap(board_id);
  cbucket.update(client.noTransaction(), board.update(namefield.assign(newName)));
}
```

## Reading from Antidote

A Bucket has a read method that retrieves the current value of an object from database. MapReadResult presents the result of a read request on a Map CRDT. The entire object is read from database and individual fields can be obtained using get methods as illustrated by the following code:

```erlang
MapReadResult boardmap = cbucket.read(client.noTransaction(), board);
String boardname = boardmap.get(namefield);
List<ColumnId> columnid_list = boardmap.get(columnidfield);
```

## Connecting to AntidoteDB

We start two AntidoteDB node instances and run two separate application instances on top of each node. This enables us to see how Antidote tackles inconsistencies arising due to concurrent updates in different replicas.

Start two antidote node instances using the start_antidote script in setup folder.

```sh
./start_antidote.sh
```

In two separate terminals, start two application instances on the antidote nodes:

```sh
./app1.sh
./app2.sh
```

Now connect to antidote on each of the terminal:

```
connect antidote1 8087
connect antidote2 8087
```

We now have an AntidoteDB cluster with two replicas!

## Interacting with AntidoteDB

We simulate real world network partition between two replicas of AntidoteDB. Here, we mimic two users running the application in terminal 1 and terminal 2. Initially the two replicas are connected and Antidote replicates data across them. In case of a network failure, Antidote will resolve conflict as per add wins semantics after syncing again.

Create a new board with name "boardname" in terminal 1. A unique boardid is returned which is later used to perform operations on the board.

```erlang
createboard boardname
```

Now, run the disconnect script that disrupts the communication between the tow replicas. This is equivalent to two users performing operations concurrently.

```erlang
./disconnect.sh
```

Rename the board previously created, replacing board_id in the command with the id returned.

```erlang
renameboard board_id newnamet1
```

Rename the same board to a different name using terminal 2.

```erlang
renameboard board_id newnamet2
```

Run getboard command on both terminals, the outputs are as follows

Terminal 1:

```erlang
Board Name - newnamet1
List of Column Ids - []
List of Columns - []
```

Terminal 2:

```erlang
Board Name - newnamet2
List of Column Ids - []
List of Columns - []
```

Run the connect script to re-connect and sync up both the replicas!

```erlang
./connect.sh
```

The output of getboard command for both terminals is now:

```erlang
Board Name - newnamet2
List of Column Ids - []
List of Columns - []
```

The renameboard operation on terminal 2 was performed after the renameboard operation on terminal 1. Since the boardname is stored in a last writer wins register, the latter operation gets preference while resolving the conflict.

In the next section, we look at an interesting example of two users concurrently moving the same task to different columns.
