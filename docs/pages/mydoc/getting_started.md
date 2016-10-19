---
title: Getting started with Antidote
keywords: sample homepage
tags: [getting_started]
toc: false
sidebar: mydoc_sidebar
permalink: getting_started.html
---

To experiment with AntidoteDB, we provide a ready-to-use environment as a Docker container.

# Prerequisite: Docker

Download and install the Docker software from [<http://www.docker.com>](<http://www.docker.com>) for your platform.
After it is installed, you can issue docker commands from a shell to run and manage containers.

The following command pulls the tutorial from the Docker Hub and spawns an Erlang shell:

    docker run -t -i cmeiklejohn/antidote-tutorial

You can interact with the datastore now on the shell.
In the following, we will do a tour together now.

# A bookstore app

## Storing and retrieving objects

Add user information for Michel

```
User1 = {michel, antidote_crdt_mvreg, user_bucket},

{ok, Time1} = antidote:update_objects(ignore, [],
[{User1, assign, {["Michel", "michel@blub.org"], client1}}]),

{ok, Result, Time2} = antidote:read_objects(ignore, [], [User1]),

Result.
```

{% include links.html %}
