# antidote-docker-alpine | Getting started
[![](https://images.microbadger.com/badges/image/itoumlilt/antidote-alpine:0.1.svg)](https://microbadger.com/images/itoumlilt/antidote-alpine:0.1 )
[![](https://images.microbadger.com/badges/version/itoumlilt/antidote-alpine:0.1.svg)](https://microbadger.com/images/itoumlilt/antidote-alpine:0.1 )

## Prerequisites

- Working recent version of [Docker][docker]. See installation guide on [Docker website][docker_install].

## Overview

This repository contains a [Docker][docker] file for a tiny container
image of [AntidoteDB][antidote] based on Alpine Linux.

Image on Docker Hub: https://hub.docker.com/r/antidotedb/antidote/tags/alpine/

Note: The image on the `iptables` branch includes iptables, which can be useful to
create network partitions for testing AntidoteDB.

## Running a local node

There are two possible ways to run the Antidote Alpine Docker node, detailed bellow. The simplest and recommended way is to pull the official binary image from [Antidote Docker Hub][antidote-docker-hub] , but you can also build your own image from the Dockerfile sources.

### Option 1: pulling the image from Docker hub (recommended) 

Start a local Antidote-alpine node with the command:
```bash
docker run --rm -it -p "8087:8087" antidotedb/antidote:alpine
```
The previous command should fetch the Antidote Alpine image automatically. 
For updating to the latest version use the command :
```bash
docker pull antidotedb/antidote:alpine
```

Wait until Antidote is ready. The current log can be inspected with `docker logs antidote`. Wait until the log message `Application antidote started on node 'antidote@127.0.0.1'` appears.

Antidote should now be running on port 8087 on localhost.

### Option 2: building the image locally

From Antidote sources (clone your copy from https://github.com/AntidoteDB/antidote), use the following command to build the Docker image on your local machine:
```bash
docker build -f Dockerfiles/alpine/Dockerfile -t antidotedb/antidote:alpine Dockerfiles/alpine
```

[antidote]: https://www.antidotedb.eu/
[docker]: http://docker.io
[docker_install]: https://docs.docker.com/install/
[antidote-docker-hub]: https://hub.docker.com/r/antidotedb/antidote/tags/alpine/
