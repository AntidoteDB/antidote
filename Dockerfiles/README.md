# Build AntidoteDB locally using Docker

The Dockerfile contained in this directory is used to build local AntidoteDB images from the source code, mainly made for dev reasons.
If you want to use AntidoteDB in production or if you need a stable Docker image, please pull Antidote Docker images from [the official Docker Hub](https://cloud.docker.com/u/antidotedb/repository/docker/antidotedb/antidote), or use Dockerfiles from its [Github source repository](https://github.com/AntidoteDB/docker-antidote).

# Getting started

## Prerequisites

- Working recent version of Docker (1.12 and up recommended)

## Building the image locally

For building the Docker image on your local machine, use the following command (must be executed from the antidote.git root directory)
```
docker build -f Dockerfile -t antidotedb:local-build .
```

or simply 
```
make docker-build
```

Then you can run it using:
```
docker run -d --name antidote -p "8087:8087" antidotedb:local-build
```
or:
```
make docker-run
```

## Using the local node

Wait until Antidote is ready. The current log can be inspected with `docker logs antidote`. Wait until the log message `Application antidote started on node 'antidote@127.0.0.1'` appears.

Antidote should now be running on port 8087 on localhost.

## Connect to the console

You can connect to the console of a local node typing the following:
```
docker exec -it antidote /opt/antidote/bin/env attach
```
