#!/bin/bash

export RELX_REPLACE_OS_VARS=true

for i in `seq 1 10`;
do
  HANDOFF_PORT=8${i}99 PB_PORT=8${i}87 PUBSUB_PORT=8${i}86 LOGREADER_PORT=8${i}85 NODE_NAME=antidote-${i} _build/default/rel/antidote/bin/antidote foreground &
  sleep 1
done
