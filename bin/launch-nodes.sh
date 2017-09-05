#!/bin/bash

export RELX_REPLACE_OS_VARS=true

for i in `seq 1 10`;
do
  PLATFORM_DATA_DIR="data/${i}" RING_STATE_DIR="data/ring/${i}" HANDOFF_PORT=8${i}99 PB_PORT=8${i}87 PUBSUB_PORT=8${i}86 LOGREADER_PORT=8${i}85 METRICS_PORT=300${i} NODE_NAME=antidote-${i} _build/default/rel/antidote/bin/antidote foreground &
  sleep 1
done
