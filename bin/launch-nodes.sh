#!/bin/bash

export RELX_REPLACE_OS_VARS=true
for i in `seq 1 $1`;
do
  PLATFORM_DATA_DIR="data/${i}" RING_STATE_DIR="data/ring/${i}" HANDOFF_PORT=8${i}99 PB_PORT=8${i}87 PUBSUB_PORT=8${i}86 LOGREADER_PORT=8${i}85 NODE_NAME=antidote${i} PB_IP=127.0.0.1 COOKIE=antidote _build/default/rel/antidote${i}/bin/antidote${i} foreground &
done
