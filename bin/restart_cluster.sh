#!/bin/bash

export RELX_REPLACE_OS_VARS=true
i=$1
   echo "stopping nodes..."
   ./bin/stop-nodes.sh ${i}
   echo "launching nodes..."
   ./bin/launch-nodes.sh ${i}
      sleep $(($i*3))
#    echo "creating arg files..."
#   ./bin/create-vm-file.sh ${i}
#   echo "joining into cluster..."
#   ./bin/join-nodes-into-cluster.sh ${i}
#  sleep 1
