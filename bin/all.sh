#!/bin/bash

export RELX_REPLACE_OS_VARS=true
i=$1
#    ./bin/start_wombat.sh
   echo "stopping Antidote nodes..."
   ./bin/stop-nodes.sh ${i}
      echo "cleanning old Antidote releases and building new ones..."
   ./bin/build-releases.sh ${i}
   echo "launching Antidote nodes..."
   ./bin/launch-nodes.sh ${i}
      sleep $(($i*3))
   echo "joining Antidote nodes into a cluster"
   ./bin/join_cluster_script.erl ${i} 127.0.0.1