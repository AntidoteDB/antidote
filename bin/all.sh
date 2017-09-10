#! /bin/bash

# Stuff
export RELX_REPLACE_OS_VARS=true

# Cause
make relclean



# First create N releases of antidote
for i in `seq 1 $1`;
do
    ./rebar3 release -n antidote${i}
done
sleep 5
# Now launch em all
for i in `seq 1 $1`;
do
    PLATFORM_DATA_DIR="data/${i}" RING_STATE_DIR="data/ring/${i}" HANDOFF_PORT=8${i}99 PB_PORT=8${i}87 PUBSUB_PORT=8${i}86 LOGREADER_PORT=8${i}85 INSTANCE_NAME=antidote${i} PB_IP=127.0.0.1 COOKIE=antidote _build/default/rel/antidote${i}/bin/env foreground &
done
sleep 5
# Create two DCs with two nodes each
./bin/join_cluster_script.erl 'antidote1@127.0.0.1' 'antidote2@127.0.0.1'
./bin/join_cluster_script.erl 'antidote3@127.0.0.1' 'antidote4@127.0.0.1'
sleep 5
# Join the DCs
# One node from each DC
./bin/join_dcs_script.erl 'antidote1@127.0.0.1' 'antidote3@127.0.0.1'
