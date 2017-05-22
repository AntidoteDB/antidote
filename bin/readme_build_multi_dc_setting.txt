1. create N releases of antidote

    ./bin/stop-nodes.sh N

2. start N instances of antidote in a local machine

    ./bin/build-releases.sh N

3. Optional:  join those instances into cluster(s)

    Example: N=4, creating two clusters of two nodes each.

    join_cluster_script.erl 'antidote1@127.0.0.1' 'antidote2@127.0.0.1'
    join_cluster_script.erl 'antidote3@127.0.0.1' 'antidote4@127.0.0.1'

4. Join the clusters (or single nodes in case you haven't clustered them into DCs)

    Use the $join_dcs_script, which takes as an argument ONE NODE FROM EACH DC.

    $join_dcs_script.erl 'antidote1@127.0.0.1' 'antidote3@127.0.0.1'

DONE!
NOTE: current maximum of N is 5.