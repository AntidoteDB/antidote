#!/usr/bin/env bash

tarAll () {
  local own_node_name="${HOSTNAME::-12}" # remove the .grid5000.fr part of the name
    local datafolder="~/antidote/_build/default/rel/antidote/data/"
    pushd "${datafolder}" > /dev/null 2>&1
    local result_f_name="node-${own_node_name}-StalenessResults.tar"
    tar czf /root/"${result_f_name}" "${datafolder}"
    popd > /dev/null 2>&1
}

tarAll
