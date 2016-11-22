#!/bin/bash

export RELX_REPLACE_OS_VARS=true
export node=antidote-1@mbp-alejandro.rsr.lip6.fr
for i in `seq 2 $1`;
do
    echo "command is ./_build/default/rel/antidote${i}/bin/antidote-admin cluster join 'antidote-1@${HOSTNAME}'"
    ./_build/default/rel/antidote${i}/bin/antidote-admin cluster join 'antidote-1@${HOSTNAME}'
    ./_build/default/rel/antidote${i}/bin/antidote-admin cluster plan
    ./_build/default/rel/antidote${i}/bin/antidote-admin cluster commit
  sleep 1
done
