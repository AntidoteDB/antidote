#!/bin/bash

export RELX_REPLACE_OS_VARS=true
for i in `seq 1 $1`;
do
   mkdir  ./_build/default/rel/antidote${i}/etc/
   cp ./config/vm.args ./_build/default/rel/antidote${i}/etc/
done
