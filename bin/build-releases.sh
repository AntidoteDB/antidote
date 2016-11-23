#!/bin/bash

export RELX_REPLACE_OS_VARS=true
make relclean
for i in `seq 1 $1`;
do
   ./rebar3 release -n antidote${i}
done
