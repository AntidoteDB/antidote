#!/bin/bash
# This builds a release, starts it and tries to do simple transaction

set -e

SCRIPTDIR=`dirname $0`
pushd "$SCRIPTDIR/.." > /dev/null
ROOT=`pwd -P`
popd > /dev/null


cd $ROOT

# Start Antidote
./_build/default/rel/antidote/bin/env start

# Execute test transaction
./test/release_test.escript

# Stop Antidote
./_build/default/rel/antidote/bin/env stop