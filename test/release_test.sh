#!/bin/bash
# This builds a release, starts it and tries to do simple transaction; exits immediately upon error
set -e

# cd to root project directory
SCRIPTDIR=`dirname $0`
cd "$SCRIPTDIR/.."

# Start Antidote
./_build/default/rel/antidote/bin/env start

# Execute test transaction
./test/release_test.escript

# Stop Antidote
./_build/default/rel/antidote/bin/env stop
