#!/bin/bash
# This builds a release, starts it and tries to do simple transaction; exits immediately upon error
set -e

# set test node name
export NODE_NAME=antidote@127.0.0.1
export ROOT_DIR_PREFIX=antidote@127.0.0.1/
export COOKIE=testcookie

# cd to root project directory
SCRIPTDIR=`dirname $0`
cd "$SCRIPTDIR/.."

# Start Antidote
./_build/default/rel/antidote/bin/antidote daemon

# Execute test transaction
./test/release_test.escript

# Stop Antidote
./_build/default/rel/antidote/bin/antidote stop
