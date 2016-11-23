#!/bin/bash

export RELX_REPLACE_OS_VARS=true
i=$1
    echo "stopping wombat..."
    ../wombat-2.0.0-rc1/stop.sh
    echo "starting wombat..."
    ../wombat-2.0.0-rc1/start.sh
