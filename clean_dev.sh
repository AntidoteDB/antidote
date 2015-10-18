#!/bin/bash

# BenchCount=$1
# BenchParallel=$2
DevCount=6

echo Killing beam processes
pkill beam
for I in $(seq 1 $DevCount); do
    echo Cleaning: ~/rt/antidote/current/dev/dev"$I"/data/
    rm -rf ~/rt/antidote/current/dev/dev"$I"/data/*
done

