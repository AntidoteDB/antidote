#!/bin/bash

NPROC=$(nproc)

if [[ ${NPROC} -ge 2 ]]; then
  trap "echo 'Shutting down' && /opt/antidote/bin/env stop" TERM
  /opt/antidote/bin/env start &

  echo -n "Waiting for Antidote logger..."
  while [ ! -f /opt/antidote/log/console.log ]
  do
	  sleep 1
  done
  echo "✔"

  tail -F /opt/antidote/log/console.log &
  wait ${!}

else
  echo "You need at least 2 cpu cores in your Docker (virtual) machine to run this image!"
  echo "You have ${NPROC} processors"
  exit 1
fi
