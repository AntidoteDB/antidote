#!/bin/bash
set -e

if [[ ! -f /opt/antidote/releases/0.0.2/setup_ok ]]; then
  cd /opt/antidote/releases/0.0.2/
  cp vm.args vm.args_backup
  if [[ "$SHORT_NAME" = "true" ]]; then
    sed "s/-name /-sname /" vm.args_backup > vm.args
  fi
  touch setup_ok
fi

exec "$@"
