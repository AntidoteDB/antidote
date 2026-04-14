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

ANTIDOTE=./_build/default/rel/antidote/bin/antidote
LOG_DIR=antidote@127.0.0.1/logger_logs

dump_diagnostics() {
    rc=$?
    echo "===== dump_diagnostics (exit rc=$rc) ====="
    echo "----- log dir listing -----"
    ls -la "$LOG_DIR" 2>/dev/null || echo "(no log dir)"
    for f in "$LOG_DIR"/info.log "$LOG_DIR"/errors.log; do
        if [ -f "$f" ]; then
            echo "----- $f -----"
            cat "$f"
        fi
    done
    echo "----- erl_crash.dump(s) -----"
    for d in $(find . -name "erl_crash.dump" 2>/dev/null); do
        echo "~~~~~ $d (first 500 lines) ~~~~~"
        head -500 "$d"
    done
    echo "----- surviving antidote processes -----"
    ps -ef | grep -E "beam|antidote|run_erl|heart" | grep -v grep || true
    return $rc
}
trap dump_diagnostics EXIT

# Start Antidote
$ANTIDOTE daemon

# Execute test transaction
./test/release_test.escript

echo "===== Pre-stop snapshot ====="
$ANTIDOTE eval 'application:which_applications().' || true
$ANTIDOTE eval 'catch supervisor:which_children(antidote_sup).' || true
$ANTIDOTE eval 'catch supervisor:which_children(riak_core_sup).' || true
$ANTIDOTE eval 'catch supervisor:which_children(ranch_sup).' || true
$ANTIDOTE eval 'length(erlang:processes()).' || true

echo "===== Bumping runtime log level so shutdown is verbose ====="
$ANTIDOTE eval 'logger:set_primary_config(level, all), [logger:set_handler_config(H, level, all) || {H, _, _} <- logger:get_handler_config()], logger:get_primary_config().' || true

# Best-effort: snapshot who is alive right before stop so we can diff
# against "who is still alive after stop timed out".
echo "===== Pre-stop process sample (registered only) ====="
$ANTIDOTE eval '[{N, P, element(2, erlang:process_info(P, current_function)), element(2, erlang:process_info(P, message_queue_len))} || N <- erlang:registered(), P <- [whereis(N)], P =/= undefined].' || true

echo "===== Running antidote stop (90s wall-clock cap) ====="
set +e
timeout --preserve-status 90 $ANTIDOTE stop
STOP_RC=$?
set -e
echo "antidote stop returned rc=$STOP_RC"

# Find any surviving BEAM and force a crash dump for post-mortem.
BEAM_PID=$(pgrep -f "beam.*antidote@127.0.0.1" | head -1 || true)
if [ -n "$BEAM_PID" ] && kill -0 "$BEAM_PID" 2>/dev/null; then
    echo "===== BEAM $BEAM_PID still alive after stop; requesting erl_crash.dump via SIGUSR1 ====="
    kill -USR1 "$BEAM_PID" || true
    # Give BEAM up to 15s to write the dump and exit on its own.
    for i in $(seq 1 15); do
        kill -0 "$BEAM_PID" 2>/dev/null || break
        sleep 1
    done
    if kill -0 "$BEAM_PID" 2>/dev/null; then
        echo "BEAM did not exit after USR1; SIGKILL"
        kill -9 "$BEAM_PID" || true
    fi
fi
pkill -9 -f "rel/antidote" 2>/dev/null || true

if [ $STOP_RC -ne 0 ]; then
    echo "stop failed / timed out (rc=$STOP_RC)"
    exit $STOP_RC
fi
