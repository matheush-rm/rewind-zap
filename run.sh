#!/bin/sh
# Entry point. The replay restarts itself after crashes/disconnects and picks
# up exactly where it stopped. Exit 3 = error that needs a human (including the
# circuit breaker tripping after 10 consecutive errors): no retry.
set -u
cmd=${1:-}
[ $# -gt 0 ] && shift
case "$cmd" in
  export)  exec node src/export.js "$@" ;;
  preview) exec node src/preview.js "$@" ;;
  status)  exec node src/status.js "$@" ;;
  replay)
    # a human (re)starting the replay resets the circuit breaker;
    # automatic restarts below keep counting
    rm -f "${DATA_DIR:-data}/breaker.json"
    while :; do
      node src/replay.js "$@"
      c=$?
      case $c in 0|3|130) exit $c ;; esac
      echo ">>> replay saiu com código $c; reiniciando em 10s (retoma de onde parou). Ctrl+C para parar."
      sleep 10 || exit 130
    done ;;
  *) echo "uso: run.sh export | preview [--skipped] | status | replay [--max N]"; exit 1 ;;
esac
