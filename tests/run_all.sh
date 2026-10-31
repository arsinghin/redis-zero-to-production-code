#!/usr/bin/env bash
# Run every example in the book against a throwaway local Redis.
#
#   bash tests/run_all.sh
#
# Needs: redis-server + redis-cli (Redis 8.x) on PATH, Python 3.10+ with
# `pip install -r requirements.txt`, and Node 18+ with `npm install` (for node/).
# Uses ports 6379, 6385, 6401-6402, 6411-6413, 26411-26413 and 7001-7006.
# It refuses to run if something is already listening on 6379.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
PASS=0; FAIL=0; FAILED=""

if redis-cli -p 6379 ping >/dev/null 2>&1; then
  echo "Something is already running on port 6379. Stop it first (these tests write data)."
  exit 1
fi
echo "Using: $(redis-server --version | cut -d' ' -f1-3)"

TMP=$(mktemp -d)
redis-server --port 6379 --daemonize yes --save "" --appendonly no \
  --dir "$TMP" --logfile "$TMP/redis.log"
for _ in $(seq 1 50); do redis-cli -p 6379 ping >/dev/null 2>&1 && break; sleep 0.1; done

run() {   # run <name> <command...>
  local name=$1; shift
  if "$@" > "$TMP/$name.out" 2>&1; then
    PASS=$((PASS + 1)); echo "PASS  $name"
  else
    FAIL=$((FAIL + 1)); FAILED="$FAILED $name"; echo "FAIL  $name"; tail -5 "$TMP/$name.out"
  fi
}

cd "$ROOT/python"
for f in part1_basics part2_structures part3_caching part4_concurrency \
         part5_messaging part6_memory part8_operations part9_ai part10_order_flow; do
  run "$f" python "$f.py"
done
if [ -d "$ROOT/node/node_modules" ] || [ -n "${NODE_PATH:-}" ]; then
  run node_part1_basics node "$ROOT/node/part1_basics.js"
else
  echo "SKIP  node_part1_basics (run 'npm install' in code/node first)"
fi
redis-cli -p 6379 shutdown nosave >/dev/null 2>&1

run part6_power_cut   bash "$HERE/part6_power_cut.sh"
run part7_replication bash "$HERE/part7_replication.sh"
run part7_sentinel    bash "$HERE/part7_sentinel.sh"
run part7_cluster     bash "$HERE/part7_cluster.sh"

rm -rf "$TMP"
echo
echo "passed: $PASS  failed: $FAIL${FAILED:+  ->$FAILED}"
[ "$FAIL" -eq 0 ]
