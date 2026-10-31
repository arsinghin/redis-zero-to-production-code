#!/usr/bin/env bash
# Part 6: pull the power cord (kill -9) and see what survives.
# Needs redis-server and redis-cli on PATH. Uses port 6385 and a temp folder.
set -u
PORT=6385
DIR=$(mktemp -d)
CLI="redis-cli -p $PORT"

start() {   # start Redis with the given persistence flags
  redis-server --port $PORT --dir "$DIR" --daemonize yes --logfile "$DIR/redis.log" "$@"
  for _ in $(seq 1 50); do $CLI ping >/dev/null 2>&1 && return; sleep 0.1; done
}
pull_the_plug() {   # kill -9: no chance to save anything on the way down
  kill -9 "$($CLI info server | awk -F: '/^process_id/ {print $2}' | tr -d '\r')"
  sleep 0.5
}
points() { $CLI dbsize; }

echo "=== 1. No persistence ==="
start --save "" --appendonly no
$CLI set points:brad 250 >/dev/null; $CLI set points:leo 300 >/dev/null
pull_the_plug; start --save "" --appendonly no
echo "keys after restart: $(points)"          # 0
$CLI shutdown nosave >/dev/null; rm -rf "$DIR"/*

echo "=== 2. RDB snapshot ==="
start --save "" --appendonly no --dbfilename dump.rdb
$CLI set points:brad 250 >/dev/null; $CLI set points:leo 300 >/dev/null
$CLI bgsave >/dev/null; sleep 1               # take the Polaroid
$CLI set points:tom 50 >/dev/null             # written AFTER the snapshot
pull_the_plug; start --save "" --appendonly no --dbfilename dump.rdb
echo "keys after restart: $(points) (tom's points were after the snapshot)"   # 2
$CLI shutdown nosave >/dev/null; rm -rf "$DIR"/*

echo "=== 3. AOF, fsync every second ==="
start --save "" --appendonly yes --appendfsync everysec
$CLI set points:brad 250 >/dev/null; $CLI set points:leo 300 >/dev/null; $CLI set points:tom 50 >/dev/null
sleep 1.5                                      # give the once-a-second fsync time to run
pull_the_plug; start --save "" --appendonly yes --appendfsync everysec
echo "keys after restart: $(points)"           # 3
echo "AOF folder:"; ls "$DIR/appendonlydir"
$CLI shutdown nosave >/dev/null
rm -rf "$DIR"
