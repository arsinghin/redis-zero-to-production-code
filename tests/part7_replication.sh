#!/usr/bin/env bash
# Part 7a: one primary, one replica. Needs redis-server/redis-cli on PATH.
set -u
DIR=$(mktemp -d)
P=6401; R=6402
for port in $P $R; do
  redis-server --port $port --dir "$DIR" --dbfilename "dump-$port.rdb" --save "" \
    --daemonize yes --logfile "$DIR/$port.log"
done
sleep 0.5

echo "=== make $R a replica of $P ==="
redis-cli -p $R REPLICAOF 127.0.0.1 $P
for _ in $(seq 1 50); do
  redis-cli -p $R info replication | grep -q "master_link_status:up" && break; sleep 0.1
done

echo "=== write on the primary, read on the replica ==="
redis-cli -p $P SET menu:special "Truffle Mushroom - \$18" >/dev/null
sleep 0.2
redis-cli -p $R GET menu:special

echo "=== try to write on the replica ==="
redis-cli -p $R SET menu:special "Free pizza for everyone"

echo "=== WAIT: block until 1 replica has the write (max 100 ms) ==="
redis-cli -p $P SET menu:price:pepperoni 15 >/dev/null
redis-cli -p $P WAIT 1 100

echo "=== replication info on the primary ==="
redis-cli -p $P info replication | grep -E "^(role|connected_slaves|slave0)" | tr -d '\r'

for port in $P $R; do redis-cli -p $port shutdown nosave >/dev/null; done
rm -rf "$DIR"
