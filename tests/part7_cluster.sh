#!/usr/bin/env bash
# Part 7c: a 6-node Redis Cluster (3 primaries + 3 replicas) on one machine.
# Needs redis-server, redis-cli and python (with redis-py) on PATH.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
DIR=$(mktemp -d)
PORTS="7001 7002 7003 7004 7005 7006"

for port in $PORTS; do
  redis-server --port $port --dir "$DIR" --cluster-enabled yes \
    --cluster-config-file "nodes-$port.conf" --cluster-node-timeout 2000 \
    --dbfilename "dump-$port.rdb" --save "" --appendonly no \
    --daemonize yes --logfile "$DIR/$port.log"
done
sleep 0.5

echo "=== create the cluster ==="
redis-cli --cluster create $(for p in $PORTS; do printf "127.0.0.1:%s " $p; done) \
  --cluster-replicas 1 --cluster-yes 2>&1 | grep -E "Master\[" | sed 's/^.*Master/Master/' 
for _ in $(seq 1 50); do
  redis-cli -p 7001 cluster info | grep -q "cluster_state:ok" && break; sleep 0.2
done
# Wait until every replica has finished its first sync with its primary.
# (A replica that never synced will refuse to take over.)
for port in $PORTS; do
  for _ in $(seq 1 100); do
    info=$(redis-cli -p $port info replication | tr -d '\r')
    echo "$info" | grep -q "role:master" && break
    echo "$info" | grep -q "master_link_status:up" && break
    sleep 0.2
  done
done

echo "=== which slots does each primary own? ==="
for port in 7001 7002 7003; do
  echo "127.0.0.1:$port -> slots $(redis-cli -p 7001 cluster nodes | awk -v p=":$port@" '$2 ~ p && $3 ~ /master/ {print $9}')"
done

echo "=== where does a key live? ==="
echo "CLUSTER KEYSLOT pizza:1 -> $(redis-cli -p 7001 CLUSTER KEYSLOT pizza:1)"
echo "CLUSTER KEYSLOT pizza:3 -> $(redis-cli -p 7001 CLUSTER KEYSLOT pizza:3)"

echo "=== without -c, a node tells you to go elsewhere ==="
for port in 7001 7002 7003; do redis-cli -p $port SET pizza:1 "Margherita" 2>&1; done | grep -v "^OK" | head -1

python "$HERE/../python/part7_cluster_client.py"

echo "=== kill a primary: the cluster fails over by itself (no Sentinel) ==="
SLOT=$(redis-cli -p 7001 CLUSTER KEYSLOT pizza:1)
OWNER=$(redis-cli -p 7001 cluster nodes | awk -v slot=$SLOT '$3 ~ /master/ {
  for (i = 9; i <= NF; i++) { split($i, r, "-"); if (slot >= r[1] && slot <= (r[2] == "" ? r[1] : r[2])) { split($2, a, "[:@]"); print a[2] } } }')
echo "pizza:1 lives on primary $OWNER; killing it"
kill -9 "$(redis-cli -p $OWNER info server | awk -F: '/^process_id/ {print $2}' | tr -d '\r')"
SURVIVOR=7001; [ "$OWNER" = "7001" ] && SURVIVOR=7002
for i in $(seq 1 60); do
  sleep 0.5
  [ $i -eq 1 ] && echo "right after the kill: $(redis-cli -c -p $SURVIVOR GET pizza:1 2>&1 | head -1)"
  redis-cli -p $SURVIVOR cluster info | grep -q "cluster_state:ok" && \
    [ "$(redis-cli -c -p $SURVIVOR GET pizza:1 2>/dev/null)" = "Margherita" ] && \
    { echo "cluster healthy again after ~$((i / 2)) s; pizza:1 = $(redis-cli -c -p $SURVIVOR GET pizza:1)"; break; }
done

for port in $PORTS; do redis-cli -p $port shutdown nosave >/dev/null 2>&1; done
rm -rf "$DIR"
