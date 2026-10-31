#!/usr/bin/env bash
# Part 7b: three Sentinels watching one primary + two replicas.
# We kill the primary and watch Sentinel promote a replica.
# Needs redis-server, redis-cli and python (with redis-py) on PATH.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
DIR=$(mktemp -d)
P=6411; R1=6412; R2=6413

for port in $P $R1 $R2; do
  redis-server --port $port --dir "$DIR" --dbfilename "dump-$port.rdb" --save "" \
    --daemonize yes --logfile "$DIR/$port.log"
done
sleep 0.5
redis-cli -p $R1 REPLICAOF 127.0.0.1 $P >/dev/null
redis-cli -p $R2 REPLICAOF 127.0.0.1 $P >/dev/null

for s in 26411 26412 26413; do
  cat > "$DIR/sentinel-$s.conf" <<EOF
port $s
daemonize yes
logfile "$DIR/sentinel-$s.log"
dir "$DIR"
sentinel monitor slicedice 127.0.0.1 $P 2
sentinel down-after-milliseconds slicedice 2000
sentinel failover-timeout slicedice 10000
sentinel parallel-syncs slicedice 1
EOF
  redis-server "$DIR/sentinel-$s.conf" --sentinel
done
sleep 3     # let the Sentinels find each other and the replicas

echo "=== Sentinel says the primary is ==="
redis-cli -p 26411 SENTINEL get-master-addr-by-name slicedice | paste -sd:
echo "replicas known: $(redis-cli -p 26411 SENTINEL replicas slicedice | grep -c '^name$')"
echo "sentinels known (others): $(redis-cli -p 26411 SENTINEL sentinels slicedice | grep -c '^name$')"

# The Python client asks Sentinel where the primary is, then keeps writing
# while we kill the primary.
python "$HERE/../python/part7_sentinel_client.py" &
CLIENT=$!
sleep 2
echo "=== pulling the plug on the primary ($P) ==="
kill -9 "$(redis-cli -p $P info server | awk -F: '/^process_id/ {print $2}' | tr -d '\r')"
wait $CLIENT

echo "=== Sentinel now says the primary is ==="
redis-cli -p 26411 SENTINEL get-master-addr-by-name slicedice | paste -sd:
grep -h "+switch-master" "$DIR"/sentinel-*.log | head -1 | sed 's/^.*+switch-master/+switch-master/'

for port in $R1 $R2 26411 26412 26413; do redis-cli -p $port shutdown nosave >/dev/null 2>&1; done
rm -rf "$DIR"
