"""Part 7b: a Sentinel-aware client keeps writing through a failover.

Started by tests/part7_sentinel.sh. It never hard-codes the primary's
address; it asks the Sentinels every time it (re)connects.
"""
import os
import time

import redis
from redis.sentinel import Sentinel

# "host:port,host:port,..." (the Docker demo sets this to the sentinel containers)
ADDRS = os.environ.get("SENTINELS", "127.0.0.1:26411,127.0.0.1:26412,127.0.0.1:26413")
sentinel = Sentinel([(h, int(p)) for h, p in (a.split(":") for a in ADDRS.split(","))],
                    socket_timeout=0.5)
primary = sentinel.master_for("slicedice", socket_timeout=0.5, decode_responses=True)
replica = sentinel.slave_for("slicedice", socket_timeout=0.5, decode_responses=True)

ok = failed = 0
first_fail = None
start = time.time()
while time.time() - start < float(os.environ.get("RUN_SECONDS", 20)):
    try:
        primary.incr("orders:today")
        ok += 1
        if first_fail is not None:
            print(f"  writes are back after {time.time() - first_fail:.1f} s "
                  f"(new primary: {sentinel.discover_master('slicedice')})")
            break
    except (redis.ConnectionError, redis.TimeoutError, redis.ReadOnlyError):
        failed += 1
        if first_fail is None:
            first_fail = time.time()
            print("  writes failing: the primary is gone")
    time.sleep(0.1)

print(f"  successful writes: {ok}, failed attempts during failover: {failed}")
print("  read from a replica:", replica.get("orders:today"))
