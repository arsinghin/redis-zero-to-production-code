"""Part 8: running it in production. ACLs, SCAN vs KEYS, UNLINK,
the slow log, hit ratio, memory per key, and pipelining."""
import time

import redis

from common import REDIS_URL, get_client, reset, title

r = get_client()
reset(r, "pizza:*", "big:*", "bulk:*", "hit:*")

# ------------------------------------------------------------------ 1. ACLs
title("ACL: a user that can only READ pizza keys")
r.acl_setuser("menu_reader", enabled=True, passwords=["+change-me-123"],
              keys=["pizza:*"], commands=["+get", "+hget", "+hgetall", "+ping"])
r.hset("pizza:1", mapping={"name": "Margherita", "price": 12})
r.set("orders:secret", "do not read")

reader = redis.Redis.from_url(REDIS_URL, username="menu_reader",
                              password="change-me-123", decode_responses=True)
print("read pizza:1 ->", reader.hgetall("pizza:1"))
for label, call in [("write pizza:1", lambda: reader.hset("pizza:1", "price", 1)),
                    ("read orders:secret", lambda: reader.get("orders:secret")),
                    ("FLUSHALL", lambda: reader.flushall())]:
    try:
        call()
    except redis.exceptions.NoPermissionError as e:
        print(f"{label} -> NOPERM:", str(e).split(" to run")[0][:70])
r.acl_deluser("menu_reader")
r.delete("orders:secret")

# ------------------------------------------------------- 2. KEYS vs SCAN
title("KEYS blocks everyone; SCAN works in small steps")
pipe = r.pipeline(transaction=False)
for i in range(300_000):
    pipe.set(f"bulk:{i}", i)
pipe.execute()
r.slowlog_reset()

start = time.perf_counter()
n = len(r.keys("bulk:*"))
print(f"KEYS bulk:*  -> {n} keys in ONE call ({(time.perf_counter() - start) * 1000:.0f} ms "
      "including the transfer)")

start = time.perf_counter()
calls, found, cursor = 0, 0, 0
while True:
    cursor, batch = r.scan(cursor=cursor, match="bulk:*", count=1000)
    calls += 1
    found += len(batch)
    if cursor == 0:
        break
print(f"SCAN         -> {found} keys in {calls} small calls, "
      f"{(time.perf_counter() - start) * 1000:.0f} ms total")

# ------------------------------------------------------- 3. the slow log
title("SLOWLOG: what held the server up for more than 10 ms?")
for entry in r.slowlog_get(10):
    cmd = entry["command"]
    cmd = cmd.decode() if isinstance(cmd, bytes) else cmd
    print(f"  {entry['duration'] / 1000:.1f} ms  {cmd[:40]}")
print("  (no SCAN call is listed: each one was too short to be logged)")

# ------------------------------------------------------- 4. DEL vs UNLINK
title("Deleting a big key: DEL blocks, UNLINK frees memory in the background")
for name in ("big:del", "big:unlink"):
    pipe = r.pipeline(transaction=False)
    for chunk in range(0, 1_000_000, 10_000):
        pipe.sadd(name, *range(chunk, chunk + 10_000))
    pipe.execute()
print("each set holds", r.scard("big:del"), "members,",
      f"{r.memory_usage('big:del') / 1_048_576:.0f} MB")
for cmd in ("delete", "unlink"):
    key = "big:del" if cmd == "delete" else "big:unlink"
    start = time.perf_counter()
    getattr(r, cmd)(key)
    print(f"  {cmd.upper():6} took {(time.perf_counter() - start) * 1000:.1f} ms")

# ------------------------------------------------------- 5. hit ratio
title("Cache hit ratio from INFO stats")
r.config_resetstat()
r.set("hit:menu", "cached")
for i in range(100):
    r.get("hit:menu" if i % 10 else "hit:missing")    # 90 hits, 10 misses
stats = r.info("stats")
hits, misses = stats["keyspace_hits"], stats["keyspace_misses"]
print(f"hits={hits} misses={misses} hit ratio={hits / (hits + misses):.0%}")

# ------------------------------------------------------- 6. memory per key
title("MEMORY USAGE: what does one key cost?")
r.set("pizza:str", "Margherita")
r.hset("pizza:hash", mapping={"name": "Margherita", "price": 12, "prep_time": 10})
for key in ("pizza:str", "pizza:hash"):
    print(f"  {key:11} -> {r.memory_usage(key)} bytes")
mem = r.info("memory")
print("  used_memory_human:", mem["used_memory_human"],
      "| fragmentation ratio:", mem["mem_fragmentation_ratio"])

# ------------------------------------------------------- 7. pipelining
title("Pipelining: 10,000 SETs")
reset(r, "bulk:*")
start = time.perf_counter()
for i in range(10_000):
    r.set(f"bulk:{i}", i)
one_by_one = time.perf_counter() - start
start = time.perf_counter()
pipe = r.pipeline(transaction=False)
for i in range(10_000):
    pipe.set(f"bulk:{i}", i)
pipe.execute()
piped = time.perf_counter() - start
print(f"  one at a time: {one_by_one * 1000:.0f} ms | pipelined: {piped * 1000:.0f} ms "
      f"| {one_by_one / piped:.0f}x faster")
print("  (on localhost the round trip is tiny; over a real network the gap is far bigger)")
reset(r, "bulk:*", "pizza:*", "hit:*")
