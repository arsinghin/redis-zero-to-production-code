"""Part 6: what happens when RAM fills up. maxmemory and eviction policies.

This script changes maxmemory on the server it connects to and puts the
old settings back at the end. Only run it against a throwaway local Redis.
"""
import redis

from common import get_client, reset, title

r = get_client()
reset(r, "fill:*", "points:*")
old_mem = r.config_get("maxmemory")["maxmemory"]
old_policy = r.config_get("maxmemory-policy")["maxmemory-policy"]
print("defaults on this server -> maxmemory:", old_mem, "| policy:", old_policy)

VALUE = "x" * 1000                                   # 1 KB per key


def fill(n):
    written = 0
    for i in range(n):
        try:
            r.set(f"fill:{i}", VALUE)
            written += 1
        except redis.exceptions.OutOfMemoryError as e:
            print("  write rejected:", str(e)[:60], "...")
            break
    return written


try:
    used = int(r.info("memory")["used_memory"])
    limit = used + 2 * 1024 * 1024                   # 2 MB above what we use now
    r.config_set("maxmemory", limit)

    title("noeviction: Redis refuses new writes")
    r.config_set("maxmemory-policy", "noeviction")
    print("  keys written before the wall:", fill(10_000))
    reset(r, "fill:*")

    title("allkeys-lru: Redis deletes the least recently used keys")
    r.config_set("maxmemory-policy", "allkeys-lru")
    r.set("points:brad", 250)                        # loyalty data on the same instance!
    before = int(r.info("stats")["evicted_keys"])
    print("  keys written:", fill(10_000))
    print("  keys evicted:", int(r.info("stats")["evicted_keys"]) - before)
    print("  is Brad's loyalty data still there?", r.get("points:brad"))

    title("volatile-lru: only keys WITH a TTL can be evicted")
    reset(r, "fill:*", "points:*")
    r.config_set("maxmemory-policy", "volatile-lru")
    r.set("points:brad", 250)                        # no TTL: protected
    for i in range(10_000):
        r.set(f"fill:{i}", VALUE, ex=3600)           # cache entries with a TTL
    print("  Brad's points:", r.get("points:brad"))
finally:
    reset(r, "fill:*", "points:*")
    r.config_set("maxmemory", old_mem)
    r.config_set("maxmemory-policy", old_policy)
    print("\nsettings restored")
