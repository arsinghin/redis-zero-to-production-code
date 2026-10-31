"""Part 3: caching for real. Cache-aside, invalidation, jitter,
negative caching, stampede protection, and failing open."""
import json
import random
import threading
import time

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from common import get_client, reset, title

r = get_client()
reset(r, "cache:*", "lock:*")

# ---------------------------------------------------------------- the "database"
DB = {"pizza:1": {"name": "Margherita", "price": 12},
      "pizza:2": {"name": "Pepperoni", "price": 14},
      "pizza:3": {"name": "Veggie Supreme", "price": 13}}
db_calls = 0
db_lock = threading.Lock()


def slow_db_menu():
    """Pretend this is a slow SQL query (50 ms)."""
    global db_calls
    with db_lock:
        db_calls += 1
    time.sleep(0.05)
    return list(DB.values())


MENU_KEY = "cache:menu"
BASE_TTL = 300                      # 5 minutes


def ttl_with_jitter():
    """300-360 s, so keys written together don't all expire together."""
    return BASE_TTL + random.randint(0, 60)


# ------------------------------------------------------------- 1. cache-aside
def get_menu():
    cached = r.get(MENU_KEY)
    if cached is not None:                       # cache hit
        return json.loads(cached)
    menu = slow_db_menu()                        # cache miss: go to the DB
    r.set(MENU_KEY, json.dumps(menu), ex=ttl_with_jitter())
    return menu


title("Cache-aside: miss, then hit")
for attempt in ("first call (miss)", "second call (hit)"):
    start = time.perf_counter()
    get_menu()
    print(f"{attempt}: {(time.perf_counter() - start) * 1000:.2f} ms")
print("TTL on menu key:", r.ttl(MENU_KEY), "seconds")


# ------------------------------------------------------ 2. invalidate on write
def update_price(pizza_id, new_price):
    DB[pizza_id]["price"] = new_price            # 1. write the source of truth
    r.delete(MENU_KEY)                           # 2. then delete the cached copy


title("Update the DB, then delete the cache key")
update_price("pizza:2", 15)
print("cache key after update:", r.get(MENU_KEY))          # None
print("next read:", [p for p in get_menu() if p["name"] == "Pepperoni"])


# ----------------------------------------------------- 3. negative caching
MISSING = "__none__"


def get_pizza(pizza_id):
    key = f"cache:{pizza_id}"
    cached = r.get(key)
    if cached == MISSING:
        return None                               # we already know it isn't there
    if cached is not None:
        return json.loads(cached)
    global db_calls
    db_calls += 1
    pizza = DB.get(pizza_id)
    if pizza is None:
        r.set(key, MISSING, ex=60)                # remember "not found" briefly
        return None
    r.set(key, json.dumps(pizza), ex=ttl_with_jitter())
    return pizza


title("Negative caching: stop asking the DB for things that don't exist")
db_calls = 0
for _ in range(1000):
    get_pizza("pizza:999")
print("1000 lookups of a missing pizza -> DB calls:", db_calls)    # 1


# ---------------------------------------------- 4. stampede (thundering herd)
def get_menu_protected():
    cached = r.get(MENU_KEY)
    if cached is not None:
        return json.loads(cached)
    # Only one caller wins this lock and rebuilds the cache.
    if r.set("lock:menu", "1", nx=True, px=5000):
        try:
            menu = slow_db_menu()
            r.set(MENU_KEY, json.dumps(menu), ex=ttl_with_jitter())
            return menu
        finally:
            r.delete("lock:menu")
    # Everyone else waits a little and reads the fresh cache.
    for _ in range(50):
        time.sleep(0.01)
        cached = r.get(MENU_KEY)
        if cached is not None:
            return json.loads(cached)
    return slow_db_menu()                         # last resort


def run_herd(fn, n=50):
    global db_calls
    r.delete(MENU_KEY)
    db_calls = 0
    threads = [threading.Thread(target=fn) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return db_calls


title("Stampede: 50 users arrive the moment the menu expires")
print("plain cache-aside   -> DB calls:", run_herd(get_menu))
print("with a rebuild lock -> DB calls:", run_herd(get_menu_protected))


# --------------------------------------------------------------- 5. fail open
# Short timeouts and NO retries: if Redis is gone, give up fast.
# (redis-py retries failed connections several times by default.)
broken = redis.Redis(host="localhost", port=6399,          # nothing listens here
                     socket_connect_timeout=0.2, socket_timeout=0.2,
                     retry=Retry(NoBackoff(), 0),
                     decode_responses=True)


def get_menu_fail_open(client):
    try:
        cached = client.get(MENU_KEY)
        if cached is not None:
            return json.loads(cached)
    except redis.RedisError as e:
        print("  Redis is down, serving from the DB:", type(e).__name__)
        return slow_db_menu()
    menu = slow_db_menu()
    try:
        client.set(MENU_KEY, json.dumps(menu), ex=ttl_with_jitter())
    except redis.RedisError:
        pass
    return menu


title("Fail open: Redis is down, the app keeps working")
start = time.perf_counter()
menu = get_menu_fail_open(broken)
print(f"  got {len(menu)} pizzas in {(time.perf_counter() - start) * 1000:.0f} ms")
