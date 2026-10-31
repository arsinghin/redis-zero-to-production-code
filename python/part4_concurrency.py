"""Part 4: never sell the last slice twice.
Race conditions, atomic commands, WATCH/MULTI, Lua, Functions,
locks, rate limiters, and idempotency keys."""
import threading
import time
import uuid

import redis

from common import get_client, reset, title

r = get_client()
reset(r, "stock:*", "lock:*", "rl:*", "idem:*")


def hammer(fn, n):
    """Run fn() in n threads at once and count how many return True."""
    results = []
    lock = threading.Lock()

    def worker():
        ok = fn()
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return sum(results)


# ------------------------------------------------------ 1. the race condition
def naive_buy():
    stock = int(r.get("stock:pepperoni"))
    if stock > 0:
        time.sleep(0.01)                     # the tiny gap between read and write
        r.set("stock:pepperoni", stock - 1)
        return True
    return False


title("Naive GET-then-SET: 1 slice left, 2 buyers")
r.set("stock:pepperoni", 1)
print("sold:", hammer(naive_buy, 2), "| stock now:", r.get("stock:pepperoni"))


# ----------------------------------------------- 2. one atomic command: DECR
def decr_buy():
    left = r.decr("stock:pepperoni")
    if left >= 0:
        return True
    r.incr("stock:pepperoni")                # went below zero: put it back
    return False


title("DECR: 10 slices, 100 buyers")
r.set("stock:pepperoni", 10)
print("sold:", hammer(decr_buy, 100), "| stock now:", r.get("stock:pepperoni"))


# ------------------------------------------- 3. optimistic locking: WATCH/MULTI
def watch_buy():
    with r.pipeline() as pipe:
        while True:
            try:
                pipe.watch("stock:pepperoni")          # tell me if this changes
                stock = int(pipe.get("stock:pepperoni"))
                if stock <= 0:
                    pipe.unwatch()
                    return False
                pipe.multi()                           # queue the write...
                pipe.set("stock:pepperoni", stock - 1)
                pipe.execute()                         # ...fails if someone else wrote
                return True
            except redis.WatchError:
                continue                               # somebody beat us; try again


title("WATCH/MULTI: 10 slices, 100 buyers")
r.set("stock:pepperoni", 10)
print("sold:", hammer(watch_buy, 100), "| stock now:", r.get("stock:pepperoni"))


# ------------------------------------------------------- 4. a Lua script
BUY_LUA = """
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock >= tonumber(ARGV[1]) then
  redis.call('DECRBY', KEYS[1], ARGV[1])
  return 1
end
return 0
"""
buy_script = r.register_script(BUY_LUA)


def lua_buy():
    return buy_script(keys=["stock:pepperoni"], args=[1]) == 1


title("Lua: 10 slices, 100 buyers")
r.set("stock:pepperoni", 10)
print("sold:", hammer(lua_buy, 100), "| stock now:", r.get("stock:pepperoni"))
print("missing key -> buy returns:", buy_script(keys=["stock:calzone"], args=[1]))


# ------------------------------------------------- 5. Redis Functions (7.0+)
LIBRARY = """#!lua name=slicedice
local function buy_slice(keys, args)
  local stock = tonumber(redis.call('GET', keys[1]) or '0')
  if stock >= tonumber(args[1]) then
    redis.call('DECRBY', keys[1], args[1])
    return 1
  end
  return 0
end
redis.register_function('buy_slice', buy_slice)
"""
title("Functions: load once, call by name")
r.function_load(LIBRARY, replace=True)
r.set("stock:pepperoni", 2)
print([r.fcall("buy_slice", 1, "stock:pepperoni", 1) for _ in range(3)])   # [1, 1, 0]


# ------------------------------------------------------- 6. a simple lock
RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""
release_script = r.register_script(RELEASE_LUA)


def acquire(name, ttl_ms):
    token = str(uuid.uuid4())                       # who owns the lock
    if r.set(f"lock:{name}", token, nx=True, px=ttl_ms):
        return token
    return None


def release(name, token):
    return release_script(keys=[f"lock:{name}"], args=[token]) == 1


title("Lock: only one oven-scheduler at a time")
a = acquire("oven", 200)
b = acquire("oven", 200)
print("A got lock:", a is not None, "| B got lock:", b is not None)
time.sleep(0.25)                                    # A is slow; the lock expires
b = acquire("oven", 200)
print("after expiry, B got lock:", b is not None)
print("A's late release deletes B's lock?", release("oven", a))   # False
print("B releases its own lock:", release("oven", b))             # True


# -------------------------------------------------- 7a. fixed-window limiter
def fixed_window_allow(user, limit, window_s):
    bucket = int(time.time() // window_s)
    key = f"rl:fixed:{user}:{bucket}"
    pipe = r.pipeline()                              # MULTI/EXEC
    pipe.incr(key)
    pipe.expire(key, window_s, nx=True)              # only set TTL the first time
    count, _ = pipe.execute()
    return count <= limit


title("Fixed window: 5 orders per 10 seconds")
print([fixed_window_allow("user:7", 5, 10) for _ in range(7)])


# ------------------------------------------------ 7b. sliding-window limiter
SLIDING_LUA = """
local key, now, window, limit = KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
if redis.call('ZCARD', key) < limit then
  redis.call('ZADD', key, now, ARGV[4])
  redis.call('PEXPIRE', key, window)
  return 1
end
return 0
"""
sliding = r.register_script(SLIDING_LUA)


def sliding_allow(user, limit, window_ms):
    now_ms = int(time.time() * 1000)
    return sliding(keys=[f"rl:slide:{user}"],
                   args=[now_ms, window_ms, limit, f"{now_ms}-{uuid.uuid4()}"]) == 1


title("Sliding window: 3 requests per 300 ms")
print([sliding_allow("user:8", 3, 300) for _ in range(5)])
time.sleep(0.31)
print("after the window slides:", sliding_allow("user:8", 3, 300))


# -------------------------------------------------- 7c. token bucket limiter
BUCKET_LUA = """
local key = KEYS[1]
local capacity, rate = tonumber(ARGV[1]), tonumber(ARGV[2])   -- rate = tokens per second
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1]) or capacity
local ts = tonumber(data[2]) or now
tokens = math.min(capacity, tokens + (now - ts) * rate)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, math.ceil(capacity / rate) * 2)
return allowed
"""
bucket = r.register_script(BUCKET_LUA)

title("Token bucket: burst of 3, refills 10 tokens/second")
print([bucket(keys=["rl:bucket:user:9"], args=[3, 10]) for _ in range(5)])
time.sleep(0.2)                                       # 2 tokens come back
print("after 200 ms:", [bucket(keys=["rl:bucket:user:9"], args=[3, 10]) for _ in range(3)])


# ------------------------------------------------------ 8. idempotency keys
def place_order(idempotency_key):
    first = r.set(f"idem:{idempotency_key}", "done", nx=True, ex=86400)
    if not first:
        return "duplicate, ignored"
    return "charged card, order created"


title("Idempotency: the customer taps 'Pay' twice")
print(place_order("checkout-7f3a"))
print(place_order("checkout-7f3a"))
