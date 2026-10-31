"""Part 10: one Friday-night order, start to finish.
Every step uses a pattern from an earlier part."""
import json
import time

from common import get_client, reset, title

r = get_client()
reset(r, "stock:*", "rl:*", "idem:*", "cart:*", "orders:stream", "leaderboard",
      "drivers", "cache:*", "session:*")

# Set up the shop.
r.set("stock:pepperoni", 3)
r.geoadd("drivers", [-73.9851, 40.7589, "driver:ana", -74.0060, 40.7128, "driver:kim"])
r.xgroup_create("orders:stream", "kitchen", id="0", mkstream=True)

BUY = r.register_script("""
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock >= tonumber(ARGV[1]) then
  redis.call('DECRBY', KEYS[1], ARGV[1])
  return 1
end
return 0
""")


def rate_ok(user, limit=5, window_s=60):                 # Part 4
    key = f"rl:{user}:{int(time.time() // window_s)}"
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, window_s, nx=True)
    return pipe.execute()[0] <= limit


def checkout(user, pizza, qty, idem_key):
    if not r.exists(f"session:{user}"):                   # Part 2: TTL sessions
        return "please log in"
    if not rate_ok(user):                                 # Part 4: rate limit
        return "slow down"
    if not r.set(f"idem:{idem_key}", 1, nx=True, ex=86400):   # Part 4: idempotency
        return "already placed"
    if BUY(keys=[f"stock:{pizza}"], args=[qty]) != 1:      # Part 4: atomic stock
        r.delete(f"idem:{idem_key}")
        return "sold out"
    order_id = r.xadd("orders:stream",                     # Part 5: durable event
                      {"user": user, "pizza": pizza, "qty": qty}, maxlen=100_000)
    r.zincrby("leaderboard", 10 * qty, user)               # Part 2: loyalty points
    r.delete(f"cart:{user}")                               # Part 2: cart is done
    r.delete("cache:menu")                                 # Part 3: stock changed
    r.publish("orders:new", json.dumps({"id": order_id}))  # Part 5: live ping
    return f"order {order_id} placed"


title("Four customers, three slices")
for user in ("user:brad", "user:leo", "user:tom", "user:zoe"):
    r.set(f"session:{user}", "ok", ex=3600)
r.hset("cart:user:brad", "pepperoni", 2)

print("brad      :", checkout("user:brad", "pepperoni", 2, "brad-1"))
print("brad again:", checkout("user:brad", "pepperoni", 2, "brad-1"))   # double tap
print("leo       :", checkout("user:leo", "pepperoni", 1, "leo-1"))
print("tom       :", checkout("user:tom", "pepperoni", 1, "tom-1"))     # nothing left
print("stranger  :", checkout("user:nobody", "pepperoni", 1, "x-1"))
print("stock left:", r.get("stock:pepperoni"))

title("The kitchen picks up the orders")
for _, entries in r.xreadgroup("kitchen", "chef_a", {"orders:stream": ">"}, count=10):
    for eid, order in entries:
        print("  cooking", order)
        r.xack("orders:stream", "kitchen", eid)

title("Nearest driver and the leaderboard")
print("  driver:", r.geosearch("drivers", longitude=-73.9857, latitude=40.7484,
                               radius=5, unit="km", sort="ASC", count=1))
print("  top customers:", r.zrange("leaderboard", 0, -1, desc=True, withscores=True))
