"""Part 2: thinking in shapes. Every data structure from the chapter."""
from common import get_client, reset, title

r = get_client()
reset(r, "pizza:*", "kitchen_queue", "leaderboard", "session:*", "cart:*",
      "drivers", "visitors:*", "visitors_set:*", "ordered:*", "streak:*")

title("Hashes: one pizza, many fields")
r.hset("pizza:1", mapping={"name": "Margherita", "price": 12, "prep_time": 10})
r.hset("pizza:2", mapping={"name": "Pepperoni", "price": 14, "prep_time": 12})
print(r.hget("pizza:1", "price"))             # 12
print(r.hgetall("pizza:2"))
print(r.hincrby("pizza:1", "price", 1))       # 13  (price went up by $1)

title("Lists: the kitchen queue (first in, first out)")
r.rpush("kitchen_queue", "Order_101", "Order_102", "Order_103")
print(r.lpop("kitchen_queue"))                # Order_101
print(r.lrange("kitchen_queue", 0, -1))       # ['Order_102', 'Order_103']

title("Sets: tags with no duplicates")
r.sadd("pizza:1:tags", "Vegetarian", "Classic")
r.sadd("pizza:3:tags", "Vegetarian", "Spicy", "Vegan")
print(r.sadd("pizza:3:tags", "Spicy"))        # 0  (already there, ignored)
print(r.sinter("pizza:1:tags", "pizza:3:tags"))  # {'Vegetarian'}
print(r.sismember("pizza:3:tags", "Vegan"))   # 1  (yes, it is a member)

title("Sorted sets: the loyalty leaderboard")
r.zadd("leaderboard", {"user:brad": 150, "user:leo": 300, "user:tom": 50})
r.zincrby("leaderboard", 100, "user:brad")    # Brad buys more pizza
print(r.zrange("leaderboard", 0, 0, desc=True, withscores=True))   # [('user:leo', 300.0)]
print(r.zrange("leaderboard", 0, -1, desc=True, withscores=True))
print(r.zrevrank("leaderboard", "user:brad"))  # 1  (0 = first place)

title("TTL: data that cleans itself up")
r.set("session:user:99", "logged_in", ex=3600)
print(r.ttl("session:user:99"))               # 3600 (or just under)
print(r.ttl("pizza:1"))                       # -1  (no expiry set)
print(r.ttl("session:does-not-exist"))        # -2  (no such key)
r.persist("session:user:99")
print(r.ttl("session:user:99"))               # -1  (expiry removed)

title("Hash field TTL: expire one field, keep the rest")
r.hset("cart:42", mapping={"pizza:1": 2, "coupon": "FRIDAY10"})
print(r.hexpire("cart:42", 900, "coupon"))    # [1]  (TTL set on that field)
print(r.httl("cart:42", "coupon", "pizza:1")) # [900, -1]

title("Geo: find the nearest delivery driver")
r.geoadd("drivers", [-73.9851, 40.7589, "driver:ana",    # Times Square
                     -73.9680, 40.7851, "driver:raj",    # Upper West Side
                     -74.0060, 40.7128, "driver:kim"])   # Lower Manhattan
shop = (-73.9857, 40.7484)                               # the shop
nearby = r.geosearch("drivers", longitude=shop[0], latitude=shop[1],
                     radius=5, unit="km", sort="ASC", withdist=True)
print(nearby)                                 # closest first, distance in km

title("HyperLogLog: unique visitors in 12 KB")
pipe = r.pipeline(transaction=False)
for i in range(100_000):
    pipe.pfadd("visitors:today", f"user:{i}")
pipe.execute()
estimate = r.pfcount("visitors:today")
print("true count: 100000, estimate:", estimate,
      f"error: {abs(estimate - 100_000) / 1000:.2f}%")
print("HLL bytes:", r.memory_usage("visitors:today"))
pipe = r.pipeline(transaction=False)
for i in range(100_000):
    pipe.sadd("visitors_set:today", f"user:{i}")
pipe.execute()
print("Set bytes:", r.memory_usage("visitors_set:today"))

title("Bitmaps: who ordered on all three days?")
for day, users in {1: [3, 5, 7, 9], 2: [3, 7, 9], 3: [1, 3, 9]}.items():
    for user_id in users:
        r.setbit(f"ordered:day:{day}", user_id, 1)
print("ordered on day 1:", r.bitcount("ordered:day:1"))   # 4
r.bitop("AND", "streak:3days", "ordered:day:1", "ordered:day:2", "ordered:day:3")
print("ordered all 3 days:", r.bitcount("streak:3days"))   # 2 (users 3 and 9)
print("user 9 on the streak?", r.getbit("streak:3days", 9))  # 1
