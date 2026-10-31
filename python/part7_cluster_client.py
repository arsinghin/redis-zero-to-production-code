"""Part 7c: talking to Redis Cluster from Python.

Started by tests/part7_cluster.sh against a local 6-node cluster.
"""
from redis.cluster import RedisCluster
from redis.exceptions import ResponseError

rc = RedisCluster(host="127.0.0.1", port=7001, decode_responses=True)

print("=== the cluster client finds the right node for you ===")
rc.set("pizza:1", "Margherita")
print("pizza:1 ->", rc.get("pizza:1"), "| slot", rc.keyslot("pizza:1"))

print("=== multi-key commands need every key in ONE slot ===")
rc.sadd("pizza:1:tags", "Vegetarian", "Classic")
rc.sadd("pizza:3:tags", "Vegetarian", "Spicy", "Vegan")
print("slots:", rc.keyslot("pizza:1:tags"), "and", rc.keyslot("pizza:3:tags"))
try:
    # Send the raw command to one node, the way a plain client would.
    node = rc.get_node_from_key("pizza:1:tags")
    rc.execute_command("SINTER", "pizza:1:tags", "pizza:3:tags", target_nodes=node)
except ResponseError as e:
    print("SINTER error:", e)

print("=== hash tags: only the part in {braces} is hashed ===")
rc.sadd("{menu}:pizza:1:tags", "Vegetarian", "Classic")
rc.sadd("{menu}:pizza:3:tags", "Vegetarian", "Spicy", "Vegan")
print("slots:", rc.keyslot("{menu}:pizza:1:tags"), "and", rc.keyslot("{menu}:pizza:3:tags"))
print("SINTER:", rc.sinter("{menu}:pizza:1:tags", "{menu}:pizza:3:tags"))

print("=== a cart and its items, kept together with the user's id ===")
rc.hset("{user:42}:cart", mapping={"pizza:1": 2})
rc.set("{user:42}:cart:coupon", "FRIDAY10")
print("same slot?", rc.keyslot("{user:42}:cart") == rc.keyslot("{user:42}:cart:coupon"))
