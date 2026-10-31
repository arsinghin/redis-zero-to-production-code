"""Part 1: your first Redis commands from Python."""
from common import get_client, reset, title

r = get_client()
reset(r, "pizza:*", "orders:*")

title("SET, GET, DEL")
r.set("pizza:1", "Margherita - $12")
r.set("pizza:2", "Pepperoni - $14")
r.set("pizza:3", "Veggie Supreme - $13")
print(r.get("pizza:2"))          # Pepperoni - $14
print(r.delete("pizza:3"))       # 1  (one key deleted)
print(r.get("pizza:3"))          # None (the key is gone)

title("A counter with INCR")
r.set("orders:today", 100)
r.incr("orders:today")
r.incr("orders:today")
print(r.get("orders:today"))     # 102

title("Many keys in one round trip")
print(r.mget("pizza:1", "pizza:2", "pizza:404"))
