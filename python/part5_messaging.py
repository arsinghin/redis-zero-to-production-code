"""Part 5: the real-time kitchen. Pub/Sub, Streams with consumer groups,
and a reliable list queue."""
import threading
import time

from common import get_client, reset, title

r = get_client()
reset(r, "orders:stream", "delivery:*", "queue:*")

# ------------------------------------------------------------ 1. Pub/Sub
title("Pub/Sub: the kitchen bell")
received = []


def waiter():
    p = r.pubsub()
    p.subscribe("oven_ready")
    for msg in p.listen():
        if msg["type"] == "message":
            received.append(msg["data"])
            if len(received) == 2:
                p.close()
                return


t = threading.Thread(target=waiter)
t.start()
time.sleep(0.2)                                       # let the waiter subscribe
print("listeners who heard it:", r.publish("oven_ready", "Pepperoni for Order #101 is ready!"))
print("listeners who heard it:", r.publish("oven_ready", "Veggie for Order #102 is ready!"))
t.join(timeout=2)
print("waiter got:", received)
print("nobody listening:", r.publish("oven_ready", "Order #103 is ready!"), "-> message is gone")

# ------------------------------------------------------- 2. Streams basics
title("Streams: an order's history")
for status in ("Order Placed", "In Oven", "Out for Delivery"):
    r.xadd("delivery:order101", {"status": status})
for entry_id, fields in r.xrange("delivery:order101", "-", "+"):
    print(entry_id, fields)
print("events since the start:", r.xlen("delivery:order101"))

# ----------------------------------------------- 3. consumer groups
title("Consumer groups: chefs share the order stream")
r.xgroup_create("orders:stream", "kitchen", id="0", mkstream=True)
for n in range(101, 107):
    r.xadd("orders:stream", {"order": n, "pizza": "Pepperoni"}, maxlen=10_000, approximate=True)


def take(chef, count):
    reply = r.xreadgroup("kitchen", chef, {"orders:stream": ">"}, count=count)
    return [(eid, f["order"]) for _, entries in reply for eid, f in entries]


chef_a = take("chef_a", 3)
chef_b = take("chef_b", 3)
print("chef_a got orders:", [o for _, o in chef_a])
print("chef_b got orders:", [o for _, o in chef_b])

for eid, _ in chef_a:                                  # chef_a finishes his pizzas
    r.xack("orders:stream", "kitchen", eid)
print("chef_b crashes before saying 'done'...")

summary = r.xpending("orders:stream", "kitchen")
print("pending (taken, not acknowledged):", summary["pending"],
      "| by consumer:", {c["name"]: c["pending"] for c in summary["consumers"]})

time.sleep(0.2)
_next, claimed, _deleted = r.xautoclaim("orders:stream", "kitchen", "chef_c",
                                        min_idle_time=100, start_id="0-0")
print("chef_c reclaims stuck orders:", [f["order"] for _, f in claimed])
for eid, _ in claimed:
    r.xack("orders:stream", "kitchen", eid)
print("pending now:", r.xpending("orders:stream", "kitchen")["pending"])

# ------------------------------------------------ 4. reliable list queue
title("Reliable queue with LMOVE: a crash doesn't lose the job")
r.rpush("queue:jobs", "print-receipt-101", "print-receipt-102")


def worker(crash=False):
    job = r.blmove("queue:jobs", "queue:processing", timeout=1, src="LEFT", dest="RIGHT")
    if crash:
        return job                                     # crashed: job stays in processing
    # ... do the work ...
    r.lrem("queue:processing", 1, job)                 # done: remove it
    return job


print("worker 1 took:", worker(crash=True), "(and crashed)")
print("worker 2 took:", worker())
print("still in processing:", r.lrange("queue:processing", 0, -1))
# A recovery job moves anything stuck in processing back to the queue.
while (job := r.lmove("queue:processing", "queue:jobs", "LEFT", "RIGHT")):
    print("recovered:", job)
print("queue now:", r.lrange("queue:jobs", 0, -1))
