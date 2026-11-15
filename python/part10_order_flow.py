"""Part 10: one Friday-night order, start to finish.
Every step uses a pattern from an earlier part, with:
  1. Atomic Lua checkout combining idempotency lookup/store, ACKED trim on
     backlog pressure, stock decrement, and ACKED stream append across
     hash-tagged KEYS.
  2. Dual consumer groups ('kitchen' and 'db_sync') with full cursor-based
     XAUTOCLAIM pending recovery, post-commit XACK, and ACKED stream trimming.
  3. Idempotency-aware rate limiting that prevents simultaneous retries with
     the same idem_key from burning extra rate-limit tokens or returning
     'slow down' when the order succeeds."""
from concurrent.futures import ThreadPoolExecutor
import json
import threading
import time
import redis

from common import get_client, reset, title

r = get_client()

STREAM_KEY = "{orders}:stream"
TARGET_STREAM_LEN = 100_000     # ~3 min at 556 orders/s (~20 MB planning estimate)
MAX_STREAM_BACKLOG = 1_000_000  # ~30 min max outage buffer (~200 MB unverified estimate)
IDEM_TTL_SECONDS = 86_400       # 24 hours
DEFAULT_CLAIM_IDLE_MS = 60_000  # 60 s (> normal max worker processing time)


def setup_shop(initial_pepperoni_stock=3):
    reset(
        r,
        "{orders}:*",
        "stock:*",
        "rl:*",
        "idem:*",
        "cart:*",
        "orders:stream",
        "leaderboard",
        "drivers",
        "cache:*",
        "session:*",
    )
    r.set("{orders}:stock:pepperoni", initial_pepperoni_stock)
    r.geoadd(
        "drivers",
        [-73.9851, 40.7589, "driver:ana", -74.0060, 40.7128, "driver:kim"],
    )
    # Register every required consumer group before orders arrive so Redis 8.2+
    # ACKED trimming retains entries until BOTH groups have read and XACKed them.
    r.xgroup_create(STREAM_KEY, "kitchen", id="0", mkstream=True)
    r.xgroup_create(STREAM_KEY, "db_sync", id="0", mkstream=True)


# Idempotency-aware rate-limit script:
#   KEYS[1] = {orders}:rl:<user>:<window_bucket>
#   KEYS[2] = {orders}:rl:<user>:idem:<idem_key>
#   KEYS[3] = {orders}:idem:<idem_key>
#   ARGV[1] = limit (max new attempts per window)
#   ARGV[2] = window_s (window length in seconds)
#
# Prevents two simultaneous requests with the same (user, idem_key) from burning
# two rate-limit tokens or rejecting the second request with 'slow down'.
RATE_LIMIT = r.register_script("""
local existing_order = redis.call('GET', KEYS[3])
if existing_order then
  return {1, existing_order}
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {1, ''}
end
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local limit = tonumber(ARGV[1])
local window_s = tonumber(ARGV[2])
if current >= limit then
  return {0, ''}
end
current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('EXPIRE', KEYS[1], window_s)
end
redis.call('SET', KEYS[2], '1', 'EX', window_s)
return {1, ''}
""")


# Checkout Lua script:
#   KEYS[1] = {orders}:stock:<pizza>
#   KEYS[2] = {orders}:stream
#   KEYS[3] = {orders}:idem:<idem_key>
#   ARGV[1] = user
#   ARGV[2] = pizza
#   ARGV[3] = qty (positive integer)
#   ARGV[4] = idem_ttl (positive integer seconds)
#   ARGV[5] = target_maxlen (positive integer)
#   ARGV[6] = max_backlog (integer >= target_maxlen)
#
# Redis Lua isolates execution from other clients, but does NOT roll back
# earlier writes if a later command errors. All argument, key-type, backlog,
# and stock checks therefore run BEFORE the first write command.
CHECKOUT = r.register_script("""
if #KEYS ~= 3 then
  return redis.error_reply('ERR CHECKOUT requires exactly 3 KEYS')
end

local user = ARGV[1]
local pizza = ARGV[2]
local qty = tonumber(ARGV[3])
local idem_ttl = tonumber(ARGV[4])
local target_maxlen = tonumber(ARGV[5])
local max_backlog = tonumber(ARGV[6])

if not user or user == '' or not pizza or pizza == '' then
  return redis.error_reply('ERR user and pizza must be non-empty strings')
end
if not qty or qty <= 0 or math.floor(qty) ~= qty then
  return redis.error_reply('ERR qty must be a positive integer')
end
if not idem_ttl or idem_ttl <= 0 or math.floor(idem_ttl) ~= idem_ttl then
  return redis.error_reply('ERR idem_ttl must be a positive integer')
end
if not target_maxlen or target_maxlen <= 0 or math.floor(target_maxlen) ~= target_maxlen then
  return redis.error_reply('ERR target_maxlen must be a positive integer')
end
if not max_backlog or max_backlog < target_maxlen or math.floor(max_backlog) ~= max_backlog then
  return redis.error_reply('ERR max_backlog must be an integer >= target_maxlen')
end

local stock_type = redis.call('TYPE', KEYS[1])['ok']
if stock_type ~= 'none' and stock_type ~= 'string' then
  return redis.error_reply('WRONGTYPE stock key must be a string')
end
local stream_type = redis.call('TYPE', KEYS[2])['ok']
if stream_type ~= 'none' and stream_type ~= 'stream' then
  return redis.error_reply('WRONGTYPE orders stream key must be a stream')
end
local idem_type = redis.call('TYPE', KEYS[3])['ok']
if idem_type ~= 'none' and idem_type ~= 'string' then
  return redis.error_reply('WRONGTYPE idempotency key must be a string')
end

local existing = redis.call('GET', KEYS[3])
if existing then
  return {'DUPLICATE', existing}
end

local raw_stock = redis.call('GET', KEYS[1]) or '0'
local stock = tonumber(raw_stock)
if not stock or stock < 0 or math.floor(stock) ~= stock then
  return redis.error_reply('ERR stock value is not a valid non-negative integer')
end

local current_len = redis.call('XLEN', KEYS[2])
if current_len >= max_backlog then
  redis.call('XTRIM', KEYS[2], 'MAXLEN', '~', tostring(target_maxlen), 'ACKED')
  current_len = redis.call('XLEN', KEYS[2])
  if current_len >= max_backlog then
    redis.call('XTRIM', KEYS[2], 'MAXLEN', '=', tostring(target_maxlen), 'ACKED')
    current_len = redis.call('XLEN', KEYS[2])
  end
  if current_len >= max_backlog then
    return {'BACKLOG_FULL', tostring(current_len)}
  end
end

if stock < qty then
  return {'SOLD_OUT', tostring(stock)}
end

redis.call('DECRBY', KEYS[1], qty)
local order_id = redis.call(
  'XADD', KEYS[2], 'ACKED', 'MAXLEN', '~', tostring(target_maxlen), '*',
  'user', user, 'pizza', pizza, 'qty', tostring(qty)
)
redis.call('SET', KEYS[3], order_id, 'EX', idem_ttl)
return {'PLACED', order_id}
""")


def rate_ok(user, idem_key=None, limit=5, window_s=60):   # Part 4
    bucket = int(time.time() // window_s)
    rl_key = f"{{orders}}:rl:{user}:{bucket}"
    if idem_key is None:
        pipe = r.pipeline()
        pipe.incr(rl_key)
        pipe.expire(rl_key, window_s, nx=True)
        return pipe.execute()[0] <= limit, ""
    admitted_key = f"{{orders}}:rl:{user}:idem:{idem_key}"
    idem_redis_key = f"{{orders}}:idem:{idem_key}"
    allowed, existing_order = RATE_LIMIT(
        keys=[rl_key, admitted_key, idem_redis_key],
        args=[int(limit), int(window_s)],
    )
    return bool(int(allowed)), existing_order


def checkout(
    user,
    pizza,
    qty,
    idem_key,
    target_maxlen=TARGET_STREAM_LEN,
    max_backlog=MAX_STREAM_BACKLOG,
):
    if not r.exists(f"session:{user}"):                   # Part 2: TTL sessions
        return "please log in"

    idem_redis_key = f"{{orders}}:idem:{idem_key}"
    allowed, existing_order_id = rate_ok(user, idem_key=idem_key)
    if existing_order_id:
        return f"already placed ({existing_order_id})"
    if not allowed:                                       # Part 4: rate limit
        # Re-check idempotency in case a concurrent request just finished CHECKOUT
        concurrent_order_id = r.get(idem_redis_key)
        if concurrent_order_id is not None:
            return f"already placed ({concurrent_order_id})"
        return "slow down"

    status, payload = CHECKOUT(
        keys=[
            f"{{orders}}:stock:{pizza}",
            STREAM_KEY,
            idem_redis_key,
        ],
        args=[user, pizza, int(qty), IDEM_TTL_SECONDS, int(target_maxlen), int(max_backlog)],
    )
    if status == "DUPLICATE":
        return f"already placed ({payload})"
    if status == "BACKLOG_FULL":
        r.delete(f"{{orders}}:rl:{user}:idem:{idem_key}")
        return "ordering paused, try again in a minute"
    if status == "SOLD_OUT":
        r.delete(f"{{orders}}:rl:{user}:idem:{idem_key}")
        return "sold out"

    order_id = payload
    r.zincrby("leaderboard", 10 * qty, user)               # Part 2: loyalty points
    r.delete(f"cart:{user}")                               # Part 2: cart is done
    r.delete("cache:menu")                                 # Part 3: stock changed
    r.publish("orders:new", json.dumps({"id": order_id}))  # Part 5: live ping
    return f"order {order_id} placed"


def _iter_group_entries(group, consumer, count=10, min_idle_ms=DEFAULT_CLAIM_IDLE_MS):
    """Yields (entry_id, fields) for a consumer group by:
      1. Reclaiming idle pending entries via cursor-paginated XAUTOCLAIM.
         Continues until next_cursor is '0-0' even if an intermediate scan
         batch returns zero claimed entries (with an unchanged-cursor guard).
      2. Reading any remaining pending entries owned by this consumer ('0').
      3. Reading new, never-delivered entries ('>')."""
    seen = set()

    if min_idle_ms is not None:
        cursor = "0-0"
        while True:
            next_cursor, claimed, _ = r.xautoclaim(
                STREAM_KEY,
                group,
                consumer,
                min_idle_time=min_idle_ms,
                start_id=cursor,
                count=count,
            )
            for eid, fields in claimed:
                if fields and eid not in seen:
                    seen.add(eid)
                    yield eid, fields
            if next_cursor in ("0-0", "0", None) or next_cursor == cursor:
                break
            cursor = next_cursor

    for _, entries in r.xreadgroup(group, consumer, {STREAM_KEY: "0"}, count=count):
        for eid, fields in entries:
            if fields and eid not in seen:
                seen.add(eid)
                yield eid, fields

    for _, entries in r.xreadgroup(group, consumer, {STREAM_KEY: ">"}, count=count):
        for eid, fields in entries:
            if fields and eid not in seen:
                seen.add(eid)
                yield eid, fields


def consume_kitchen_orders(
    consumer="chef_a",
    count=10,
    min_idle_ms=DEFAULT_CLAIM_IDLE_MS,
    target_maxlen=TARGET_STREAM_LEN,
    approximate_trim=True,
    verbose=True,
):
    """Reads and cooks orders in the 'kitchen' group, then triggers ACKED trim."""
    cooked_ids = []
    for eid, order in _iter_group_entries("kitchen", consumer, count=count, min_idle_ms=min_idle_ms):
        if verbose:
            print("  cooking", order)
        r.xack(STREAM_KEY, "kitchen", eid)
        cooked_ids.append(eid)
    if cooked_ids:
        r.xtrim(STREAM_KEY, maxlen=target_maxlen, approximate=approximate_trim, ref_policy="ACKED")
    return cooked_ids


def sync_orders_to_postgres(
    postgres_ledger,
    pg_available=True,
    consumer="sync_1",
    count=10,
    min_idle_ms=DEFAULT_CLAIM_IDLE_MS,
    target_maxlen=TARGET_STREAM_LEN,
    approximate_trim=True,
):
    """Recovers idle pending entries (via XAUTOCLAIM, default 60s idle timeout)
    and reads new entries for 'db_sync'. Uses postgres_ledger (a Python dict
    stand-in for a PostgreSQL table in this demo) to commit idempotently BEFORE
    calling XACK, then triggers ACKED stream trimming."""
    acked_ids = []
    for eid, order in _iter_group_entries("db_sync", consumer, count=count, min_idle_ms=min_idle_ms):
        if not pg_available:
            # Database write/commit failed: never call XACK.
            # Entry remains pending in 'db_sync' so ACKED trimming cannot evict it.
            continue
        # Step 1: Idempotent write + commit to PostgreSQL (INSERT ... ON CONFLICT DO NOTHING)
        postgres_ledger.setdefault(eid, dict(order))
        # Step 2: Acknowledge ONLY after the database transaction commits
        r.xack(STREAM_KEY, "db_sync", eid)
        acked_ids.append(eid)

    if acked_ids:
        r.xtrim(STREAM_KEY, maxlen=target_maxlen, approximate=approximate_trim, ref_policy="ACKED")
    return acked_ids


def run_verification_tests():
    """Covers all verification requirements, including:
      - Multi-batch XAUTOCLAIM cursor pagination across empty intermediate batches
      - Sequential and ThreadPoolExecutor concurrent duplicate checkout requests
      - Safe 60,000 ms default min_idle_ms (no stealing from active peers)"""
    assert sync_orders_to_postgres.__defaults__[3] == DEFAULT_CLAIM_IDLE_MS == 60_000

    setup_shop(initial_pepperoni_stock=3)
    for u in ("user:t1", "user:t2", "user:t3"):
        r.set(f"session:{u}", "ok", ex=3600)

    # Test 1 & 2: Correct XADD ACKED execution & duplicate checkout returning original order_id
    first = checkout("user:t1", "pepperoni", 1, "idem-t1", target_maxlen=1, max_backlog=2)
    assert first.startswith("order ") and first.endswith(" placed"), first
    first_order_id = first.split()[1]

    retry = checkout("user:t1", "pepperoni", 1, "idem-t1", target_maxlen=1, max_backlog=2)
    assert retry == f"already placed ({first_order_id})", retry
    assert int(r.get("{orders}:stock:pepperoni")) == 2
    assert r.xlen(STREAM_KEY) == 1

    # Verify idempotency retry still returns original order_id even if user is rate-limited
    bucket = int(time.time() // 60)
    rl_key = f"{{orders}}:rl:user:t1:{bucket}"
    r.set(rl_key, 999, ex=60)
    retry_when_rate_limited = checkout("user:t1", "pepperoni", 1, "idem-t1", target_maxlen=1, max_backlog=2)
    assert retry_when_rate_limited == f"already placed ({first_order_id})", retry_when_rate_limited
    r.delete(rl_key)

    # Test Simultaneous Duplicate Race on Final Rate-Limit Token:
    # Suppose user:t2 has used 4 of 5 tokens. Two simultaneous requests arrive with
    # the SAME idem_key ("idem-t2-race") before CHECKOUT runs. Both must pass rate_ok()
    # without the second one getting rejected with 'slow down'.
    r.set(f"{{orders}}:rl:user:t2:{bucket}", 4, ex=60)
    ok_a, existing_a = rate_ok("user:t2", idem_key="idem-t2-race", limit=5, window_s=60)
    ok_b, existing_b = rate_ok("user:t2", idem_key="idem-t2-race", limit=5, window_s=60)
    assert ok_a is True and existing_a == ""
    assert ok_b is True and existing_b == "", "Simultaneous retry with same idem_key must not fail rate_ok!"
    assert int(r.get(f"{{orders}}:rl:user:t2:{bucket}")) == 5
    # Meanwhile, a 6th distinct idem_key for user:t2 is properly rate-limited:
    ok_c, _ = rate_ok("user:t2", idem_key="idem-t2-distinct-6th", limit=5, window_s=60)
    assert ok_c is False
    r.delete(f"{{orders}}:rl:user:t2:{bucket}", "{orders}:rl:user:t2:idem:idem-t2-race")

    # Test 3: Insufficient stock does not leave an idempotency record
    sold_out = checkout("user:t2", "pepperoni", 5, "idem-t2-oversell", target_maxlen=1, max_backlog=2)
    assert sold_out == "sold out", sold_out
    assert r.exists("{orders}:idem:idem-t2-oversell") == 0
    assert int(r.get("{orders}:stock:pepperoni")) == 2

    # Test 4: Validation errors happen before any writes (stock unchanged)
    try:
        CHECKOUT(
            keys=["{orders}:stock:pepperoni", STREAM_KEY, "{orders}:idem:bad-arg"],
            args=["user:t2", "pepperoni", -1, IDEM_TTL_SECONDS, 1, 2],
        )
        raise AssertionError("Expected ResponseError for negative qty")
    except redis.ResponseError:
        pass
    assert int(r.get("{orders}:stock:pepperoni")) == 2
    assert r.exists("{orders}:idem:bad-arg") == 0

    # Place a second valid order so stream reaches max_backlog=2
    second = checkout("user:t2", "pepperoni", 1, "idem-t2-ok", target_maxlen=1, max_backlog=2)
    assert second.startswith("order ") and second.endswith(" placed"), second
    second_order_id = second.split()[1]
    assert int(r.get("{orders}:stock:pepperoni")) == 1
    assert r.xlen(STREAM_KEY) == 2

    # Test 5: PostgreSQL outage leaves entries pending and unacknowledged in db_sync
    pg_ledger = {}
    acked_during_outage = sync_orders_to_postgres(
        pg_ledger,
        pg_available=False,
        consumer="sync_crashed",
        min_idle_ms=0,
        target_maxlen=1,
        approximate_trim=False,
    )
    assert acked_during_outage == []
    assert len(pg_ledger) == 0
    assert r.xpending(STREAM_KEY, "db_sync")["pending"] == 2

    # Verify default min_idle_ms=60_000 does NOT prematurely steal fresh pending entries
    premature_steal = sync_orders_to_postgres(
        pg_ledger,
        pg_available=True,
        consumer="sync_peer",
        target_maxlen=1,
        approximate_trim=False,
    )
    assert premature_steal == [], f"Default 60s min_idle_ms must not steal fresh entries: {premature_steal}"

    # Test 8: Kitchen acknowledges both orders, but db_sync has NOT acknowledged them;
    # ACKED trimming must NOT evict either order.
    cooked = consume_kitchen_orders(
        consumer="chef_a",
        min_idle_ms=0,
        target_maxlen=1,
        approximate_trim=False,
        verbose=False,
    )
    assert len(cooked) == 2
    assert r.xlen(STREAM_KEY) == 2, f"Expected 2 unpersisted orders retained, got {r.xlen(STREAM_KEY)}"

    # Because backlog is still at max_backlog=2, a new checkout pauses cleanly
    paused = checkout("user:t3", "pepperoni", 1, "idem-t3", target_maxlen=1, max_backlog=2)
    assert paused == "ordering paused, try again in a minute", paused
    assert int(r.get("{orders}:stock:pepperoni")) == 1
    assert r.exists("{orders}:idem:idem-t3") == 0

    # Test 11: Duplicate retry still returns original order_id even when backlog is full
    dup_full = checkout("user:t1", "pepperoni", 1, "idem-t1", target_maxlen=1, max_backlog=2)
    assert dup_full == f"already placed ({first_order_id})", dup_full

    # Test 6, 7 & 9: PostgreSQL recovers; a worker with controlled min_idle_ms=0 reclaims
    # pending entries via XAUTOCLAIM, commits to Postgres before XACK, and trims backlog.
    recovered_acks = sync_orders_to_postgres(
        pg_ledger,
        pg_available=True,
        consumer="sync_recovered",
        min_idle_ms=0,
        target_maxlen=1,
        approximate_trim=False,
    )
    assert recovered_acks == [first_order_id, second_order_id], recovered_acks
    assert set(pg_ledger.keys()) == {first_order_id, second_order_id}
    assert r.xpending(STREAM_KEY, "db_sync")["pending"] == 0
    assert r.xlen(STREAM_KEY) == 1, f"Expected backlog trimmed to 1, got {r.xlen(STREAM_KEY)}"

    # Test 10: Checkout becomes available again after backlog recovery
    third = checkout("user:t3", "pepperoni", 1, "idem-t3", target_maxlen=1, max_backlog=2)
    assert third.startswith("order ") and third.endswith(" placed"), third
    assert int(r.get("{orders}:stock:pepperoni")) == 0

    # Test 12 (Point 1): Multi-batch XAUTOCLAIM pagination across an empty intermediate batch.
    # In Redis's t_stream.c, XAUTOCLAIM with count=1 scans at most 10*count = 10 PEL nodes per call.
    # If the first 10 PEL entries are fresh (idle < min_idle_ms) and entries 11..12 are idle
    # (idle >= min_idle_ms), the first XAUTOCLAIM call returns (non_zero_cursor, [], []).
    # _iter_group_entries() must continue past that empty batch and recover entries 11..12.
    setup_shop(initial_pepperoni_stock=20)
    r.set("session:user:batch", "ok", ex=3600)
    batch_order_ids = []
    for idx in range(12):
        res = checkout("user:batch", "pepperoni", 1, f"idem-batch-{idx}", target_maxlen=50, max_backlog=100)
        assert res.startswith("order "), res
        batch_order_ids.append(res.split()[1])
        r.delete("{orders}:rl:user:batch:" + str(int(time.time() // 60)))

    # 1. Dead worker reads all 12 entries into db_sync's PEL
    r.xreadgroup("db_sync", "dead_worker", {STREAM_KEY: ">"}, count=20)
    assert r.xpending(STREAM_KEY, "db_sync")["pending"] == 12

    # 2. Wait 160 ms so all 12 entries become idle (>= 120 ms)
    time.sleep(0.16)

    # 3. Active worker claims ONLY the first 10 entries, resetting their idle time to ~0 ms,
    #    while entries 11 and 12 remain owned by dead_worker with idle >= 160 ms.
    r.xclaim(STREAM_KEY, "db_sync", "active_worker", min_idle_time=0, message_ids=batch_order_ids[:10])

    # Verify Redis itself returns an empty claimed list with a non-0-0 cursor on the first step:
    probe_cursor, probe_claimed, _ = r.xautoclaim(
        STREAM_KEY, "db_sync", "probe_worker", min_idle_time=120, start_id="0-0", count=1
    )
    assert probe_claimed == [] and probe_cursor not in ("0-0", "0", None), (probe_cursor, probe_claimed)

    # 4. Now run sync_orders_to_postgres with count=1 and min_idle_ms=120:
    #    It must paginate past the empty first batch and recover entries 11 and 12!
    paginated_ledger = {}
    paginated_acks = sync_orders_to_postgres(
        paginated_ledger,
        pg_available=True,
        consumer="recover_worker",
        count=1,
        min_idle_ms=120,
    )
    assert paginated_acks == batch_order_ids[10:], f"Expected {batch_order_ids[10:]}, got {paginated_acks}"
    assert set(paginated_ledger.keys()) == set(batch_order_ids[10:])

    # Test 13: Genuine concurrent idempotency test using ThreadPoolExecutor.
    # 8 threads simultaneously call checkout() with the same user and idem_key
    # while the user has only 1 rate-limit token remaining (4 of 5 used).
    setup_shop(initial_pepperoni_stock=5)
    concurrent_user = "user:concurrent"
    concurrent_idem = "idem-concurrent-1"
    r.set(f"session:{concurrent_user}", "ok", ex=3600)
    r.hset(f"cart:{concurrent_user}", "pepperoni", 1)

    num_threads = 8
    barrier = threading.Barrier(num_threads)
    rl_bucket_key = f"{{orders}}:rl:{concurrent_user}:{int(time.time() // 60)}"
    r.set(rl_bucket_key, 4, ex=60)  # 4 of 5 rate-limit tokens already used

    def _concurrent_worker():
        barrier.wait(timeout=5)
        return checkout(concurrent_user, "pepperoni", 1, concurrent_idem)

    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(_concurrent_worker) for _ in range(num_threads)]
        results = [f.result(timeout=10) for f in futures]

    placed_results = [res for res in results if res.startswith("order ") and res.endswith(" placed")]
    dup_results = [res for res in results if res.startswith("already placed (")]
    assert len(placed_results) == 1, f"Expected exactly 1 placed order, got: {results}"
    assert len(dup_results) == num_threads - 1, f"Expected {num_threads - 1} duplicates, got: {results}"

    winning_order_id = placed_results[0].split()[1]
    assert all(res == f"already placed ({winning_order_id})" for res in dup_results), results
    assert int(r.get("{orders}:stock:pepperoni")) == 4, "Stock must decrease exactly once"
    assert r.xlen(STREAM_KEY) == 1, "Exactly one stream entry must be created"
    assert int(r.get(rl_bucket_key)) == 5, "Rate limiter must count the idempotency key only once"
    assert r.zscore("leaderboard", concurrent_user) == 10.0, "Points must be added only once"

    print("ALL PART 10 VERIFICATION TESTS PASSED")

# --- Main Demo Output (matches Section 10.3 of the book) ---
setup_shop(initial_pepperoni_stock=3)

title("Four customers, three slices")
for user in ("user:brad", "user:leo", "user:tom", "user:zoe"):
    r.set(f"session:{user}", "ok", ex=3600)
r.hset("cart:user:brad", "pepperoni", 2)

print("brad      :", checkout("user:brad", "pepperoni", 2, "brad-1"))
print("brad again:", checkout("user:brad", "pepperoni", 2, "brad-1"))   # double tap
print("leo       :", checkout("user:leo", "pepperoni", 1, "leo-1"))
print("tom       :", checkout("user:tom", "pepperoni", 1, "tom-1"))     # nothing left
print("stranger  :", checkout("user:nobody", "pepperoni", 1, "x-1"))
print("stock left:", r.get("{orders}:stock:pepperoni"))

title("The kitchen and PostgreSQL sync workers pick up the orders")
consume_kitchen_orders(consumer="chef_a", min_idle_ms=DEFAULT_CLAIM_IDLE_MS, verbose=True)

postgres_orders = {}
acked_by_db = sync_orders_to_postgres(
    postgres_orders,
    pg_available=True,
    consumer="sync_1",
    min_idle_ms=DEFAULT_CLAIM_IDLE_MS,
)
print("  persisted to postgres & acked:", acked_by_db)

title("Nearest driver and the leaderboard")
print("  driver:", r.geosearch("drivers", longitude=-73.9857, latitude=40.7484,
                               radius=5, unit="km", sort="ASC", count=1))
print("  top customers:", r.zrange("leaderboard", 0, -1, desc=True, withscores=True))

run_verification_tests()