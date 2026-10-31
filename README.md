# Redis: Zero to Production · Code Companion

[![tests](https://github.com/arsinghin/redis-zero-to-production-code/actions/workflows/tests.yml/badge.svg)](https://github.com/arsinghin/redis-zero-to-production-code/actions/workflows/tests.yml)
[![Redis 8.10.2](https://img.shields.io/badge/Redis-8.10.2-DC382D)](https://redis.io/docs/latest/)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB)](https://www.python.org/)
[![Node 22](https://img.shields.io/badge/Node-22-339933)](https://nodejs.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Runnable, tested examples for the book
_[Redis: The Complete Guide from Zero to Production](https://arsinghin.gumroad.com/l/redis-zero-to-production)_
by AR Singh.**

The book follows one app from start to finish: **Slice & Dice**, a pizza
ordering service. It starts with a slow menu page and ends with a complete
production setup. Every pattern along the way is a script in this repo. The
book explains *why* each one works; this repo lets you run it, break it, and
watch it recover.

> **Get the book:** [Redis: The Complete Guide from Zero to Production](https://arsinghin.gumroad.com/l/redis-zero-to-production)
> 10 parts · 12 diagrams · PDF + EPUB

Every script runs end to end on **Redis 8.10.2**, **Python 3.12** with
**redis-py 8.1.0**, and **Node 22** with **node-redis 6.3.0**. The output
printed in the book is the real output of these files, and the same suite runs
on GitHub [after every change and every week](#automatic-tests).

> [!WARNING]
> Run these against a **throwaway local Redis**. Some scripts change server
> settings (Part 6 sets `maxmemory`) or write a few hundred thousand keys
> (Part 8). They only delete their own keys, but don't point them at anything
> you care about.

## Contents

- [What you'll learn](#what-youll-learn)
- [Quick start](#quick-start)
- [Requirements](#requirements)
- [Examples by book part](#examples-by-book-part)
- [Run the full test suite](#run-the-full-test-suite)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [About the author](#about-the-author)
- [License](#license)

## What you'll learn

- **Data structures** beyond strings: Hashes, Lists, Sets, Sorted Sets, Geo,
  HyperLogLog, Bitmaps, and per-field expiry
- **Caching** that survives production: cache-aside, invalidation, TTL jitter,
  stampede protection, negative caching, and failing open when Redis is down
- **Concurrency**: the double-sell race and four ways to fix it (`DECR`,
  `WATCH`/`MULTI`, Lua, Functions), safe locks, three rate limiters, and
  idempotency keys
- **Messaging**: Pub/Sub, Streams with consumer groups and crash recovery, and
  reliable queues
- **Durability**: what `kill -9` does with no persistence, RDB, and AOF, and how
  eviction policies behave when memory runs out
- **High availability**: replication, Sentinel failover, and a 6-node Cluster
  with `MOVED`, `CROSSSLOT`, hash tags, and automatic failover
- **Operations and security**: ACL users, `SCAN` instead of `KEYS`, `SLOWLOG`,
  memory analysis, and pipelining
- **AI workloads**: chat memory, vector search with Vector Sets, a semantic
  cache, and RAG retrieval

## Quick start

```bash
git clone https://github.com/arsinghin/redis-zero-to-production-code.git
cd redis-zero-to-production-code

# 1. Start a throwaway Redis 8
docker run -d --name redis -p 127.0.0.1:6379:6379 redis:8

# 2. Python examples
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cd python && python part1_basics.py

# 3. Node example (from the repo root)
cd node && npm install && node part1_basics.js
```

All scripts read `REDIS_URL` (default `redis://localhost:6379/0`):

```bash
REDIS_URL=redis://localhost:6380/0 python part3_caching.py
```

## Requirements

| | Version tested | Needed for |
|---|---|---|
| Redis | 8.10.2 (any 8.x works) | Everything |
| Python | 3.12 (3.10+ works) | All Python examples |
| redis-py | 8.1.0 (pinned in `requirements.txt`) | All Python examples |
| Node.js | 22 (18+ works) | `node/part1_basics.js` |
| node-redis | 6.3.0 (pinned in `node/package.json`) | `node/part1_basics.js` |
| `redis-server` + `redis-cli` on your PATH | 8.x | The Part 6 and Part 7 shell demos, which start their own servers |

Ways to get Redis 8:

| OS | Command |
|---|---|
| Any (Docker) | `docker run -d --name redis -p 127.0.0.1:6379:6379 redis:8` |
| macOS | `brew install redis` (gives you `redis-server` and `redis-cli`) |
| Ubuntu / Debian | Official packages: see [Install Redis on Linux](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/apt/) |
| Windows | Use Docker, or [WSL](https://learn.microsoft.com/windows/wsl/) with the Ubuntu steps |

## Examples by book part

```
redis-zero-to-production-code/
├── python/          one script per book part (run from this folder)
├── node/            the Part 1 example in Node.js
├── tests/           run_all.sh + the Part 6/7 demos that start and kill servers
├── docker/          Compose files: standalone, Sentinel, Cluster
└── requirements.txt
```

| Part | File | What it shows |
|:---:|---|---|
| | `python/common.py` | Shared client and a "delete only my demo keys" helper |
| 1 | `python/part1_basics.py`, `node/part1_basics.js` | `SET` / `GET` / `DEL`, `INCR`, `MGET` |
| 2 | `python/part2_structures.py` | Hashes, Lists, Sets, Sorted Sets, TTL, hash-field TTL, Geo, HyperLogLog, Bitmaps |
| 3 | `python/part3_caching.py` | Cache-aside, invalidation, TTL jitter, negative caching, stampede lock, failing open |
| 4 | `python/part4_concurrency.py` | The double-sell race, `DECR`, `WATCH`/`MULTI`, Lua, Functions, locks, 3 rate limiters, idempotency |
| 5 | `python/part5_messaging.py` | Pub/Sub, Streams, consumer groups and crash recovery, reliable list queue |
| 6 | `python/part6_memory.py` | `maxmemory`, `noeviction` vs `allkeys-lru` vs `volatile-lru` |
| 6 | `tests/part6_power_cut.sh` | `kill -9` with no persistence, with RDB, and with AOF |
| 7 | `tests/part7_replication.sh` | Primary and replica, `READONLY`, `WAIT` |
| 7 | `tests/part7_sentinel.sh` + `python/part7_sentinel_client.py` | 3 Sentinels, kill the primary, the client keeps writing |
| 7 | `tests/part7_cluster.sh` + `python/part7_cluster_client.py` | 6-node cluster, `MOVED`, `CROSSSLOT`, hash tags, automatic failover |
| 8 | `python/part8_operations.py` | ACLs, `KEYS` vs `SCAN`, `SLOWLOG`, `DEL` vs `UNLINK`, hit ratio, `MEMORY USAGE`, pipelining |
| 9 | `python/part9_ai.py` | Chat memory, Vector Sets search and filters, semantic cache, RAG retrieval |
| 9 | `python/part9b_query_engine_UNTESTED.py` | The same search with `FT.CREATE` / `FT.SEARCH` ([see below](#not-covered-by-the-test-suite)) |
| 10 | `python/part10_order_flow.py` | One order through every pattern in the book |
| Setup | `docker/compose.standalone.yml` | Redis 8 and the Redis Insight GUI |
| 7 | `docker/compose.sentinel.yml` | Primary, 2 replicas, 3 Sentinels, and a demo client |
| 7 | `docker/compose.cluster.yml` | 6-node cluster (Linux, host networking) |

## Run the full test suite

```bash
pip install -r requirements.txt
npm install --prefix node
bash tests/run_all.sh
```

Expected result:

```
passed: 14  failed: 0
```

The runner starts and stops its own Redis servers, so it needs `redis-server`
and `redis-cli` on your PATH and a **free port 6379**. Stop any Redis you
started for the quick start first (`docker stop redis`). It uses ports 6379,
6385, 6401–6402, 6411–6413, 26411–26413, and 7001–7006.

### Automatic tests

The [tests workflow](.github/workflows/tests.yml) runs the same suite on GitHub
on every push and pull request, and **every Monday**. The weekly run catches
breakage from new Redis or client releases before readers hit it. The badge at
the top of this page shows the latest result.

### Not covered by the test suite

- `python/part9b_query_engine_UNTESTED.py`: the Query Engine (`FT.*`) isn't in
  every Redis 8 build. The official `redis:8` Docker image includes it. Check
  with `redis-cli FT._LIST`.
- `docker/`: the same setups as the tested shell scripts, written for Docker
  Compose, but not run by the suite.

## Troubleshooting

| You see | What to do |
|---|---|
| `Something is already running on port 6379` | The test runner needs the port free. Stop the other Redis: `docker stop redis`, `brew services stop redis`, or `sudo systemctl stop redis-server`. |
| `ConnectionError: Error 111 connecting to localhost:6379` | Redis isn't running, or it's on another port. Start it, or set `REDIS_URL`. |
| `unknown command 'VADD'` or `'VSIM'` (Part 9) | Vector Sets need Redis 8.0 or newer. Check with `redis-server --version`. |
| `unknown command 'HEXPIRE'` (Part 2) | Hash-field expiry needs Redis 7.4 or newer. |
| `unknown command 'FT.CREATE'` (Part 9b) | Your build has no Query Engine. Use the `redis:8` Docker image. |
| `redis-server: command not found` | The shell demos need the binaries, not only Docker. Install with `brew install redis` or the Linux packages above. |
| `/bin/bash^M: bad interpreter` | The file has Windows line endings. Clone with git (the repo forces Unix endings) instead of copying files by hand, and run the scripts in WSL. |

Still stuck? [Open an issue](https://github.com/arsinghin/redis-zero-to-production-code/issues/new/choose).

## Contributing

Redis and its client libraries change often. If a script fails on a newer
version, please [report it](https://github.com/arsinghin/redis-zero-to-production-code/issues/new/choose).
The form asks for exactly what's needed to reproduce it. Fixes land here, so
every reader gets them without waiting for a new edition of the book.

Pull requests that fix or clarify existing examples are welcome; see
[CONTRIBUTING.md](CONTRIBUTING.md). Security problems:
[SECURITY.md](SECURITY.md).

## About the author

**AR Singh** wrote this book to turn Redis from a collection of isolated
facts into one connected mental model: not just *what* each command does, but
*why* it works and *when* it breaks in production.

- Book: [Redis: The Complete Guide from Zero to Production](https://arsinghin.gumroad.com/l/redis-zero-to-production)
- GitHub: [@arsinghin](https://github.com/arsinghin)
- Email: arsinghin@gmail.com

If the examples helped you, a star on this repo helps other engineers find it.

## License

The **code** in this repository is released under the [MIT License](LICENSE).
Use it in your own projects, commercial or not.

The **book** (text, diagrams, and cover) is not part of this repository and is
not covered by this license. © 2026 AR Singh. All rights reserved.
