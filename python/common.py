"""Shared helpers for the Slice & Dice examples.

Every script talks to REDIS_URL (default: a local Redis on port 6379).
Run these against a throwaway local Redis, never against production.
"""
import os

import redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def get_client(**kwargs):
    """Return a client that gives back str instead of bytes."""
    return redis.Redis.from_url(REDIS_URL, decode_responses=True, **kwargs)


def reset(r, *patterns):
    """Delete only the demo keys a script uses. We never call FLUSHDB."""
    for pattern in patterns:
        for key in r.scan_iter(match=pattern, count=500):
            r.unlink(key)


def title(text):
    print(f"\n=== {text} ===")
