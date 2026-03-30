"""Tests for the async rate limiter."""

import asyncio
import time

import pytest

from src.utils.rate_limiter import RateLimiter


@pytest.mark.asyncio
async def test_unlimited_does_not_block():
    limiter = RateLimiter(max_calls=0, period_seconds=60)
    assert limiter.is_unlimited

    start = time.monotonic()
    for _ in range(100):
        async with limiter:
            pass
    elapsed = time.monotonic() - start
    assert elapsed < 1.0


@pytest.mark.asyncio
async def test_rate_limiting_blocks():
    limiter = RateLimiter(max_calls=3, period_seconds=1)

    start = time.monotonic()
    for _ in range(4):
        async with limiter:
            pass
    elapsed = time.monotonic() - start

    # 4th call should have waited ~1 second for token refill
    assert elapsed >= 0.9


@pytest.mark.asyncio
async def test_fallback_after_consecutive_429s():
    limiter = RateLimiter(max_calls=5, period_seconds=60, fallback_threshold=3)

    assert not limiter.should_fallback

    limiter.record_429()
    limiter.record_429()
    assert not limiter.should_fallback

    limiter.record_429()
    assert limiter.should_fallback


@pytest.mark.asyncio
async def test_success_resets_429_counter():
    limiter = RateLimiter(max_calls=5, period_seconds=60, fallback_threshold=3)

    limiter.record_429()
    limiter.record_429()
    limiter.record_success()

    assert not limiter.should_fallback

    # Need 3 consecutive again
    limiter.record_429()
    limiter.record_429()
    assert not limiter.should_fallback
