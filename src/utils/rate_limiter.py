"""Async token-bucket rate limiter for Polygon API with yfinance fallback signaling."""

import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class RateLimiter:
    """Async token-bucket rate limiter.

    Usage:
        limiter = RateLimiter(max_calls=5, period_seconds=60)
        async with limiter:
            await httpx_client.get(...)

    Set max_calls=0 for unlimited (paid tier).
    """

    max_calls: int
    period_seconds: float = 60.0
    fallback_threshold: int = 3

    _tokens: int = field(init=False, default=0)
    _last_refill: float = field(init=False, default=0.0)
    _lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)
    _consecutive_429s: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        self._tokens = self.max_calls
        self._last_refill = time.monotonic()

    @property
    def is_unlimited(self) -> bool:
        return self.max_calls == 0

    @property
    def should_fallback(self) -> bool:
        """True when consecutive 429 responses exceed the threshold."""
        return self._consecutive_429s >= self.fallback_threshold

    async def acquire(self) -> None:
        """Wait until a token is available, then consume it."""
        if self.is_unlimited:
            return

        async with self._lock:
            self._refill()
            if self._tokens <= 0:
                sleep_time = self.period_seconds - (time.monotonic() - self._last_refill)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    self._refill()
            self._tokens -= 1

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed >= self.period_seconds:
            self._tokens = self.max_calls
            self._last_refill = now

    def record_429(self) -> None:
        """Record a rate-limit (HTTP 429) response."""
        self._consecutive_429s += 1

    def record_success(self) -> None:
        """Reset the 429 counter on a successful request."""
        self._consecutive_429s = 0

    async def __aenter__(self) -> "RateLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, *exc: object) -> None:
        pass
