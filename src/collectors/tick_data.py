"""Collector for 1-minute OHLCV (tick) data.

Strategies by tier:
  - Free tier (5 calls/min): sequential fetching, best run as post-market batch.
    500 symbols / 5 per min = ~100 min.
  - Paid tier (unlimited): parallel async fetching with semaphore for concurrency control.
"""

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from src.collectors.base import BaseCollector
from src.models.schemas import TickBar

logger = logging.getLogger(__name__)


class TickDataCollector(BaseCollector):
    """Fetches 1-minute OHLCV bars for all configured symbols."""

    async def collect(self, target_date: date | None = None) -> None:
        """Collect all 1-min bars for target_date for all symbols."""
        target_date = target_date or date.today()
        logger.info("Collecting tick data for %s (%d symbols)", target_date, len(self.symbols))

        if self.rate_limiter.is_unlimited:
            bars = await self._fetch_all_parallel(target_date)
        else:
            bars = await self._fetch_all_rate_limited(target_date)

        if bars:
            self.parquet_store.write_tick_bars(bars, target_date)
            await self.postgres_store.insert_tick_bars(bars)
            logger.info("Stored %d total tick bars for %s", len(bars), target_date)
        else:
            logger.warning("No tick bars collected for %s", target_date)

    async def _fetch_all_parallel(self, target_date: date) -> list[TickBar]:
        """Paid tier: fetch all symbols concurrently with a semaphore."""
        sem = asyncio.Semaphore(50)

        async def fetch_with_sem(symbol: str) -> list[TickBar]:
            async with sem:
                return await self._fetch_polygon(symbol, target_date=target_date)

        tasks = [fetch_with_sem(sym) for sym in self.symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_bars: list[TickBar] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error("Failed to fetch %s: %s", self.symbols[i], result)
            else:
                all_bars.extend(result)

        return all_bars

    async def _fetch_all_rate_limited(self, target_date: date) -> list[TickBar]:
        """Free tier: fetch one symbol at a time, respecting rate limits."""
        all_bars: list[TickBar] = []

        for symbol in self.symbols:
            if self.rate_limiter.should_fallback:
                logger.warning("Rate limit hit, falling back to yfinance for %s", symbol)
                bars = await self._fetch_yfinance_fallback(symbol, target_date=target_date)
            else:
                async with self.rate_limiter:
                    bars = await self._fetch_polygon(symbol, target_date=target_date)

            all_bars.extend(bars)

        return all_bars

    async def _fetch_polygon(self, symbol: str, **kwargs: object) -> list[TickBar]:
        """Fetch 1-min bars from Polygon.io aggregates endpoint.

        GET /v2/aggs/ticker/{symbol}/range/1/minute/{date}/{date}
        """
        target_date = kwargs.get("target_date", date.today())

        url = f"/v2/aggs/ticker/{symbol}/range/1/minute/{target_date}/{target_date}"
        params = {"adjusted": "true", "sort": "asc", "limit": 50000}

        try:
            resp = await self.client.get(url, params=params)
        except Exception as e:
            logger.error("HTTP error fetching tick data for %s: %s", symbol, e)
            return []

        if resp.status_code == 429:
            self.rate_limiter.record_429()
            logger.warning("Rate limited (429) fetching tick data for %s", symbol)
            return []

        if resp.status_code != 200:
            logger.error("Unexpected status %d for %s: %s", resp.status_code, symbol, resp.text)
            return []

        self.rate_limiter.record_success()
        data = resp.json()
        results = data.get("results", [])

        bars: list[TickBar] = []
        for r in results:
            try:
                bars.append(TickBar(
                    symbol=symbol,
                    timestamp=datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc),
                    open=Decimal(str(r["o"])),
                    high=Decimal(str(r["h"])),
                    low=Decimal(str(r["l"])),
                    close=Decimal(str(r["c"])),
                    volume=r["v"],
                    vwap=Decimal(str(r["vw"])) if r.get("vw") else None,
                    num_transactions=r.get("n"),
                    source="polygon",
                ))
            except Exception as e:
                logger.warning("Skipping invalid bar for %s: %s", symbol, e)

        logger.debug("Fetched %d bars for %s from Polygon", len(bars), symbol)
        return bars

    async def _fetch_yfinance_fallback(self, symbol: str, **kwargs: object) -> list[TickBar]:
        """Fallback: fetch 1-min bars from yfinance.

        Note: yfinance only keeps 7 days of 1-min data.
        Runs synchronous yfinance call in an executor to avoid blocking.
        """
        target_date = kwargs.get("target_date", date.today())

        try:
            import yfinance as yf

            loop = asyncio.get_running_loop()
            ticker = yf.Ticker(symbol)
            df = await loop.run_in_executor(
                None,
                lambda: ticker.history(
                    start=str(target_date),
                    end=str(target_date + timedelta(days=1)),
                    interval="1m",
                ),
            )

            if df.empty:
                logger.debug("No yfinance data for %s on %s", symbol, target_date)
                return []

            bars: list[TickBar] = []
            for idx, row in df.iterrows():
                ts = idx.to_pydatetime()
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                bars.append(TickBar(
                    symbol=symbol,
                    timestamp=ts,
                    open=Decimal(str(round(row["Open"], 4))),
                    high=Decimal(str(round(row["High"], 4))),
                    low=Decimal(str(round(row["Low"], 4))),
                    close=Decimal(str(round(row["Close"], 4))),
                    volume=int(row["Volume"]),
                    vwap=None,
                    source="yfinance",
                ))

            logger.debug("Fetched %d bars for %s from yfinance", len(bars), symbol)
            return bars

        except Exception as e:
            logger.error("yfinance fallback failed for %s: %s", symbol, e)
            return []
