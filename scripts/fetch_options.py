"""Standalone script to fetch option chain snapshots.

Usage:
    python scripts/fetch_options.py                    # today's data
    python scripts/fetch_options.py 2026-03-27         # specific date
    python scripts/fetch_options.py 2026-03-27 AAPL    # specific date + symbol
"""

import asyncio
import logging
import os
import sys
from datetime import date
from pathlib import Path

import yaml
from dotenv import load_dotenv

from src.collectors.options import OptionsCollector
from src.storage.parquet_store import ParquetStore
from src.storage.postgres_store import PostgresStore
from src.utils.rate_limiter import RateLimiter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


async def main() -> None:
    load_dotenv()

    with open("config/settings.yaml") as f:
        config = yaml.safe_load(f)

    target_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()
    symbols = [sys.argv[2]] if len(sys.argv) > 2 else config["symbols"]

    parquet_store = ParquetStore(Path(os.getenv("DATA_DIR", config["storage"]["data_dir"])))
    postgres_store = PostgresStore(os.environ["POSTGRES_DSN"])
    await postgres_store.initialize()

    rate_limit = config["polygon"].get("rate_limit_free", 5)
    rate_limiter = RateLimiter(max_calls=rate_limit, period_seconds=60)

    collector = OptionsCollector(
        symbols=symbols,
        polygon_api_key=os.environ["POLYGON_API_KEY"],
        rate_limiter=rate_limiter,
        parquet_store=parquet_store,
        postgres_store=postgres_store,
    )

    try:
        await collector.collect(target_date)
    finally:
        await collector.close()
        await postgres_store.close()


if __name__ == "__main__":
    asyncio.run(main())
