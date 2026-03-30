"""Main entry point for the stock data tracking system.

Loads configuration, initializes storage and collectors, starts the scheduler,
and runs until interrupted with Ctrl+C.

Usage:
    python -m src.main
"""

import asyncio
import logging
import logging.config
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

from src.collectors.earnings import EarningsCollector
from src.collectors.options import OptionsCollector
from src.collectors.tick_data import TickDataCollector
from src.scheduler import create_scheduler
from src.storage.parquet_store import ParquetStore
from src.storage.postgres_store import PostgresStore
from src.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


async def main() -> None:
    load_dotenv()

    # Load config
    config_dir = Path("config")
    with open(config_dir / "settings.yaml") as f:
        config = yaml.safe_load(f)

    # Set up logging
    logging_config_path = config_dir / "logging.yaml"
    if logging_config_path.exists():
        # Ensure logs directory exists
        Path("logs").mkdir(exist_ok=True)
        with open(logging_config_path) as f:
            logging.config.dictConfig(yaml.safe_load(f))
    else:
        logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

    logger.info("Starting stock data tracking system")

    # Validate required env vars
    polygon_api_key = os.environ.get("POLYGON_API_KEY", "")
    if not polygon_api_key or polygon_api_key == "your_polygon_api_key_here":
        logger.error("POLYGON_API_KEY not set. Copy .env.example to .env and fill it in.")
        return

    postgres_dsn = os.environ.get("POSTGRES_DSN", "")
    if not postgres_dsn:
        logger.error("POSTGRES_DSN not set. Check your .env file.")
        return

    # Initialize storage
    data_dir = Path(os.getenv("DATA_DIR", config["storage"]["data_dir"]))
    parquet_store = ParquetStore(
        base_dir=data_dir,
        compression=config["storage"]["parquet"].get("compression", "snappy"),
    )

    postgres_store = PostgresStore(postgres_dsn)
    await postgres_store.initialize()

    # Rate limiter
    polygon_cfg = config.get("polygon", {})
    rate_limit = polygon_cfg.get("rate_limit_free", 5)
    rate_limiter = RateLimiter(max_calls=rate_limit, period_seconds=60)
    logger.info(
        "Rate limiter: %s",
        "unlimited" if rate_limiter.is_unlimited else f"{rate_limit} calls/min",
    )

    # Symbols
    symbols = config.get("symbols", [])
    logger.info("Tracking %d symbols", len(symbols))

    # Create collectors
    collector_kwargs = {
        "symbols": symbols,
        "polygon_api_key": polygon_api_key,
        "rate_limiter": rate_limiter,
        "parquet_store": parquet_store,
        "postgres_store": postgres_store,
    }
    tick_collector = TickDataCollector(**collector_kwargs)
    options_collector = OptionsCollector(**collector_kwargs)
    earnings_collector = EarningsCollector(**collector_kwargs)

    # Start scheduler
    schedules = config.get("schedules", {})
    scheduler = create_scheduler(
        tick_collector, options_collector, earnings_collector, schedules
    )
    scheduler.start()

    jobs = scheduler.get_jobs()
    for job in jobs:
        logger.info("Scheduled: %s (trigger: %s)", job.name, job.trigger)

    logger.info("Scheduler running. Press Ctrl+C to stop.")

    try:
        # Run forever until interrupted
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down...")
    finally:
        scheduler.shutdown(wait=False)
        await tick_collector.close()
        await options_collector.close()
        await earnings_collector.close()
        await postgres_store.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
