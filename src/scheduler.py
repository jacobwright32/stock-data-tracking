"""APScheduler-based orchestrator with separate cron jobs for each data type.

Schedules:
  - Tick data:  every minute, Mon-Fri, 9:30 AM - 4:00 PM ET
  - Options:    daily at 4:30 PM ET, Mon-Fri
  - Earnings:   weekly on Sunday at 10:00 AM ET
"""

import logging
from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.collectors.earnings import EarningsCollector
from src.collectors.options import OptionsCollector
from src.collectors.tick_data import TickDataCollector

logger = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")


async def _tick_job(collector: TickDataCollector) -> None:
    """Wrapper that skips execution before 9:30 AM ET."""
    now_et = datetime.now(ET)
    if now_et.hour == 9 and now_et.minute < 30:
        logger.debug("Skipping tick collection: market not yet open (before 9:30 AM)")
        return
    await collector.collect()


async def _options_job(collector: OptionsCollector) -> None:
    """Wrapper for daily option chain collection."""
    await collector.collect()


async def _earnings_job(collector: EarningsCollector) -> None:
    """Wrapper for weekly earnings collection."""
    await collector.collect()


def create_scheduler(
    tick_collector: TickDataCollector,
    options_collector: OptionsCollector,
    earnings_collector: EarningsCollector,
    schedules: dict | None = None,
) -> AsyncIOScheduler:
    """Create and configure the scheduler with all three collection jobs.

    Args:
        tick_collector: Tick data collector instance.
        options_collector: Options collector instance.
        earnings_collector: Earnings collector instance.
        schedules: Optional schedule overrides from config/settings.yaml.

    Returns:
        Configured (but not started) AsyncIOScheduler.
    """
    scheduler = AsyncIOScheduler(timezone=ET)
    schedules = schedules or {}

    # ─── Tick Data ────────────────────────────────────────────
    tick_cfg = schedules.get("tick", {})
    scheduler.add_job(
        _tick_job,
        CronTrigger(
            minute=tick_cfg.get("minute", "*"),
            hour=tick_cfg.get("hour", "9-15"),
            day_of_week=tick_cfg.get("day_of_week", "mon-fri"),
            timezone=ET,
        ),
        args=[tick_collector],
        id="tick_data",
        name="1-min OHLCV bars",
        misfire_grace_time=tick_cfg.get("misfire_grace_time", 30),
        max_instances=1,
    )

    # ─── Options ──────────────────────────────────────────────
    options_cfg = schedules.get("options", {})
    scheduler.add_job(
        _options_job,
        CronTrigger(
            minute=options_cfg.get("minute", "30"),
            hour=options_cfg.get("hour", "16"),
            day_of_week=options_cfg.get("day_of_week", "mon-fri"),
            timezone=ET,
        ),
        args=[options_collector],
        id="options_snapshot",
        name="Daily option chain snapshot",
        misfire_grace_time=options_cfg.get("misfire_grace_time", 3600),
        max_instances=1,
    )

    # ─── Earnings ─────────────────────────────────────────────
    earnings_cfg = schedules.get("earnings", {})
    scheduler.add_job(
        _earnings_job,
        CronTrigger(
            minute=earnings_cfg.get("minute", "0"),
            hour=earnings_cfg.get("hour", "10"),
            day_of_week=earnings_cfg.get("day_of_week", "sun"),
            timezone=ET,
        ),
        args=[earnings_collector],
        id="earnings_check",
        name="Weekly earnings check",
        misfire_grace_time=earnings_cfg.get("misfire_grace_time", 7200),
        max_instances=1,
    )

    logger.info("Scheduler configured with %d jobs", len(scheduler.get_jobs()))
    return scheduler
