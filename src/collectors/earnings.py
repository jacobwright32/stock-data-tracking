"""Collector for quarterly earnings data.

Fetches SEC-filed financial data (EPS, revenue) from Polygon.io,
with yfinance as a fallback. Runs weekly to catch new filings.

Known gap: Polygon provides actuals from SEC filings, not consensus estimates.
Estimate fields are left None until a separate estimates source is integrated.
"""

import asyncio
import logging
from datetime import date, datetime, timezone
from decimal import Decimal

from src.collectors.base import BaseCollector
from src.models.schemas import EarningsRecord

logger = logging.getLogger(__name__)


class EarningsCollector(BaseCollector):
    """Fetches quarterly earnings data for all configured symbols."""

    async def collect(self, target_date: date | None = None) -> None:
        """Fetch earnings records for all symbols."""
        target_date = target_date or date.today()
        logger.info("Collecting earnings data (%d symbols)", len(self.symbols))

        all_records: list[EarningsRecord] = []

        for symbol in self.symbols:
            if self.rate_limiter.should_fallback:
                logger.warning("Rate limit hit, falling back to yfinance for %s earnings", symbol)
                records = await self._fetch_yfinance_fallback(symbol)
            else:
                async with self.rate_limiter:
                    records = await self._fetch_polygon(symbol)

            all_records.extend(records)

        if all_records:
            self.parquet_store.write_earnings(all_records)
            await self.postgres_store.insert_earnings(all_records)
            logger.info("Stored %d total earnings records", len(all_records))
        else:
            logger.warning("No earnings records collected")

    async def _fetch_polygon(self, symbol: str, **kwargs: object) -> list[EarningsRecord]:
        """Fetch quarterly financials from Polygon.io.

        GET /vX/reference/financials?ticker={symbol}&timeframe=quarterly
        """
        url = "/vX/reference/financials"
        params = {
            "ticker": symbol,
            "timeframe": "quarterly",
            "order": "desc",
            "limit": 20,
        }

        try:
            resp = await self.client.get(url, params=params)
        except Exception as e:
            logger.error("HTTP error fetching earnings for %s: %s", symbol, e)
            return []

        if resp.status_code == 429:
            self.rate_limiter.record_429()
            logger.warning("Rate limited (429) fetching earnings for %s", symbol)
            return []

        if resp.status_code != 200:
            logger.error(
                "Unexpected status %d for %s earnings: %s",
                resp.status_code, symbol, resp.text,
            )
            return []

        self.rate_limiter.record_success()
        data = resp.json()
        now = datetime.now(timezone.utc)

        records: list[EarningsRecord] = []
        for r in data.get("results", []):
            try:
                financials = r.get("financials", {})
                income = financials.get("income_statement", {})

                eps_actual = income.get("basic_earnings_per_share", {}).get("value")
                revenue_actual = income.get("revenues", {}).get("value")

                # Parse fiscal period (e.g. "Q1", "Q2") to integer
                fiscal_period = r.get("fiscal_period", "")
                quarter = _parse_quarter(fiscal_period)
                if quarter is None:
                    continue

                records.append(EarningsRecord(
                    symbol=symbol,
                    fiscal_year=r.get("fiscal_year"),
                    fiscal_quarter=quarter,
                    report_date=_safe_date(r.get("start_date")),
                    filing_date=_safe_date(r.get("filing_date")),
                    eps_actual=Decimal(str(eps_actual)) if eps_actual is not None else None,
                    revenue_actual=Decimal(str(revenue_actual))
                    if revenue_actual is not None
                    else None,
                    source="polygon",
                    fetch_timestamp=now,
                ))
            except Exception as e:
                logger.warning("Skipping invalid earnings record for %s: %s", symbol, e)

        logger.debug("Fetched %d earnings records for %s from Polygon", len(records), symbol)
        return records

    async def _fetch_yfinance_fallback(self, symbol: str, **kwargs: object) -> list[EarningsRecord]:
        """Fallback: fetch earnings data from yfinance."""
        try:
            import yfinance as yf

            loop = asyncio.get_running_loop()
            ticker = yf.Ticker(symbol)

            # Get quarterly financials
            financials = await loop.run_in_executor(None, lambda: ticker.quarterly_financials)

            if financials is None or financials.empty:
                logger.debug("No yfinance earnings data for %s", symbol)
                return []

            now = datetime.now(timezone.utc)
            records: list[EarningsRecord] = []

            for col in financials.columns:
                try:
                    report_date = col.to_pydatetime().date() if hasattr(col, "to_pydatetime") else None
                    if report_date is None:
                        continue

                    # Estimate quarter from month
                    quarter = (report_date.month - 1) // 3 + 1
                    fiscal_year = report_date.year

                    # Try to get net income / total revenue
                    revenue = None
                    eps = None
                    for rev_key in ("Total Revenue", "Operating Revenue"):
                        if rev_key in financials.index:
                            val = financials.loc[rev_key, col]
                            if val is not None and str(val) != "nan":
                                revenue = Decimal(str(int(val)))
                                break

                    for eps_key in ("Basic EPS", "Diluted EPS"):
                        if eps_key in financials.index:
                            val = financials.loc[eps_key, col]
                            if val is not None and str(val) != "nan":
                                eps = Decimal(str(round(float(val), 4)))
                                break

                    records.append(EarningsRecord(
                        symbol=symbol,
                        fiscal_year=fiscal_year,
                        fiscal_quarter=quarter,
                        report_date=report_date,
                        eps_actual=eps,
                        revenue_actual=revenue,
                        source="yfinance",
                        fetch_timestamp=now,
                    ))
                except Exception as e:
                    logger.warning("Skipping yfinance earnings column for %s: %s", symbol, e)

            logger.debug("Fetched %d earnings records for %s from yfinance", len(records), symbol)
            return records

        except Exception as e:
            logger.error("yfinance earnings fallback failed for %s: %s", symbol, e)
            return []


def _parse_quarter(fiscal_period: str) -> int | None:
    """Parse 'Q1', 'Q2', 'Q3', 'Q4' to integer 1-4."""
    fiscal_period = fiscal_period.strip().upper()
    if fiscal_period in ("Q1", "Q2", "Q3", "Q4"):
        return int(fiscal_period[1])
    return None


def _safe_date(value: object) -> date | None:
    """Parse a date string, returning None on failure."""
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None
