"""PostgreSQL storage layer using psycopg v3 with async support.

Handles table creation, bulk inserts, and upserts for tick data,
option chains, and earnings.
"""

import logging
from decimal import Decimal

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from src.models.schemas import EarningsRecord, OptionContract, TickBar

logger = logging.getLogger(__name__)

# ─── DDL ──────────────────────────────────────────────────────────────

CREATE_TICK_TABLE = """
CREATE TABLE IF NOT EXISTS tick_data (
    symbol          VARCHAR(10)    NOT NULL,
    timestamp       TIMESTAMPTZ    NOT NULL,
    open            NUMERIC(12,4)  NOT NULL,
    high            NUMERIC(12,4)  NOT NULL,
    low             NUMERIC(12,4)  NOT NULL,
    close           NUMERIC(12,4)  NOT NULL,
    volume          BIGINT         NOT NULL,
    vwap            NUMERIC(12,4),
    num_transactions INTEGER,
    source          VARCHAR(20)    NOT NULL DEFAULT 'polygon',
    PRIMARY KEY (symbol, timestamp)
);
"""

CREATE_TICK_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_tick_symbol ON tick_data (symbol);
CREATE INDEX IF NOT EXISTS idx_tick_timestamp ON tick_data (timestamp);
CREATE INDEX IF NOT EXISTS idx_tick_symbol_ts_desc ON tick_data (symbol, timestamp DESC);
"""

CREATE_OPTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS option_chains (
    underlying_symbol    VARCHAR(10)   NOT NULL,
    contract_ticker      VARCHAR(40)   NOT NULL,
    option_type          VARCHAR(4)    NOT NULL CHECK (option_type IN ('call', 'put')),
    strike_price         NUMERIC(12,4) NOT NULL,
    expiration_date      DATE          NOT NULL,
    bid                  NUMERIC(12,4),
    ask                  NUMERIC(12,4),
    mid                  NUMERIC(12,4),
    last_price           NUMERIC(12,4),
    volume               INTEGER,
    open_interest        INTEGER,
    implied_volatility   DOUBLE PRECISION,
    delta                DOUBLE PRECISION,
    gamma                DOUBLE PRECISION,
    theta                DOUBLE PRECISION,
    vega                 DOUBLE PRECISION,
    rho                  DOUBLE PRECISION,
    break_even_price     NUMERIC(12,4),
    snapshot_date        DATE          NOT NULL,
    snapshot_timestamp   TIMESTAMPTZ   NOT NULL,
    source               VARCHAR(20)   NOT NULL DEFAULT 'polygon',
    PRIMARY KEY (contract_ticker, snapshot_date)
);
"""

CREATE_OPTIONS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_options_underlying ON option_chains (underlying_symbol);
CREATE INDEX IF NOT EXISTS idx_options_expiration ON option_chains (expiration_date);
CREATE INDEX IF NOT EXISTS idx_options_underlying_snap ON option_chains (underlying_symbol, snapshot_date);
"""

CREATE_EARNINGS_TABLE = """
CREATE TABLE IF NOT EXISTS earnings (
    symbol                VARCHAR(10)   NOT NULL,
    fiscal_year           INTEGER       NOT NULL,
    fiscal_quarter        SMALLINT      NOT NULL CHECK (fiscal_quarter BETWEEN 1 AND 4),
    report_date           DATE,
    filing_date           DATE,
    eps_estimate          NUMERIC(10,4),
    eps_actual            NUMERIC(10,4),
    eps_surprise          NUMERIC(10,4),
    eps_surprise_pct      DOUBLE PRECISION,
    revenue_estimate      NUMERIC(18,2),
    revenue_actual        NUMERIC(18,2),
    revenue_surprise      NUMERIC(18,2),
    revenue_surprise_pct  DOUBLE PRECISION,
    guidance_eps_low      NUMERIC(10,4),
    guidance_eps_high     NUMERIC(10,4),
    guidance_revenue_low  NUMERIC(18,2),
    guidance_revenue_high NUMERIC(18,2),
    source                VARCHAR(20)   NOT NULL DEFAULT 'polygon',
    fetch_timestamp       TIMESTAMPTZ,
    updated_at            TIMESTAMPTZ   DEFAULT now(),
    PRIMARY KEY (symbol, fiscal_year, fiscal_quarter)
);
"""

CREATE_EARNINGS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_earnings_symbol ON earnings (symbol);
CREATE INDEX IF NOT EXISTS idx_earnings_report_date ON earnings (report_date);
CREATE INDEX IF NOT EXISTS idx_earnings_annual ON earnings (symbol, fiscal_year);
"""


def _decimal_or_none(val: Decimal | None) -> float | None:
    """Convert Decimal to float for psycopg parameter binding."""
    if val is None:
        return None
    return float(val)


class PostgresStore:
    """Async PostgreSQL storage with connection pooling."""

    def __init__(self, dsn: str, min_size: int = 2, max_size: int = 10) -> None:
        self.dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._pool: AsyncConnectionPool | None = None

    async def initialize(self) -> None:
        """Create connection pool and ensure all tables/indexes exist."""
        self._pool = AsyncConnectionPool(
            self.dsn, min_size=self._min_size, max_size=self._max_size
        )
        await self._pool.open()
        await self._create_tables()
        logger.info("PostgreSQL store initialized")

    async def _create_tables(self) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(CREATE_TICK_TABLE)
                await cur.execute(CREATE_TICK_INDEXES)
                await cur.execute(CREATE_OPTIONS_TABLE)
                await cur.execute(CREATE_OPTIONS_INDEXES)
                await cur.execute(CREATE_EARNINGS_TABLE)
                await cur.execute(CREATE_EARNINGS_INDEXES)
            await conn.commit()
        logger.info("Database tables and indexes verified")

    # ─── Tick Data ────────────────────────────────────────────

    async def insert_tick_bars(self, bars: list[TickBar]) -> int:
        """Bulk insert tick bars. Returns number of rows inserted.

        Uses ON CONFLICT DO NOTHING for idempotent re-runs.
        """
        if not bars:
            return 0

        query = """
            INSERT INTO tick_data (symbol, timestamp, open, high, low, close, volume, vwap, num_transactions, source)
            VALUES (%(symbol)s, %(timestamp)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(vwap)s, %(num_transactions)s, %(source)s)
            ON CONFLICT (symbol, timestamp) DO NOTHING
        """

        inserted = 0
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                for bar in bars:
                    await cur.execute(query, {
                        "symbol": bar.symbol,
                        "timestamp": bar.timestamp,
                        "open": _decimal_or_none(bar.open),
                        "high": _decimal_or_none(bar.high),
                        "low": _decimal_or_none(bar.low),
                        "close": _decimal_or_none(bar.close),
                        "volume": bar.volume,
                        "vwap": _decimal_or_none(bar.vwap),
                        "num_transactions": bar.num_transactions,
                        "source": bar.source,
                    })
                    inserted += cur.rowcount
            await conn.commit()

        logger.info("Inserted %d tick bars into PostgreSQL", inserted)
        return inserted

    # ─── Option Chains ────────────────────────────────────────

    async def insert_option_contracts(
        self, contracts: list[OptionContract], snapshot_date: "date"
    ) -> int:
        """Bulk insert option contracts. ON CONFLICT DO NOTHING."""
        if not contracts:
            return 0

        query = """
            INSERT INTO option_chains (
                underlying_symbol, contract_ticker, option_type, strike_price,
                expiration_date, bid, ask, mid, last_price, volume, open_interest,
                implied_volatility, delta, gamma, theta, vega, rho,
                break_even_price, snapshot_date, snapshot_timestamp, source
            ) VALUES (
                %(underlying_symbol)s, %(contract_ticker)s, %(option_type)s, %(strike_price)s,
                %(expiration_date)s, %(bid)s, %(ask)s, %(mid)s, %(last_price)s, %(volume)s, %(open_interest)s,
                %(implied_volatility)s, %(delta)s, %(gamma)s, %(theta)s, %(vega)s, %(rho)s,
                %(break_even_price)s, %(snapshot_date)s, %(snapshot_timestamp)s, %(source)s
            )
            ON CONFLICT (contract_ticker, snapshot_date) DO NOTHING
        """

        inserted = 0
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                for contract in contracts:
                    greeks = contract.greeks or {}
                    if hasattr(greeks, "model_dump"):
                        greeks = greeks.model_dump()

                    await cur.execute(query, {
                        "underlying_symbol": contract.underlying_symbol,
                        "contract_ticker": contract.contract_ticker,
                        "option_type": contract.option_type.value,
                        "strike_price": _decimal_or_none(contract.strike_price),
                        "expiration_date": contract.expiration_date,
                        "bid": _decimal_or_none(contract.bid),
                        "ask": _decimal_or_none(contract.ask),
                        "mid": _decimal_or_none(contract.mid),
                        "last_price": _decimal_or_none(contract.last_price),
                        "volume": contract.volume,
                        "open_interest": contract.open_interest,
                        "implied_volatility": contract.implied_volatility,
                        "delta": greeks.get("delta") if isinstance(greeks, dict) else None,
                        "gamma": greeks.get("gamma") if isinstance(greeks, dict) else None,
                        "theta": greeks.get("theta") if isinstance(greeks, dict) else None,
                        "vega": greeks.get("vega") if isinstance(greeks, dict) else None,
                        "rho": greeks.get("rho") if isinstance(greeks, dict) else None,
                        "break_even_price": _decimal_or_none(contract.break_even_price),
                        "snapshot_date": snapshot_date,
                        "snapshot_timestamp": contract.snapshot_timestamp,
                        "source": contract.source,
                    })
                    inserted += cur.rowcount
            await conn.commit()

        logger.info("Inserted %d option contracts into PostgreSQL", inserted)
        return inserted

    # ─── Earnings ─────────────────────────────────────────────

    async def insert_earnings(self, records: list[EarningsRecord]) -> int:
        """Upsert earnings records. ON CONFLICT DO UPDATE to handle revised filings."""
        if not records:
            return 0

        query = """
            INSERT INTO earnings (
                symbol, fiscal_year, fiscal_quarter, report_date, filing_date,
                eps_estimate, eps_actual, eps_surprise, eps_surprise_pct,
                revenue_estimate, revenue_actual, revenue_surprise, revenue_surprise_pct,
                guidance_eps_low, guidance_eps_high, guidance_revenue_low, guidance_revenue_high,
                source, fetch_timestamp
            ) VALUES (
                %(symbol)s, %(fiscal_year)s, %(fiscal_quarter)s, %(report_date)s, %(filing_date)s,
                %(eps_estimate)s, %(eps_actual)s, %(eps_surprise)s, %(eps_surprise_pct)s,
                %(revenue_estimate)s, %(revenue_actual)s, %(revenue_surprise)s, %(revenue_surprise_pct)s,
                %(guidance_eps_low)s, %(guidance_eps_high)s, %(guidance_revenue_low)s, %(guidance_revenue_high)s,
                %(source)s, %(fetch_timestamp)s
            )
            ON CONFLICT (symbol, fiscal_year, fiscal_quarter) DO UPDATE SET
                report_date = EXCLUDED.report_date,
                filing_date = EXCLUDED.filing_date,
                eps_estimate = COALESCE(EXCLUDED.eps_estimate, earnings.eps_estimate),
                eps_actual = COALESCE(EXCLUDED.eps_actual, earnings.eps_actual),
                eps_surprise = COALESCE(EXCLUDED.eps_surprise, earnings.eps_surprise),
                eps_surprise_pct = COALESCE(EXCLUDED.eps_surprise_pct, earnings.eps_surprise_pct),
                revenue_estimate = COALESCE(EXCLUDED.revenue_estimate, earnings.revenue_estimate),
                revenue_actual = COALESCE(EXCLUDED.revenue_actual, earnings.revenue_actual),
                revenue_surprise = COALESCE(EXCLUDED.revenue_surprise, earnings.revenue_surprise),
                revenue_surprise_pct = COALESCE(EXCLUDED.revenue_surprise_pct, earnings.revenue_surprise_pct),
                guidance_eps_low = COALESCE(EXCLUDED.guidance_eps_low, earnings.guidance_eps_low),
                guidance_eps_high = COALESCE(EXCLUDED.guidance_eps_high, earnings.guidance_eps_high),
                guidance_revenue_low = COALESCE(EXCLUDED.guidance_revenue_low, earnings.guidance_revenue_low),
                guidance_revenue_high = COALESCE(EXCLUDED.guidance_revenue_high, earnings.guidance_revenue_high),
                source = EXCLUDED.source,
                fetch_timestamp = EXCLUDED.fetch_timestamp,
                updated_at = now()
        """

        upserted = 0
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                for record in records:
                    await cur.execute(query, {
                        "symbol": record.symbol,
                        "fiscal_year": record.fiscal_year,
                        "fiscal_quarter": record.fiscal_quarter,
                        "report_date": record.report_date,
                        "filing_date": record.filing_date,
                        "eps_estimate": _decimal_or_none(record.eps_estimate),
                        "eps_actual": _decimal_or_none(record.eps_actual),
                        "eps_surprise": _decimal_or_none(record.eps_surprise),
                        "eps_surprise_pct": record.eps_surprise_pct,
                        "revenue_estimate": _decimal_or_none(record.revenue_estimate),
                        "revenue_actual": _decimal_or_none(record.revenue_actual),
                        "revenue_surprise": _decimal_or_none(record.revenue_surprise),
                        "revenue_surprise_pct": record.revenue_surprise_pct,
                        "guidance_eps_low": _decimal_or_none(record.guidance_eps_low),
                        "guidance_eps_high": _decimal_or_none(record.guidance_eps_high),
                        "guidance_revenue_low": _decimal_or_none(record.guidance_revenue_low),
                        "guidance_revenue_high": _decimal_or_none(record.guidance_revenue_high),
                        "source": record.source,
                        "fetch_timestamp": record.fetch_timestamp,
                    })
                    upserted += cur.rowcount
            await conn.commit()

        logger.info("Upserted %d earnings records into PostgreSQL", upserted)
        return upserted

    # ─── Lifecycle ────────────────────────────────────────────

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("PostgreSQL connection pool closed")
