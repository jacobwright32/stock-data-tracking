"""Pydantic models for all stock data types collected by the system."""

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


# ─── Tick / OHLCV Data ───────────────────────────────────────────────


class TickBar(BaseModel):
    """A single 1-minute OHLCV bar for one symbol."""

    symbol: str = Field(..., min_length=1, max_length=10)
    timestamp: datetime = Field(..., description="Bar open time in UTC")
    open: Decimal = Field(..., ge=0)
    high: Decimal = Field(..., ge=0)
    low: Decimal = Field(..., ge=0)
    close: Decimal = Field(..., ge=0)
    volume: int = Field(..., ge=0)
    vwap: Decimal | None = Field(None, ge=0)
    num_transactions: int | None = Field(None, ge=0)
    source: str = Field(default="polygon")

    @model_validator(mode="after")
    def high_gte_low(self) -> "TickBar":
        if self.high < self.low:
            raise ValueError("high must be >= low")
        return self


class TickBatch(BaseModel):
    """A batch of tick bars, typically one API response worth."""

    bars: list[TickBar]
    fetch_timestamp: datetime
    market_date: date


# ─── Options Data ───────────────────────────────────────────────────


class OptionType(StrEnum):
    CALL = "call"
    PUT = "put"


class OptionGreeks(BaseModel):
    """Greeks for an option contract. All fields optional as they may be unavailable."""

    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    rho: float | None = None


class OptionContract(BaseModel):
    """A single option contract snapshot."""

    underlying_symbol: str = Field(..., min_length=1, max_length=10)
    contract_ticker: str = Field(
        ..., description="OCC-style option symbol, e.g. O:AAPL250418C00200000"
    )
    option_type: OptionType
    strike_price: Decimal = Field(..., ge=0)
    expiration_date: date
    bid: Decimal | None = Field(None, ge=0)
    ask: Decimal | None = Field(None, ge=0)
    mid: Decimal | None = Field(None, ge=0)
    last_price: Decimal | None = Field(None, ge=0)
    volume: int | None = Field(None, ge=0)
    open_interest: int | None = Field(None, ge=0)
    implied_volatility: float | None = Field(None, ge=0)
    greeks: OptionGreeks | None = None
    break_even_price: Decimal | None = None
    snapshot_timestamp: datetime
    source: str = Field(default="polygon")


class OptionChainSnapshot(BaseModel):
    """Full option chain for one underlying on one date."""

    underlying_symbol: str
    snapshot_date: date
    contracts: list[OptionContract]
    fetch_timestamp: datetime


# ─── Earnings Data ─────────────────────────────────────────────────


class EarningsRecord(BaseModel):
    """One quarterly earnings report for one company."""

    symbol: str = Field(..., min_length=1, max_length=10)
    fiscal_year: int
    fiscal_quarter: int = Field(..., ge=1, le=4)
    report_date: date | None = None
    filing_date: date | None = None

    # EPS
    eps_estimate: Decimal | None = None
    eps_actual: Decimal | None = None
    eps_surprise: Decimal | None = None
    eps_surprise_pct: float | None = None

    # Revenue (in dollars)
    revenue_estimate: Decimal | None = None
    revenue_actual: Decimal | None = None
    revenue_surprise: Decimal | None = None
    revenue_surprise_pct: float | None = None

    # Guidance (forward-looking, if available)
    guidance_eps_low: Decimal | None = None
    guidance_eps_high: Decimal | None = None
    guidance_revenue_low: Decimal | None = None
    guidance_revenue_high: Decimal | None = None

    source: str = Field(default="polygon")
    fetch_timestamp: datetime | None = None


class AnnualEarnings(BaseModel):
    """Aggregated annual earnings derived from quarterly records.

    This is a computed model -- not stored directly in the database.
    Assemble from quarterly EarningsRecord rows.
    """

    symbol: str
    fiscal_year: int
    quarters: list[EarningsRecord] = Field(..., min_length=1, max_length=4)
    total_eps: Decimal | None = None
    total_revenue: Decimal | None = None
