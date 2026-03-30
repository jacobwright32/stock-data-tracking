"""Tests for Pydantic data models."""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.models.schemas import (
    AnnualEarnings,
    EarningsRecord,
    OptionChainSnapshot,
    OptionContract,
    OptionGreeks,
    OptionType,
    TickBar,
    TickBatch,
)


# ─── TickBar Tests ────────────────────────────────────────────────


class TestTickBar:
    def test_valid_tick_bar(self):
        bar = TickBar(
            symbol="AAPL",
            timestamp=datetime(2026, 3, 30, 14, 30, tzinfo=timezone.utc),
            open=Decimal("150.0000"),
            high=Decimal("151.5000"),
            low=Decimal("149.5000"),
            close=Decimal("151.0000"),
            volume=100000,
            vwap=Decimal("150.5000"),
            num_transactions=500,
        )
        assert bar.symbol == "AAPL"
        assert bar.source == "polygon"

    def test_high_must_be_gte_low(self):
        with pytest.raises(ValidationError, match="high must be >= low"):
            TickBar(
                symbol="AAPL",
                timestamp=datetime(2026, 3, 30, 14, 30, tzinfo=timezone.utc),
                open=Decimal("150.0000"),
                high=Decimal("149.0000"),
                low=Decimal("150.0000"),
                close=Decimal("150.0000"),
                volume=100,
            )

    def test_negative_volume_rejected(self):
        with pytest.raises(ValidationError):
            TickBar(
                symbol="AAPL",
                timestamp=datetime(2026, 3, 30, 14, 30, tzinfo=timezone.utc),
                open=Decimal("150.0000"),
                high=Decimal("151.0000"),
                low=Decimal("149.0000"),
                close=Decimal("150.0000"),
                volume=-1,
            )

    def test_empty_symbol_rejected(self):
        with pytest.raises(ValidationError):
            TickBar(
                symbol="",
                timestamp=datetime(2026, 3, 30, 14, 30, tzinfo=timezone.utc),
                open=Decimal("150.0000"),
                high=Decimal("151.0000"),
                low=Decimal("149.0000"),
                close=Decimal("150.0000"),
                volume=100,
            )

    def test_model_dump_roundtrip(self):
        bar = TickBar(
            symbol="MSFT",
            timestamp=datetime(2026, 3, 30, 14, 30, tzinfo=timezone.utc),
            open=Decimal("300.0000"),
            high=Decimal("305.0000"),
            low=Decimal("299.0000"),
            close=Decimal("304.0000"),
            volume=50000,
        )
        d = bar.model_dump()
        bar2 = TickBar(**d)
        assert bar == bar2


class TestTickBatch:
    def test_valid_batch(self):
        bar = TickBar(
            symbol="AAPL",
            timestamp=datetime(2026, 3, 30, 14, 30, tzinfo=timezone.utc),
            open=Decimal("150.0000"),
            high=Decimal("151.0000"),
            low=Decimal("149.0000"),
            close=Decimal("150.0000"),
            volume=100,
        )
        batch = TickBatch(
            bars=[bar],
            fetch_timestamp=datetime.now(timezone.utc),
            market_date=date(2026, 3, 30),
        )
        assert len(batch.bars) == 1


# ─── Option Contract Tests ───────────────────────────────────────


class TestOptionContract:
    def test_valid_call_option(self):
        contract = OptionContract(
            underlying_symbol="AAPL",
            contract_ticker="O:AAPL250418C00200000",
            option_type=OptionType.CALL,
            strike_price=Decimal("200.0000"),
            expiration_date=date(2025, 4, 18),
            bid=Decimal("5.50"),
            ask=Decimal("5.75"),
            volume=1000,
            open_interest=5000,
            implied_volatility=0.25,
            greeks=OptionGreeks(delta=0.55, gamma=0.03, theta=-0.05, vega=0.12),
            snapshot_timestamp=datetime.now(timezone.utc),
        )
        assert contract.option_type == OptionType.CALL
        assert contract.greeks.delta == 0.55

    def test_valid_put_option_no_greeks(self):
        contract = OptionContract(
            underlying_symbol="TSLA",
            contract_ticker="O:TSLA250418P00150000",
            option_type=OptionType.PUT,
            strike_price=Decimal("150.0000"),
            expiration_date=date(2025, 4, 18),
            snapshot_timestamp=datetime.now(timezone.utc),
        )
        assert contract.greeks is None
        assert contract.source == "polygon"

    def test_negative_strike_rejected(self):
        with pytest.raises(ValidationError):
            OptionContract(
                underlying_symbol="AAPL",
                contract_ticker="O:AAPL250418C00200000",
                option_type=OptionType.CALL,
                strike_price=Decimal("-1.0"),
                expiration_date=date(2025, 4, 18),
                snapshot_timestamp=datetime.now(timezone.utc),
            )


class TestOptionChainSnapshot:
    def test_valid_snapshot(self):
        contract = OptionContract(
            underlying_symbol="AAPL",
            contract_ticker="O:AAPL250418C00200000",
            option_type=OptionType.CALL,
            strike_price=Decimal("200.0000"),
            expiration_date=date(2025, 4, 18),
            snapshot_timestamp=datetime.now(timezone.utc),
        )
        snapshot = OptionChainSnapshot(
            underlying_symbol="AAPL",
            snapshot_date=date(2026, 3, 30),
            contracts=[contract],
            fetch_timestamp=datetime.now(timezone.utc),
        )
        assert len(snapshot.contracts) == 1


# ─── Earnings Tests ──────────────────────────────────────────────


class TestEarningsRecord:
    def test_valid_earnings(self):
        record = EarningsRecord(
            symbol="AAPL",
            fiscal_year=2025,
            fiscal_quarter=4,
            report_date=date(2025, 10, 30),
            eps_actual=Decimal("1.64"),
            revenue_actual=Decimal("94930000000"),
            source="polygon",
            fetch_timestamp=datetime.now(timezone.utc),
        )
        assert record.fiscal_quarter == 4
        assert record.eps_actual == Decimal("1.64")

    def test_invalid_quarter_rejected(self):
        with pytest.raises(ValidationError):
            EarningsRecord(
                symbol="AAPL",
                fiscal_year=2025,
                fiscal_quarter=5,
            )

    def test_quarter_zero_rejected(self):
        with pytest.raises(ValidationError):
            EarningsRecord(
                symbol="AAPL",
                fiscal_year=2025,
                fiscal_quarter=0,
            )

    def test_all_optional_fields_none(self):
        record = EarningsRecord(
            symbol="AAPL",
            fiscal_year=2025,
            fiscal_quarter=1,
        )
        assert record.eps_actual is None
        assert record.revenue_actual is None
        assert record.guidance_eps_low is None


class TestAnnualEarnings:
    def test_aggregation(self):
        quarters = [
            EarningsRecord(
                symbol="AAPL",
                fiscal_year=2025,
                fiscal_quarter=q,
                eps_actual=Decimal("1.50"),
                revenue_actual=Decimal("90000000000"),
            )
            for q in range(1, 5)
        ]
        annual = AnnualEarnings(
            symbol="AAPL",
            fiscal_year=2025,
            quarters=quarters,
            total_eps=sum(q.eps_actual for q in quarters),
            total_revenue=sum(q.revenue_actual for q in quarters),
        )
        assert annual.total_eps == Decimal("6.00")
        assert len(annual.quarters) == 4
