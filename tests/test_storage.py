"""Tests for the Parquet storage layer.

PostgreSQL tests are integration tests that require a running database
and are skipped by default. Run with: pytest -m integration
"""

import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.models.schemas import EarningsRecord, OptionContract, OptionGreeks, OptionType, TickBar
from src.storage.parquet_store import ParquetStore


@pytest.fixture
def tmp_store(tmp_path):
    """ParquetStore backed by a temporary directory."""
    return ParquetStore(base_dir=tmp_path)


def _sample_tick_bars(symbol: str = "AAPL", count: int = 5) -> list[TickBar]:
    """Generate sample tick bars."""
    base_ts = datetime(2026, 3, 30, 14, 30, tzinfo=timezone.utc)
    return [
        TickBar(
            symbol=symbol,
            timestamp=datetime(
                base_ts.year, base_ts.month, base_ts.day,
                base_ts.hour, base_ts.minute + i,
                tzinfo=timezone.utc,
            ),
            open=Decimal("150.0000") + i,
            high=Decimal("151.0000") + i,
            low=Decimal("149.0000") + i,
            close=Decimal("150.5000") + i,
            volume=10000 * (i + 1),
            vwap=Decimal("150.2500") + i,
            source="polygon",
        )
        for i in range(count)
    ]


def _sample_option_contracts(symbol: str = "AAPL", count: int = 3) -> list[OptionContract]:
    """Generate sample option contracts."""
    now = datetime.now(timezone.utc)
    return [
        OptionContract(
            underlying_symbol=symbol,
            contract_ticker=f"O:{symbol}250418C{200 + i:08d}",
            option_type=OptionType.CALL,
            strike_price=Decimal(str(200 + i)),
            expiration_date=date(2025, 4, 18),
            bid=Decimal("5.50"),
            ask=Decimal("5.75"),
            volume=100 * (i + 1),
            open_interest=1000,
            implied_volatility=0.25,
            greeks=OptionGreeks(delta=0.5, gamma=0.03, theta=-0.05, vega=0.12),
            snapshot_timestamp=now,
        )
        for i in range(count)
    ]


def _sample_earnings(symbol: str = "AAPL") -> list[EarningsRecord]:
    """Generate sample earnings records."""
    now = datetime.now(timezone.utc)
    return [
        EarningsRecord(
            symbol=symbol,
            fiscal_year=2025,
            fiscal_quarter=q,
            report_date=date(2025, q * 3, 15),
            eps_actual=Decimal("1.50"),
            revenue_actual=Decimal("90000000000"),
            source="polygon",
            fetch_timestamp=now,
        )
        for q in range(1, 5)
    ]


# ─── Tick Data Tests ────────────────────────────────────────────


class TestParquetTickData:
    def test_write_and_read(self, tmp_store):
        bars = _sample_tick_bars("AAPL", 5)
        market_date = date(2026, 3, 30)

        paths = tmp_store.write_tick_bars(bars, market_date)
        assert len(paths) == 1
        assert paths[0].exists()

        table = tmp_store.read_tick_bars("AAPL", market_date, market_date)
        assert table.num_rows == 5

    def test_append_to_existing(self, tmp_store):
        market_date = date(2026, 3, 30)

        bars1 = _sample_tick_bars("AAPL", 3)
        tmp_store.write_tick_bars(bars1, market_date)

        bars2 = _sample_tick_bars("AAPL", 2)
        tmp_store.write_tick_bars(bars2, market_date)

        table = tmp_store.read_tick_bars("AAPL", market_date, market_date)
        assert table.num_rows == 5  # 3 + 2

    def test_multiple_symbols(self, tmp_store):
        market_date = date(2026, 3, 30)

        bars = _sample_tick_bars("AAPL", 3) + _sample_tick_bars("MSFT", 2)
        paths = tmp_store.write_tick_bars(bars, market_date)
        assert len(paths) == 2

        aapl = tmp_store.read_tick_bars("AAPL", market_date, market_date)
        msft = tmp_store.read_tick_bars("MSFT", market_date, market_date)
        assert aapl.num_rows == 3
        assert msft.num_rows == 2

    def test_read_empty(self, tmp_store):
        table = tmp_store.read_tick_bars("NONE", date(2020, 1, 1), date(2020, 1, 1))
        assert table.num_rows == 0

    def test_write_empty(self, tmp_store):
        paths = tmp_store.write_tick_bars([], date(2026, 3, 30))
        assert paths == []

    def test_date_partitioning(self, tmp_store):
        bars_day1 = _sample_tick_bars("AAPL", 3)
        bars_day2 = _sample_tick_bars("AAPL", 2)

        tmp_store.write_tick_bars(bars_day1, date(2026, 3, 30))
        tmp_store.write_tick_bars(bars_day2, date(2026, 3, 31))

        table = tmp_store.read_tick_bars("AAPL", date(2026, 3, 30), date(2026, 3, 31))
        assert table.num_rows == 5


# ─── Options Tests ──────────────────────────────────────────────


class TestParquetOptions:
    def test_write_and_read(self, tmp_store):
        contracts = _sample_option_contracts("AAPL", 3)
        snap_date = date(2026, 3, 30)

        path = tmp_store.write_option_chain(contracts, snap_date)
        assert path.exists()

        table = tmp_store.read_option_chain("AAPL", snap_date)
        assert table.num_rows == 3

    def test_read_empty(self, tmp_store):
        table = tmp_store.read_option_chain("NONE", date(2020, 1, 1))
        assert table.num_rows == 0

    def test_greeks_flattened(self, tmp_store):
        contracts = _sample_option_contracts("AAPL", 1)
        snap_date = date(2026, 3, 30)
        tmp_store.write_option_chain(contracts, snap_date)

        table = tmp_store.read_option_chain("AAPL", snap_date)
        assert "delta" in table.column_names
        assert "gamma" in table.column_names


# ─── Earnings Tests ─────────────────────────────────────────────


class TestParquetEarnings:
    def test_write_and_read(self, tmp_store):
        records = _sample_earnings("AAPL")

        paths = tmp_store.write_earnings(records)
        assert len(paths) == 1

        table = tmp_store.read_earnings("AAPL")
        assert table.num_rows == 4

    def test_deduplication_on_append(self, tmp_store):
        records = _sample_earnings("AAPL")
        tmp_store.write_earnings(records)

        # Write the same records again
        tmp_store.write_earnings(records)

        table = tmp_store.read_earnings("AAPL")
        assert table.num_rows == 4  # Deduplicated, not 8

    def test_read_empty(self, tmp_store):
        table = tmp_store.read_earnings("NONE")
        assert table.num_rows == 0
