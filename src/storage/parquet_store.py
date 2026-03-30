"""Parquet file storage for tick, options, and earnings data.

Files are partitioned by date and symbol for efficient reads:
  - tick:     data/tick/{YYYY-MM-DD}/{SYMBOL}.parquet
  - options:  data/options/{YYYY-MM-DD}/{SYMBOL}.parquet
  - earnings: data/earnings/{SYMBOL}.parquet
"""

import logging
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.models.schemas import EarningsRecord, OptionContract, TickBar

logger = logging.getLogger(__name__)


def _models_to_table(records: list, exclude_fields: set[str] | None = None) -> pa.Table:
    """Convert a list of Pydantic models to a PyArrow Table.

    Decimal fields are serialized as float64 for Parquet compatibility.
    Nested models (e.g. OptionGreeks) are flattened.
    """
    if not records:
        return pa.table({})

    rows: list[dict] = []
    for record in records:
        d = record.model_dump()
        if exclude_fields:
            for f in exclude_fields:
                d.pop(f, None)

        # Flatten nested OptionGreeks
        greeks = d.pop("greeks", None)
        if greeks is not None:
            for k, v in greeks.items():
                d[k] = v
        elif hasattr(record, "greeks"):
            # Ensure greek columns exist even when None
            for k in ("delta", "gamma", "theta", "vega", "rho"):
                d[k] = None

        # Convert Decimals to float for Parquet
        for k, v in d.items():
            if hasattr(v, "as_tuple"):  # Decimal check
                d[k] = float(v)

        rows.append(d)

    return pa.Table.from_pylist(rows)


class ParquetStore:
    """Read/write Parquet files with date-based partitioning."""

    def __init__(self, base_dir: Path, compression: str = "snappy") -> None:
        self.base_dir = Path(base_dir)
        self.compression = compression

    # ─── Tick Data ────────────────────────────────────────────

    def write_tick_bars(self, bars: list[TickBar], market_date: date) -> list[Path]:
        """Write tick bars partitioned by symbol.

        Returns the list of written file paths.
        """
        if not bars:
            return []

        by_symbol: dict[str, list[TickBar]] = {}
        for bar in bars:
            by_symbol.setdefault(bar.symbol, []).append(bar)

        written: list[Path] = []
        for symbol, symbol_bars in by_symbol.items():
            dir_path = self.base_dir / "tick" / str(market_date)
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / f"{symbol}.parquet"

            table = _models_to_table(symbol_bars)

            if file_path.exists():
                existing = pq.read_table(file_path)
                table = pa.concat_tables([existing, table])

            pq.write_table(table, file_path, compression=self.compression)
            written.append(file_path)
            logger.info("Wrote %d tick bars to %s", len(symbol_bars), file_path)

        return written

    def read_tick_bars(self, symbol: str, start: date, end: date) -> pa.Table:
        """Read tick bars for a symbol across a date range."""
        tables: list[pa.Table] = []
        current = start
        while current <= end:
            file_path = self.base_dir / "tick" / str(current) / f"{symbol}.parquet"
            if file_path.exists():
                tables.append(pq.read_table(file_path))
            current = date.fromordinal(current.toordinal() + 1)

        if not tables:
            return pa.table({})
        return pa.concat_tables(tables)

    # ─── Options Data ─────────────────────────────────────────

    def write_option_chain(self, contracts: list[OptionContract], snapshot_date: date) -> Path:
        """Write option contracts for one underlying on one date."""
        if not contracts:
            return Path()

        symbol = contracts[0].underlying_symbol
        dir_path = self.base_dir / "options" / str(snapshot_date)
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"{symbol}.parquet"

        table = _models_to_table(contracts)
        pq.write_table(table, file_path, compression=self.compression)
        logger.info("Wrote %d option contracts to %s", len(contracts), file_path)
        return file_path

    def read_option_chain(self, symbol: str, snapshot_date: date) -> pa.Table:
        """Read a single day's option chain for a symbol."""
        file_path = self.base_dir / "options" / str(snapshot_date) / f"{symbol}.parquet"
        if not file_path.exists():
            return pa.table({})
        return pq.read_table(file_path)

    # ─── Earnings Data ────────────────────────────────────────

    def write_earnings(self, records: list[EarningsRecord]) -> list[Path]:
        """Write earnings records, one file per symbol (append + deduplicate)."""
        if not records:
            return []

        by_symbol: dict[str, list[EarningsRecord]] = {}
        for record in records:
            by_symbol.setdefault(record.symbol, []).append(record)

        written: list[Path] = []
        earnings_dir = self.base_dir / "earnings"
        earnings_dir.mkdir(parents=True, exist_ok=True)

        for symbol, symbol_records in by_symbol.items():
            file_path = earnings_dir / f"{symbol}.parquet"
            table = _models_to_table(symbol_records)

            if file_path.exists():
                existing = pq.read_table(file_path)
                table = pa.concat_tables([existing, table])
                # Deduplicate on (fiscal_year, fiscal_quarter) keeping latest
                table = _deduplicate_earnings(table)

            pq.write_table(table, file_path, compression=self.compression)
            written.append(file_path)
            logger.info("Wrote %d earnings records to %s", len(symbol_records), file_path)

        return written

    def read_earnings(self, symbol: str) -> pa.Table:
        """Read all earnings records for a symbol."""
        file_path = self.base_dir / "earnings" / f"{symbol}.parquet"
        if not file_path.exists():
            return pa.table({})
        return pq.read_table(file_path)


def _deduplicate_earnings(table: pa.Table) -> pa.Table:
    """Keep only the last row for each (fiscal_year, fiscal_quarter) pair."""
    import pyarrow.compute as pc

    # Build a composite key for deduplication
    keys = pc.binary_join_element_wise(
        pc.cast(table.column("fiscal_year"), pa.string()),
        pc.cast(table.column("fiscal_quarter"), pa.string()),
        "-",
    )

    # Walk backwards, keeping first occurrence of each key (= last in original order)
    seen: set[str] = set()
    keep_indices: list[int] = []
    for i in range(len(keys) - 1, -1, -1):
        key = keys[i].as_py()
        if key not in seen:
            seen.add(key)
            keep_indices.append(i)

    keep_indices.reverse()
    return table.take(keep_indices)
