# Stock Data Tracking

Automated US stock market data collection system. Tracks 1-minute OHLCV bars, daily option chain snapshots, and quarterly earnings for 500+ symbols.

## Data Types

| Data | Source | Schedule | Storage |
|------|--------|----------|---------|
| **Tick (1-min OHLCV)** | Polygon.io / yfinance | Every minute during market hours (9:30 AM - 4:00 PM ET) | Parquet + PostgreSQL |
| **Option Chains** | Polygon.io / yfinance | Daily at 4:30 PM ET | Parquet + PostgreSQL |
| **Earnings** | Polygon.io / yfinance | Weekly (Sunday 10 AM ET) | Parquet + PostgreSQL |

## Setup

### Prerequisites

- Python 3.11+
- PostgreSQL
- [Polygon.io API key](https://polygon.io/) (free tier works, paid recommended for 500+ symbols)

### Installation

```bash
pip install -e ".[dev]"
```

### Configuration

1. Copy the environment template:
   ```bash
   cp .env.example .env
   ```

2. Fill in your credentials in `.env`:
   ```
   POLYGON_API_KEY=your_key_here
   POSTGRES_DSN=postgresql://user:password@localhost:5432/stockdata
   ```

3. Edit `config/settings.yaml` to configure your symbol watchlist and schedule preferences.

### Database Setup

Create the PostgreSQL database:

```bash
createdb stockdata
```

Tables are created automatically on first run.

## Usage

### Run the Scheduler (all jobs)

```bash
python -m src.main
```

This starts the APScheduler with all three collection jobs running on their configured cron schedules.

### Run Individual Collectors

```bash
# Fetch tick data for today
python scripts/fetch_tick_data.py

# Fetch tick data for a specific date
python scripts/fetch_tick_data.py 2026-03-27

# Fetch tick data for a specific date and symbol
python scripts/fetch_tick_data.py 2026-03-27 AAPL

# Fetch option chains
python scripts/fetch_options.py
python scripts/fetch_options.py 2026-03-27 AAPL

# Fetch earnings
python scripts/fetch_earnings.py
python scripts/fetch_earnings.py AAPL
```

## Project Structure

```
config/             Configuration (YAML)
src/
  collectors/       Data fetchers (tick, options, earnings)
  storage/          Parquet + PostgreSQL write/read
  models/           Pydantic schemas
  utils/            Rate limiter
  scheduler.py      APScheduler cron job setup
  main.py           Entry point
scripts/            Standalone CLI scripts
tests/              Unit tests
data/               Local Parquet files (gitignored)
```

## Storage

- **Parquet files** are partitioned by date: `data/tick/2026-03-30/AAPL.parquet`
- **PostgreSQL** tables: `tick_data`, `option_chains`, `earnings`
- All inserts are idempotent (ON CONFLICT DO NOTHING / DO UPDATE)

## Rate Limiting

- Free Polygon tier: 5 API calls/minute. At this rate, batch-collecting 500 symbols takes ~100 minutes (post-market).
- Paid tier: unlimited calls. Enables parallel real-time collection.
- After 3 consecutive HTTP 429 responses, the system automatically falls back to yfinance.

## Tests

```bash
pytest -v
```

## Known Limitations

- **Earnings estimates**: Polygon provides actuals from SEC filings. Consensus estimates (EPS/revenue) require a separate data source (e.g., Benzinga). The schema fields are ready.
- **yfinance 1-min history**: Only 7 days of 1-minute data available as fallback.
- **Real-time streaming**: WebSocket-based real-time tick collection is not yet implemented (Polygon WebSocket is available on paid plans).
