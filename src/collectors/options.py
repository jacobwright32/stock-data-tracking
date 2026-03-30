"""Collector for option chain snapshots.

Fetches full option chains (calls + puts) for each symbol from Polygon.io,
with yfinance as a fallback. Runs daily after market close.
"""

import asyncio
import logging
from datetime import date, datetime, timezone
from decimal import Decimal

from src.collectors.base import BaseCollector
from src.models.schemas import OptionChainSnapshot, OptionContract, OptionGreeks, OptionType

logger = logging.getLogger(__name__)


class OptionsCollector(BaseCollector):
    """Fetches daily option chain snapshots for all configured symbols."""

    async def collect(self, target_date: date | None = None) -> None:
        """Fetch full option chains for all symbols."""
        target_date = target_date or date.today()
        logger.info("Collecting option chains for %s (%d symbols)", target_date, len(self.symbols))

        for symbol in self.symbols:
            if self.rate_limiter.should_fallback:
                logger.warning("Rate limit hit, falling back to yfinance for %s options", symbol)
                contracts = await self._fetch_yfinance_fallback(symbol, target_date=target_date)
            else:
                async with self.rate_limiter:
                    contracts = await self._fetch_polygon(symbol, target_date=target_date)

            if contracts:
                snapshot = OptionChainSnapshot(
                    underlying_symbol=symbol,
                    snapshot_date=target_date,
                    contracts=contracts,
                    fetch_timestamp=datetime.now(timezone.utc),
                )
                self.parquet_store.write_option_chain(snapshot.contracts, target_date)
                await self.postgres_store.insert_option_contracts(snapshot.contracts, target_date)
                logger.info("Stored %d contracts for %s", len(contracts), symbol)
            else:
                logger.warning("No option contracts for %s on %s", symbol, target_date)

    async def _fetch_polygon(self, symbol: str, **kwargs: object) -> list[OptionContract]:
        """Fetch option chain snapshot from Polygon.io.

        GET /v3/snapshot/options/{symbol}
        Handles pagination via next_url.
        """
        contracts: list[OptionContract] = []
        url = f"/v3/snapshot/options/{symbol}"
        params: dict = {"limit": 250}
        now = datetime.now(timezone.utc)

        while url:
            try:
                resp = await self.client.get(url, params=params)
            except Exception as e:
                logger.error("HTTP error fetching options for %s: %s", symbol, e)
                break

            if resp.status_code == 429:
                self.rate_limiter.record_429()
                logger.warning("Rate limited (429) fetching options for %s", symbol)
                break

            if resp.status_code != 200:
                logger.error(
                    "Unexpected status %d for %s options: %s",
                    resp.status_code, symbol, resp.text,
                )
                break

            self.rate_limiter.record_success()
            data = resp.json()

            for r in data.get("results", []):
                try:
                    details = r.get("details", {})
                    greeks_data = r.get("greeks")
                    last_quote = r.get("last_quote", {})
                    day = r.get("day", {})

                    contract_type = details.get("contract_type", "").lower()
                    if contract_type not in ("call", "put"):
                        continue

                    contracts.append(OptionContract(
                        underlying_symbol=symbol,
                        contract_ticker=details.get("ticker", ""),
                        option_type=OptionType(contract_type),
                        strike_price=Decimal(str(details.get("strike_price", 0))),
                        expiration_date=details.get("expiration_date"),
                        bid=_safe_decimal(last_quote.get("bid")),
                        ask=_safe_decimal(last_quote.get("ask")),
                        mid=_safe_decimal(last_quote.get("midpoint")),
                        last_price=_safe_decimal(r.get("last_trade", {}).get("price")),
                        volume=day.get("volume"),
                        open_interest=r.get("open_interest"),
                        implied_volatility=r.get("implied_volatility"),
                        greeks=OptionGreeks(**greeks_data) if greeks_data else None,
                        break_even_price=_safe_decimal(r.get("break_even_price")),
                        snapshot_timestamp=now,
                        source="polygon",
                    ))
                except Exception as e:
                    logger.warning("Skipping invalid option contract for %s: %s", symbol, e)

            # Follow pagination
            next_url = data.get("next_url")
            if next_url:
                url = next_url
                params = {}  # next_url already includes params
            else:
                break

        logger.debug("Fetched %d option contracts for %s from Polygon", len(contracts), symbol)
        return contracts

    async def _fetch_yfinance_fallback(
        self, symbol: str, **kwargs: object
    ) -> list[OptionContract]:
        """Fallback: fetch option chains from yfinance.

        Note: yfinance provides basic chains without greeks.
        """
        target_date = kwargs.get("target_date", date.today())

        try:
            import yfinance as yf

            loop = asyncio.get_running_loop()
            ticker = yf.Ticker(symbol)

            # Get available expiration dates
            expirations = await loop.run_in_executor(None, lambda: ticker.options)
            if not expirations:
                return []

            now = datetime.now(timezone.utc)
            contracts: list[OptionContract] = []

            for exp_date_str in expirations:
                chain = await loop.run_in_executor(
                    None, lambda exp=exp_date_str: ticker.option_chain(exp)
                )

                for option_type, df in [("call", chain.calls), ("put", chain.puts)]:
                    for _, row in df.iterrows():
                        try:
                            contracts.append(OptionContract(
                                underlying_symbol=symbol,
                                contract_ticker=row.get("contractSymbol", ""),
                                option_type=OptionType(option_type),
                                strike_price=Decimal(str(round(row["strike"], 4))),
                                expiration_date=date.fromisoformat(exp_date_str),
                                bid=_safe_decimal(row.get("bid")),
                                ask=_safe_decimal(row.get("ask")),
                                mid=None,
                                last_price=_safe_decimal(row.get("lastPrice")),
                                volume=int(row["volume"]) if row.get("volume") else None,
                                open_interest=int(row["openInterest"])
                                if row.get("openInterest")
                                else None,
                                implied_volatility=row.get("impliedVolatility"),
                                greeks=None,
                                break_even_price=None,
                                snapshot_timestamp=now,
                                source="yfinance",
                            ))
                        except Exception as e:
                            logger.warning(
                                "Skipping yfinance option for %s: %s", symbol, e
                            )

            logger.debug("Fetched %d contracts for %s from yfinance", len(contracts), symbol)
            return contracts

        except Exception as e:
            logger.error("yfinance options fallback failed for %s: %s", symbol, e)
            return []


def _safe_decimal(value: object) -> Decimal | None:
    """Convert a value to Decimal, returning None if invalid."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None
