"""Abstract base collector defining the interface all collectors must implement."""

import logging
from abc import ABC, abstractmethod
from datetime import date

import httpx

from src.storage.parquet_store import ParquetStore
from src.storage.postgres_store import PostgresStore
from src.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Base class for all data collectors.

    Provides shared infrastructure:
    - httpx.AsyncClient configured for Polygon.io
    - RateLimiter for API rate management
    - ParquetStore + PostgresStore for dual-write storage
    - Symbol list
    - Fallback interface to yfinance
    """

    def __init__(
        self,
        symbols: list[str],
        polygon_api_key: str,
        rate_limiter: RateLimiter,
        parquet_store: ParquetStore,
        postgres_store: PostgresStore,
    ) -> None:
        self.symbols = symbols
        self.api_key = polygon_api_key
        self.rate_limiter = rate_limiter
        self.parquet_store = parquet_store
        self.postgres_store = postgres_store
        self.client = httpx.AsyncClient(
            base_url="https://api.polygon.io",
            timeout=30.0,
            params={"apiKey": polygon_api_key},
        )

    @abstractmethod
    async def collect(self, target_date: date | None = None) -> None:
        """Run one collection cycle for all symbols."""

    @abstractmethod
    async def _fetch_polygon(self, symbol: str, **kwargs: object) -> list:
        """Fetch data from Polygon.io for a single symbol."""

    @abstractmethod
    async def _fetch_yfinance_fallback(self, symbol: str, **kwargs: object) -> list:
        """Fallback fetcher using yfinance when Polygon is rate-limited."""

    async def close(self) -> None:
        """Clean up the HTTP client."""
        await self.client.aclose()
        logger.info("%s closed", self.__class__.__name__)
