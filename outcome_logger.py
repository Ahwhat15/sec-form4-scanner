"""
outcome_logger.py — VMc1 signal and trade outcome logger for Supabase.
Shared across all VMc1 services. DO NOT REMOVE — feeds Hermes intelligence layer.
"""
import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
import aiohttp

log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
RAILWAY_SERVICE_NAME = os.environ.get("VMC1_SERVICE_NAME", "unknown")


class OutcomeLogger:
    def __init__(self, service_name: str = RAILWAY_SERVICE_NAME):
        self.service_name = service_name
        self._service_id: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None

    def _headers(self):
        return {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._headers())
        return self._session

    async def _get_service_id(self) -> Optional[str]:
        if self._service_id:
            return self._service_id
        try:
            session = await self._get_session()
            url = f"{SUPABASE_URL}/rest/v1/services"
            params = {"railway_service_name": f"eq.{self.service_name}", "select": "id"}
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        self._service_id = data[0]["id"]
                        return self._service_id
        except Exception as e:
            log.debug(f"Service ID lookup failed: {e}")
        return None

    async def _post(self, table: str, payload: dict) -> Optional[dict]:
        try:
            session = await self._get_session()
            url = f"{SUPABASE_URL}/rest/v1/{table}"
            async with session.post(url, json=payload) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    return data[0] if isinstance(data, list) else data
                log.debug(f"POST {table} failed: {resp.status}")
        except Exception as e:
            log.debug(f"POST {table} error: {e}")
        return None

    async def log_signal(self, symbol: str, signal_type: str, trade_taken: bool = False,
                         score: float = 0.0, signal_detail: dict = None,
                         skip_reason: str = None) -> Optional[dict]:
        if not SUPABASE_URL or not SUPABASE_KEY:
            return None
        service_id = await self._get_service_id()
        if not service_id:
            return None
        payload = {
            "service_id": service_id,
            "symbol": symbol,
            "signal_type": signal_type,
            "trade_taken": trade_taken,
            "score": score,
            "signal_detail": signal_detail or {},
            "skip_reason": skip_reason,
            "fired_at": datetime.now(timezone.utc).isoformat(),
        }
        return await self._post("signal_events", payload)

    async def log_trade(self, symbol: str, direction: str, entry_price: float,
                        exit_price: float, position_size: float, pnl_usd: float,
                        pnl_pct: float, entry_at, exit_at, entry_signal: str = "",
                        exit_reason: str = "") -> Optional[dict]:
        if not SUPABASE_URL or not SUPABASE_KEY:
            return None
        service_id = await self._get_service_id()
        if not service_id:
            return None
        payload = {
            "service_id": service_id,
            "symbol": symbol,
            "direction": direction.lower(),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "position_size": position_size,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "entry_at": entry_at.isoformat() if hasattr(entry_at, 'isoformat') else str(entry_at),
            "exit_at": exit_at.isoformat() if hasattr(exit_at, 'isoformat') else str(exit_at),
            "entry_signal": entry_signal,
            "exit_reason": exit_reason,
        }
        return await self._post("trade_outcomes", payload)

    def log_signal_sync(self, **kwargs) -> Optional[dict]:
        """Sync wrapper — works in both sync and async contexts."""
        try:
            asyncio.get_running_loop()
            # Already in async context — run in thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, self.log_signal(**kwargs))
                return future.result(timeout=10)
        except RuntimeError:
            # No running loop — safe to call directly
            return asyncio.run(self.log_signal(**kwargs))

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


_default_logger: Optional[OutcomeLogger] = None


def get_logger(service_name: str = RAILWAY_SERVICE_NAME) -> OutcomeLogger:
    global _default_logger
    if _default_logger is None:
        _default_logger = OutcomeLogger(service_name)
    return _default_logger
