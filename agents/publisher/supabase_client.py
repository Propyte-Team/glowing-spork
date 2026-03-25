"""
Cliente Supabase Management API para el Publisher Agent.
Mismo patrón que enrichment_agent_v2/supabase_writer.py.
"""

import asyncio
import logging
import httpx
from .config import SUPABASE_MGMT_TOKEN, MGMT_API_URL

logger = logging.getLogger(__name__)

_sql_semaphore = asyncio.Semaphore(1)
_SQL_DELAY = 1.2  # segundos entre llamadas (rate limit ~60/min)


async def execute_sql(client: httpx.AsyncClient, query: str) -> list | None:
    """Ejecuta SQL via Supabase Management API con rate limiting."""
    async with _sql_semaphore:
        for attempt in range(3):
            try:
                resp = await client.post(
                    MGMT_API_URL,
                    headers={
                        "Authorization": f"Bearer {SUPABASE_MGMT_TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json={"query": query},
                    timeout=30,
                )
                if resp.status_code == 200:
                    await asyncio.sleep(_SQL_DELAY)
                    return resp.json()
                elif resp.status_code == 429:
                    wait = 2 ** attempt * 2
                    logger.warning(f"Rate limit SQL, retrying in {wait}s")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"SQL error {resp.status_code}: {resp.text[:300]}")
                    return None
            except Exception as e:
                logger.error(f"SQL attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
        return None
