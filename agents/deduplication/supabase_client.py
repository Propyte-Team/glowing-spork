"""
Cliente Supabase para el agente de deduplicacion.
Patron extraido de enrichment_agent_v2/supabase_writer.py.

Usa Management API SQL para escrituras y REST API para lecturas.
"""

import asyncio
import logging
import os

import httpx

from .config import (
    SUPABASE_URL,
    SUPABASE_HEADERS,
    SUPABASE_MGMT_TOKEN,
    MGMT_API_URL,
)

logger = logging.getLogger("dedup.supabase")

_sql_semaphore = asyncio.Semaphore(1)
_SQL_DELAY = 1.2  # segundos entre llamadas a Management API (~60/min)


async def execute_sql(client: httpx.AsyncClient, query: str) -> dict | list | None:
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
                if resp.status_code == 429:
                    wait = 5 * (attempt + 1)
                    logger.warning(f"Rate limited (429), esperando {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                if resp.status_code >= 400:
                    logger.error(f"SQL error {resp.status_code}: {resp.text[:500]}")
                    return None
                await asyncio.sleep(_SQL_DELAY)
                return resp.json()
            except Exception as e:
                logger.error(f"SQL exception: {e}")
                return None
        logger.error("SQL fallo despues de 3 reintentos (rate limited)")
        return None


async def execute_sql_file(client: httpx.AsyncClient, filepath: str) -> bool:
    """Lee un archivo .sql y lo ejecuta via Management API."""
    if not os.path.exists(filepath):
        logger.error(f"Archivo SQL no encontrado: {filepath}")
        return False

    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    if not sql.strip():
        logger.warning(f"Archivo SQL vacio: {filepath}")
        return True

    result = await execute_sql(client, sql)
    return result is not None


def _escape_sql(value: str) -> str:
    """Escapa comillas simples para SQL."""
    return str(value).replace("'", "''")


async def rest_get(
    client: httpx.AsyncClient,
    table: str,
    select: str = "*",
    filters: dict | None = None,
    limit: int = 1000,
    offset: int = 0,
) -> list[dict]:
    """Lee registros via REST API GET."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {
        "select": select,
        "limit": str(limit),
        "offset": str(offset),
    }
    if filters:
        params.update(filters)

    try:
        resp = await client.get(
            url,
            headers={**SUPABASE_HEADERS, "Prefer": "count=exact"},
            params=params,
            timeout=30,
        )
        if resp.status_code >= 400:
            logger.error(f"REST GET {table} error {resp.status_code}: {resp.text[:300]}")
            return []
        return resp.json()
    except Exception as e:
        logger.error(f"REST GET {table} exception: {e}")
        return []
