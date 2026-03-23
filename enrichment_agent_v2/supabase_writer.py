"""
Supabase client: REST API reads + Management API SQL writes.

public.developments es una VIEW — REST API PATCH no funciona.
Writes van via Management API SQL (POST /v1/projects/{ref}/database/query).
"""

import asyncio
import json
import logging
from datetime import datetime

import httpx

from .config import (
    SUPABASE_URL,
    SUPABASE_SERVICE_KEY,
    SUPABASE_MGMT_TOKEN,
    SUPABASE_PROJECT_REF,
    SUPABASE_HEADERS,
)

logger = logging.getLogger("enrichment_v2.supabase")

MGMT_API_URL = f"https://api.supabase.com/v1/projects/{SUPABASE_PROJECT_REF}/database/query"

ENRICHMENT_LOG_DDL = """
CREATE TABLE IF NOT EXISTS public.enrichment_log (
    id SERIAL PRIMARY KEY,
    development_id UUID NOT NULL,
    development_name TEXT,
    field_name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    strategy TEXT NOT NULL,
    source_url TEXT,
    portal TEXT,
    success BOOLEAN DEFAULT true,
    error_msg TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_dev_id
    ON public.enrichment_log(development_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_created
    ON public.enrichment_log(created_at);
"""


# =============================================================================
# Management API SQL (writes)
# =============================================================================

_sql_semaphore = asyncio.Semaphore(1)  # One SQL request at a time
_SQL_DELAY = 1.2  # seconds between Management API calls (rate limit ~60/min)


async def execute_sql(client: httpx.AsyncClient, query: str) -> dict | None:
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
                    logger.warning(f"Rate limited (429), waiting {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                if resp.status_code >= 400:
                    logger.error(f"SQL error {resp.status_code}: {resp.text[:300]}")
                    return None
                await asyncio.sleep(_SQL_DELAY)
                return resp.json()
            except Exception as e:
                logger.error(f"SQL exception: {e}")
                return None
        logger.error("SQL failed after 3 retries (rate limited)")
        return None


def _escape_sql(value: str) -> str:
    """Escapa comillas simples para SQL."""
    return str(value).replace("'", "''")


def build_set_clause(data: dict) -> str:
    """Construye SET clause para UPDATE SQL. Porta buildSetClause() de enrich-agent.js."""
    parts = []
    for key, value in data.items():
        if value is None:
            parts.append(f'"{key}" = NULL')
        elif isinstance(value, bool):
            parts.append(f'"{key}" = {str(value).lower()}')
        elif isinstance(value, (int, float)):
            parts.append(f'"{key}" = {value}')
        elif isinstance(value, list):
            if len(value) == 0:
                parts.append(f"\"{key}\" = '{{}}'::text[]")
            else:
                items = ",".join(f"'{_escape_sql(x)}'" for x in value)
                parts.append(f'"{key}" = ARRAY[{items}]::text[]')
        else:
            parts.append(f"\"{key}\" = '{_escape_sql(value)}'")
    return ", ".join(parts)


# =============================================================================
# REST API (reads)
# =============================================================================

async def get_developments(
    client: httpx.AsyncClient,
    select: str = "*",
    filters: str = "",
    limit: int = 1000,
    offset: int = 0,
) -> list[dict]:
    """Lee desarrollos via REST API GET."""
    url = f"{SUPABASE_URL}/rest/v1/developments"
    params = {
        "select": select,
        "limit": str(limit),
        "offset": str(offset),
        "deleted_at": "is.null",
    }
    if filters:
        for f in filters.split("&"):
            if "=" in f:
                k, v = f.split("=", 1)
                params[k] = v

    resp = await client.get(
        url,
        headers={**SUPABASE_HEADERS, "Prefer": "count=exact"},
        params=params,
        timeout=30,
    )
    if resp.status_code >= 400:
        logger.error(f"GET developments error {resp.status_code}: {resp.text[:300]}")
        return []
    return resp.json()


async def get_total_count(client: httpx.AsyncClient, filters: str = "") -> int:
    """Obtiene conteo total de desarrollos."""
    url = f"{SUPABASE_URL}/rest/v1/developments"
    params = {"select": "id", "limit": "1", "deleted_at": "is.null"}
    if filters:
        for f in filters.split("&"):
            if "=" in f:
                k, v = f.split("=", 1)
                params[k] = v

    resp = await client.get(
        url,
        headers={**SUPABASE_HEADERS, "Prefer": "count=exact"},
        params=params,
        timeout=15,
    )
    if resp.status_code >= 400:
        return 0
    content_range = resp.headers.get("content-range", "")
    if "/" in content_range:
        try:
            return int(content_range.split("/")[1])
        except (ValueError, IndexError):
            pass
    return len(resp.json())


async def get_developers(
    client: httpx.AsyncClient, name: str
) -> list[dict]:
    """Busca desarrolladores por nombre."""
    url = f"{SUPABASE_URL}/rest/v1/developers"
    resp = await client.get(
        url,
        headers=SUPABASE_HEADERS,
        params={
            "select": "id,name",
            "name": f"ilike.%{name}%",
            "limit": "1",
        },
        timeout=15,
    )
    if resp.status_code >= 400:
        return []
    return resp.json()


# =============================================================================
# Write operations
# =============================================================================

async def ensure_enrichment_log_table(client: httpx.AsyncClient) -> bool:
    """Crea tabla enrichment_log si no existe."""
    result = await execute_sql(client, ENRICHMENT_LOG_DDL)
    if result is not None:
        logger.info("enrichment_log table ensured")
        return True
    logger.error("Failed to create enrichment_log table")
    return False


async def update_development(
    client: httpx.AsyncClient, dev_id: str, updates: dict
) -> bool:
    """Actualiza un desarrollo via Management API SQL."""
    if not updates:
        return True
    set_clause = build_set_clause(updates)
    query = (
        f"UPDATE public.developments "
        f"SET {set_clause}, updated_at = NOW() "
        f"WHERE id = '{_escape_sql(dev_id)}'"
    )
    result = await execute_sql(client, query)
    return result is not None


async def batch_update_developments(
    client: httpx.AsyncClient,
    updates: list[dict],
    chunk_size: int = 50,
) -> int:
    """Batch update de desarrollos. Cada item: {"id": str, "data": dict}.
    Retorna número de updates exitosos."""
    if not updates:
        return 0
    success = 0
    for i in range(0, len(updates), chunk_size):
        chunk = updates[i : i + chunk_size]
        statements = []
        for item in chunk:
            set_clause = build_set_clause(item["data"])
            stmt = (
                f"UPDATE public.developments "
                f"SET {set_clause}, updated_at = NOW() "
                f"WHERE id = '{_escape_sql(item['id'])}'"
            )
            statements.append(stmt)
        query = ";\n".join(statements)
        result = await execute_sql(client, query)
        if result is not None:
            success += len(chunk)
        else:
            logger.error(f"Batch update failed for chunk starting at {i}")
    return success


async def log_enrichment(
    client: httpx.AsyncClient,
    dev_id: str,
    dev_name: str,
    field: str,
    new_value: str,
    strategy: str,
    source_url: str | None = None,
    portal: str | None = None,
    success: bool = True,
    error_msg: str | None = None,
) -> bool:
    """Registra un enriquecimiento en enrichment_log."""
    values = {
        "development_id": dev_id,
        "development_name": _escape_sql(dev_name or ""),
        "field_name": _escape_sql(field),
        "new_value": _escape_sql(str(new_value)[:500]) if new_value else None,
        "strategy": _escape_sql(strategy),
        "source_url": _escape_sql(source_url) if source_url else None,
        "portal": _escape_sql(portal) if portal else None,
        "success": success,
        "error_msg": _escape_sql(error_msg) if error_msg else None,
    }

    cols = []
    vals = []
    for k, v in values.items():
        cols.append(f'"{k}"')
        if v is None:
            vals.append("NULL")
        elif isinstance(v, bool):
            vals.append(str(v).lower())
        else:
            vals.append(f"'{v}'")

    query = (
        f"INSERT INTO public.enrichment_log ({', '.join(cols)}) "
        f"VALUES ({', '.join(vals)})"
    )
    result = await execute_sql(client, query)
    return result is not None


async def get_coverage_stats(client: httpx.AsyncClient) -> dict:
    """Obtiene estadísticas de cobertura de campos."""
    query = """
    SELECT
        COUNT(*) as total,
        COUNT(name) as con_nombre,
        COUNT(city) as con_ciudad,
        COUNT(state) as con_estado,
        COUNT(price_min_mxn) as con_precio_min,
        COUNT(price_max_mxn) as con_precio_max,
        COUNT(lat) as con_lat,
        COUNT(lng) as con_lng,
        COUNT(description_es) as con_descripcion,
        COUNT(images) as con_imagenes,
        COUNT(developer_id) as con_developer,
        COUNT(contact_phone) as con_telefono,
        COUNT(source_url) as con_url_fuente,
        COUNT(amenities) as con_amenidades,
        COUNT(total_units) as con_num_unidades,
        COUNT(delivery_text) as con_fecha_entrega,
        COUNT(zone) as con_zona
    FROM public.developments
    WHERE deleted_at IS NULL
    """
    result = await execute_sql(client, query)
    if result and isinstance(result, list) and len(result) > 0:
        return result[0]
    return {}


async def get_portal_coverage(client: httpx.AsyncClient) -> list[dict]:
    """Obtiene cobertura por portal fuente."""
    query = """
    SELECT
        detection_source as portal,
        COUNT(*) as registros,
        COUNT(price_min_mxn) as con_precio,
        COUNT(lat) as con_coords,
        COUNT(images) as con_imagen,
        COUNT(description_es) as con_descripcion
    FROM public.developments
    WHERE deleted_at IS NULL
    GROUP BY detection_source
    ORDER BY COUNT(*) DESC
    """
    result = await execute_sql(client, query)
    if result and isinstance(result, list):
        return result
    return []
