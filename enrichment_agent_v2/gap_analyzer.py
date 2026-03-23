"""
Analiza brechas de datos en Supabase y prioriza registros para enriquecimiento.
"""

import json
import logging
import os
from datetime import datetime

import httpx

from .config import ENRICHMENT_PRIORITY, BATCH_SIZE
from .supabase_writer import execute_sql, get_developments, get_coverage_stats, get_portal_coverage

logger = logging.getLogger("enrichment_v2.gap_analyzer")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


async def analyze_full_coverage(client: httpx.AsyncClient) -> dict:
    """Analiza cobertura completa y genera enrichment_gaps.json."""
    stats = await get_coverage_stats(client)
    if not stats:
        logger.error("No se pudo obtener estadísticas de cobertura")
        return {}

    total = int(stats.get("total", 0))
    if total == 0:
        return {"total_developments": 0, "gaps": {}}

    field_map = {
        "price_min_mxn": "con_precio_min",
        "price_max_mxn": "con_precio_max",
        "lat": "con_lat",
        "lng": "con_lng",
        "description_es": "con_descripcion",
        "images": "con_imagenes",
        "developer_id": "con_developer",
        "contact_phone": "con_telefono",
        "source_url": "con_url_fuente",
        "amenities": "con_amenidades",
        "total_units": "con_num_unidades",
        "delivery_text": "con_fecha_entrega",
        "zone": "con_zona",
    }

    gaps = {}
    for field, stat_key in field_map.items():
        con = int(stats.get(stat_key, 0))
        faltantes = total - con
        pct = round(faltantes / total * 100) if total > 0 else 0

        if field in ("price_min_mxn", "price_max_mxn", "lat", "lng", "images"):
            prioridad = "ALTA" if pct > 30 else "MEDIA"
        elif field in ("description_es", "amenities"):
            prioridad = "MEDIA"
        else:
            prioridad = "BAJA" if pct < 20 else "MEDIA"

        gaps[field] = {
            "existentes": con,
            "faltantes": faltantes,
            "pct_faltante": pct,
            "prioridad": prioridad,
        }

    # Registros con URL pero sin precio (candidatos prioritarios para scraping)
    url_sin_precio_query = """
    SELECT COUNT(*) as total
    FROM public.developments
    WHERE deleted_at IS NULL
      AND source_url IS NOT NULL AND source_url != ''
      AND price_min_mxn IS NULL
    """
    result = await execute_sql(client, url_sin_precio_query)
    url_sin_precio = 0
    if result and isinstance(result, list) and len(result) > 0:
        url_sin_precio = int(result[0].get("total", 0))

    # Cobertura por portal
    portal_coverage = await get_portal_coverage(client)
    portales_con_brecha = []
    for p in portal_coverage:
        registros = int(p.get("registros", 0))
        con_precio = int(p.get("con_precio", 0))
        if registros > 10 and con_precio / registros < 0.3:
            portales_con_brecha.append({
                "portal": p.get("portal"),
                "registros": registros,
                "pct_sin_precio": round((1 - con_precio / registros) * 100),
            })

    report = {
        "total_developments": total,
        "gaps": gaps,
        "registros_con_url_sin_precio": url_sin_precio,
        "portales_con_mayor_brecha": portales_con_brecha,
        "portal_coverage": portal_coverage,
        "generated_at": datetime.now().isoformat(),
    }

    # Guardar JSON
    out_path = os.path.join(BASE_DIR, "enrichment_gaps.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info(f"Gaps report saved to {out_path}")

    return report


async def get_geocoding_candidates(
    client: httpx.AsyncClient, limit: int = BATCH_SIZE, ciudad: str | None = None
) -> list[dict]:
    """Registros con city+state pero sin lat/lng."""
    query = (
        "SELECT id, name, city, state, zone "
        "FROM public.developments "
        "WHERE deleted_at IS NULL "
        "AND lat IS NULL "
        "AND city IS NOT NULL AND city != '' "
    )
    if ciudad:
        query += f"AND city ILIKE '%{ciudad}%' "
    query += f"ORDER BY RANDOM() LIMIT {limit}"

    result = await execute_sql(client, query)
    if result and isinstance(result, list):
        return result
    return []


async def get_scraping_candidates(
    client: httpx.AsyncClient, limit: int = BATCH_SIZE, portal: str | None = None
) -> list[dict]:
    """Registros con source_url pero datos faltantes (precio, descripción, etc.)."""
    query = (
        "SELECT id, name, city, state, source_url, detection_source, "
        "price_min_mxn, description_es, images, contact_phone, amenities, "
        "total_units, delivery_text "
        "FROM public.developments "
        "WHERE deleted_at IS NULL "
        "AND source_url IS NOT NULL AND source_url != '' "
        "AND (price_min_mxn IS NULL OR description_es IS NULL OR images IS NULL) "
    )
    if portal:
        query += f"AND detection_source ILIKE '%{portal}%' "
    query += f"ORDER BY RANDOM() LIMIT {limit}"

    result = await execute_sql(client, query)
    if result and isinstance(result, list):
        return result
    return []


async def get_search_candidates(
    client: httpx.AsyncClient, limit: int = BATCH_SIZE, ciudad: str | None = None
) -> list[dict]:
    """Registros sin source_url y sin precio (necesitan web search)."""
    query = (
        "SELECT id, name, city, state, zone, detection_source "
        "FROM public.developments "
        "WHERE deleted_at IS NULL "
        "AND price_min_mxn IS NULL "
        "AND (source_url IS NULL OR source_url = '') "
    )
    if ciudad:
        query += f"AND city ILIKE '%{ciudad}%' "
    query += f"ORDER BY RANDOM() LIMIT {limit}"

    result = await execute_sql(client, query)
    if result and isinstance(result, list):
        return result
    return []


async def get_ai_candidates(
    client: httpx.AsyncClient, limit: int = BATCH_SIZE
) -> list[dict]:
    """Registros sin descripción (para generación con AI)."""
    query = (
        "SELECT id, name, city, state, zone, stage, "
        "price_min_mxn, price_max_mxn, total_units, "
        "amenities, detection_source, developer_id "
        "FROM public.developments "
        "WHERE deleted_at IS NULL "
        "AND description_es IS NULL "
        "AND name IS NOT NULL AND name != '' "
        f"ORDER BY RANDOM() LIMIT {limit}"
    )

    result = await execute_sql(client, query)
    if result and isinstance(result, list):
        return result
    return []


def calculate_priority(dev: dict) -> int:
    """Calcula score de prioridad para un desarrollo."""
    score = 0
    if dev.get("price_min_mxn") is None:
        score += 3
    if dev.get("description_es") is None:
        score += 2
    if dev.get("lat") is None:
        score += 1
    if dev.get("images") is None:
        score += 1
    if dev.get("source_url"):
        score += 2  # Tiene URL = más fácil de enriquecer
    return score


def get_missing_fields(dev: dict) -> list[str]:
    """Retorna lista de campos faltantes de un desarrollo."""
    check_fields = [
        "price_min_mxn", "price_max_mxn", "lat", "lng",
        "description_es", "images", "contact_phone",
        "amenities", "total_units", "delivery_text",
    ]
    return [f for f in check_fields if dev.get(f) is None]
