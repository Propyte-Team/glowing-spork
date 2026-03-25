#!/usr/bin/env python3
"""
Propyte Enrichment Runner — Agente autónomo de enriquecimiento de datos.
========================================================================
Un solo archivo que corre como servicio HTTP en Railway.
Supabase lo activa via triggers (pg_net) y cron (pg_cron).

Uso:
    python enrichment_runner.py                # Servidor HTTP (Railway)
    python enrichment_runner.py --test         # Solo 3 desarrollos, sin server
    python enrichment_runner.py --loop         # Loop local cada 5 min
    python enrichment_runner.py --dry-run      # Sin escrituras a Supabase

Endpoints:
    POST /webhook/enrich/{dev_id}   — Trigger: enriquecer 1 desarrollo
    POST /webhook/enrich-batch      — Cron: enriquecer lote de 25
    GET  /health                    — Health check
    GET  /stats                     — Estadísticas de cobertura
"""

import asyncio
import io
import json
import logging
import sys
import time
from datetime import datetime

# Fix encoding Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import os

import httpx

# --- Imports del paquete v2 existente ----------------------------------------
from enrichment_agent_v2.config import (
    SUPABASE_URL,
    SUPABASE_SERVICE_KEY,
    SUPABASE_MGMT_TOKEN,
    SUPABASE_PROJECT_REF,
    SUPABASE_HEADERS,
    HEADERS_HTTP,
    ANTHROPIC_API_KEY,
    ENRICHMENT_PRIORITY,
    AMENIDAD_MAP,
    AMENIDAD_COLUMNS,
    NOMINATIM_DELAY,
)
from enrichment_agent_v2.supabase_writer import (
    execute_sql,
    build_set_clause,
    _escape_sql,
    update_development,
)
from enrichment_agent_v2.extractors import (
    extraer_precio,
    extraer_superficie,
    extraer_num_unidades,
    extraer_amenidades_lista,
    extraer_fecha_entrega,
    extraer_contacto,
    extraer_imagen,
    extraer_coordenadas,
    extraer_json_ld,
    campo_vacio,
)
from enrichment_agent_v2.geocoder import Geocoder
from enrichment_agent_v2.ai_enricher import (
    extract_price_from_html,
    generate_description,
)
from enrichment_agent_v2.url_scraper import (
    _fetch_html,
    _identify_portal,
    _scrape_generic,
    _scrape_json_ld,
)

# === Config ==================================================================

BATCH_SIZE = 25
TEST_LIMIT = 3
LOOP_INTERVAL = 300  # 5 minutos
WEBHOOK_SECRET = os.environ.get("ENRICHMENT_WEBHOOK_SECRET", "propyte-enrich-2026")
SERVER_PORT = int(os.environ.get("PORT", "8080"))

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="[%H:%M:%S]",
)
logger = logging.getLogger("runner")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("enrichment_v2").setLevel(logging.WARNING)

# === DDL: Tablas de log ======================================================

DDL_ENRICHMENT_LOG_V2 = """
CREATE TABLE IF NOT EXISTS public.enrichment_log_v2 (
    id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    record_id     UUID NOT NULL,
    record_name   TEXT,
    record_city   TEXT,
    portal        TEXT,
    campo         TEXT NOT NULL,
    resultado     TEXT NOT NULL CHECK (resultado IN ('exito', 'fallo', 'skip')),
    estrategia    TEXT,
    valor_nuevo   TEXT,
    error_msg     TEXT,
    duracion_ms   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_elog2_record ON public.enrichment_log_v2(record_id);
CREATE INDEX IF NOT EXISTS idx_elog2_fecha ON public.enrichment_log_v2(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_elog2_resultado ON public.enrichment_log_v2(resultado);
"""

DDL_ENRICHMENT_PENDIENTES = """
CREATE TABLE IF NOT EXISTS public.enrichment_pendientes (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    record_id       UUID NOT NULL UNIQUE,
    record_name     TEXT,
    record_city     TEXT,
    portal          TEXT,
    source_url      TEXT,
    campos_faltantes TEXT[],
    razon_fallo     TEXT,
    intentos        INTEGER DEFAULT 1,
    ultimo_intento  TIMESTAMPTZ DEFAULT NOW(),
    resuelto        BOOLEAN DEFAULT FALSE,
    notas_manuales  TEXT
);
CREATE INDEX IF NOT EXISTS idx_pend_resuelto ON public.enrichment_pendientes(resuelto);
"""


async def ensure_tables(client: httpx.AsyncClient) -> bool:
    """Crea las tablas de log si no existen."""
    r1 = await execute_sql(client, DDL_ENRICHMENT_LOG_V2)
    r2 = await execute_sql(client, DDL_ENRICHMENT_PENDIENTES)
    if r1 is not None and r2 is not None:
        logger.info("Tablas enrichment_log_v2 y enrichment_pendientes OK")
        return True
    logger.error("Error creando tablas de log")
    return False


# === Fetch candidates ========================================================

PRIORITY_FIELDS = [
    "price_min_mxn", "price_max_mxn", "lat", "lng",
    "contact_phone", "description_es", "total_units",
    "delivery_text", "developer_id",
]

FETCH_BATCH_SQL = """
SELECT d.*,
  (CASE WHEN d.price_min_mxn IS NULL THEN 3 ELSE 0 END +
   CASE WHEN d.lat IS NULL THEN 2 ELSE 0 END +
   CASE WHEN d.contact_phone IS NULL THEN 1 ELSE 0 END +
   CASE WHEN d.description_es IS NULL THEN 1 ELSE 0 END +
   CASE WHEN d.total_units IS NULL THEN 1 ELSE 0 END +
   CASE WHEN d.delivery_text IS NULL THEN 1 ELSE 0 END) as null_score
FROM public.developments d
WHERE d.deleted_at IS NULL
  AND (d.price_min_mxn IS NULL OR d.lat IS NULL
       OR d.description_es IS NULL OR d.total_units IS NULL
       OR d.contact_phone IS NULL OR d.delivery_text IS NULL)
  AND d.id NOT IN (
    SELECT p.record_id FROM public.enrichment_pendientes p
    WHERE p.resuelto = false
      AND p.ultimo_intento > NOW() - INTERVAL '24 hours'
  )
ORDER BY null_score DESC, d.created_at ASC
LIMIT {limit}
"""


async def fetch_priority_batch(
    client: httpx.AsyncClient, size: int = BATCH_SIZE
) -> list[dict]:
    """Obtiene un lote de desarrollos priorizados por campos faltantes."""
    query = FETCH_BATCH_SQL.format(limit=size)
    result = await execute_sql(client, query)
    if result and isinstance(result, list):
        return result
    return []


# === Logging helpers =========================================================

async def log_enrichment(
    client: httpx.AsyncClient,
    record_id: str,
    record_name: str = "",
    record_city: str = "",
    portal: str = "",
    campo: str = "",
    resultado: str = "exito",
    estrategia: str = "",
    valor_nuevo: str = None,
    error_msg: str = None,
    duracion_ms: int = None,
):
    """Inserta un registro en enrichment_log_v2."""
    cols = ["record_id", "record_name", "record_city", "portal",
            "campo", "resultado", "estrategia"]
    vals = [
        f"'{_escape_sql(record_id)}'",
        f"'{_escape_sql(record_name or '')}'",
        f"'{_escape_sql(record_city or '')}'",
        f"'{_escape_sql(portal or '')}'",
        f"'{_escape_sql(campo)}'",
        f"'{_escape_sql(resultado)}'",
        f"'{_escape_sql(estrategia or '')}'",
    ]
    if valor_nuevo is not None:
        cols.append("valor_nuevo")
        vals.append(f"'{_escape_sql(str(valor_nuevo)[:500])}'")
    if error_msg is not None:
        cols.append("error_msg")
        vals.append(f"'{_escape_sql(str(error_msg)[:500])}'")
    if duracion_ms is not None:
        cols.append("duracion_ms")
        vals.append(str(int(duracion_ms)))

    sql = (
        f"INSERT INTO public.enrichment_log_v2 ({', '.join(cols)}) "
        f"VALUES ({', '.join(vals)})"
    )
    await execute_sql(client, sql)


async def upsert_pendiente(
    client: httpx.AsyncClient,
    record_id: str,
    record_name: str = "",
    record_city: str = "",
    portal: str = "",
    source_url: str = "",
    campos_faltantes: list[str] = None,
    razon_fallo: str = "datos_no_encontrados",
):
    """Inserta o actualiza un registro en enrichment_pendientes."""
    campos_arr = "'{}'::text[]"
    if campos_faltantes:
        items = ",".join(f"'{_escape_sql(c)}'" for c in campos_faltantes)
        campos_arr = f"ARRAY[{items}]::text[]"

    sql = f"""
    INSERT INTO public.enrichment_pendientes
        (record_id, record_name, record_city, portal, source_url,
         campos_faltantes, razon_fallo, intentos, ultimo_intento)
    VALUES (
        '{_escape_sql(record_id)}',
        '{_escape_sql(record_name or "")}',
        '{_escape_sql(record_city or "")}',
        '{_escape_sql(portal or "")}',
        '{_escape_sql(source_url or "")}',
        {campos_arr},
        '{_escape_sql(razon_fallo)}',
        1, NOW()
    )
    ON CONFLICT (record_id) DO UPDATE SET
        campos_faltantes = EXCLUDED.campos_faltantes,
        razon_fallo = EXCLUDED.razon_fallo,
        intentos = enrichment_pendientes.intentos + 1,
        ultimo_intento = NOW(),
        updated_at = NOW()
    """
    await execute_sql(client, sql)


# === Core: process one development ===========================================

def _get_null_fields(dev: dict) -> list[str]:
    """Retorna lista de campos prioritarios que son NULL."""
    nulls = []
    for field in PRIORITY_FIELDS:
        val = dev.get(field)
        if val is None or (isinstance(val, str) and val.strip() == ""):
            nulls.append(field)
        elif isinstance(val, list) and len(val) == 0:
            nulls.append(field)
    return nulls


async def _try_scrape_url(
    client: httpx.AsyncClient, dev: dict, null_fields: list[str]
) -> tuple[dict, str | None, str | None]:
    """Intenta scraping de source_url. Retorna (datos, html_text, error)."""
    url = dev.get("source_url")
    if not url or not url.startswith("http"):
        return {}, None, "sin_url"

    html = await _fetch_html(client, url)
    if not html:
        return {}, None, "url_bloqueada"

    portal = _identify_portal(url)

    # JSON-LD primero (LaHaus y similares)
    if portal in ("lahaus", "behome", "propiedades"):
        datos = _scrape_json_ld(html, url)
    else:
        datos = _scrape_generic(html, url)

    # Filtrar: solo campos que están en null_fields
    filtered = {}
    for k, v in datos.items():
        if k in null_fields and v is not None:
            filtered[k] = v

    return filtered, html, None


async def _try_geocode(
    client: httpx.AsyncClient,
    dev: dict,
    geocoder: Geocoder,
    null_fields: list[str],
) -> dict:
    """Intenta geocodificar si lat/lng están NULL."""
    if "lat" not in null_fields:
        return {}

    city = dev.get("city", "")
    state = dev.get("state", "")
    zone = dev.get("zone", "")

    if not city:
        return {}

    coords = await geocoder.geocode(client, city, state, zone)
    if coords:
        return {"lat": coords[0], "lng": coords[1]}
    return {}


async def _try_ai(
    client: httpx.AsyncClient,
    dev: dict,
    null_fields: list[str],
    html_text: str | None,
    updates_so_far: dict,
) -> dict:
    """Intenta enriquecimiento con Claude Haiku."""
    datos = {}
    name = dev.get("name", "")
    city = dev.get("city", "")

    # Extraer precio con AI si tenemos HTML pero no se encontró precio
    if "price_min_mxn" in null_fields and "price_min_mxn" not in updates_so_far:
        if html_text:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_text, "html.parser")
            visible_text = soup.get_text(separator=" ", strip=True)
            ai_prices = await extract_price_from_html(
                client, visible_text, name, city
            )
            if ai_prices:
                datos.update(ai_prices)

    # Generar descripción si aún no tiene y ya conseguimos precio
    if "description_es" in null_fields and "description_es" not in updates_so_far:
        # Merge current dev with updates for context
        merged = {**dev, **updates_so_far, **datos}
        desc = await generate_description(client, merged)
        if desc:
            datos["description_es"] = desc

    return datos


async def process_development(
    client: httpx.AsyncClient,
    dev: dict,
    geocoder: Geocoder,
    dry_run: bool = False,
) -> dict:
    """Procesa un desarrollo: scraping → geocoding → AI. Retorna resumen."""
    dev_id = dev.get("id", "")
    name = dev.get("name", "?")
    city = dev.get("city", "?")
    portal = dev.get("detection_source", "") or ""
    source_url = dev.get("source_url", "") or ""

    null_fields = _get_null_fields(dev)
    if not null_fields:
        return {"name": name, "status": "skip", "reason": "sin campos NULL"}

    t_start = time.time()
    all_updates = {}
    strategies_used = []
    errors = []

    # --- Strategy 1: URL Scraping ---
    if source_url:
        t0 = time.time()
        scrape_data, html_text, scrape_error = await _try_scrape_url(
            client, dev, null_fields
        )
        ms = int((time.time() - t0) * 1000)

        if scrape_error:
            errors.append(scrape_error)
            if not dry_run:
                for f in null_fields:
                    await log_enrichment(
                        client, dev_id, name, city, portal,
                        campo=f, resultado="fallo",
                        estrategia="scraping_url",
                        error_msg=scrape_error, duracion_ms=ms,
                    )
        else:
            strategies_used.append("scraping_url")
            for campo, valor in scrape_data.items():
                if not dry_run:
                    await log_enrichment(
                        client, dev_id, name, city, portal,
                        campo=campo, resultado="exito",
                        estrategia="scraping_url",
                        valor_nuevo=str(valor), duracion_ms=ms,
                    )
            all_updates.update(scrape_data)
    else:
        html_text = None

    # --- Strategy 2: Geocoding ---
    remaining_nulls = [f for f in null_fields if f not in all_updates]
    if "lat" in remaining_nulls:
        t0 = time.time()
        geo_data = await _try_geocode(client, dev, geocoder, remaining_nulls)
        ms = int((time.time() - t0) * 1000)

        if geo_data:
            strategies_used.append("geocoding")
            if not dry_run:
                await log_enrichment(
                    client, dev_id, name, city, portal,
                    campo="lat+lng", resultado="exito",
                    estrategia="geocoding",
                    valor_nuevo=f"{geo_data['lat']},{geo_data['lng']}",
                    duracion_ms=ms,
                )
            all_updates.update(geo_data)
        else:
            if not dry_run:
                await log_enrichment(
                    client, dev_id, name, city, portal,
                    campo="lat+lng", resultado="fallo",
                    estrategia="geocoding",
                    error_msg="no_coords_found", duracion_ms=ms,
                )

    # --- Strategy 3: AI Enrichment ---
    remaining_nulls = [f for f in null_fields if f not in all_updates]
    ai_candidates = [f for f in remaining_nulls
                     if f in ("price_min_mxn", "price_max_mxn",
                              "description_es", "total_units", "delivery_text")]
    if ai_candidates and ANTHROPIC_API_KEY:
        t0 = time.time()
        ai_data = await _try_ai(client, dev, remaining_nulls, html_text, all_updates)
        ms = int((time.time() - t0) * 1000)

        if ai_data:
            strategies_used.append("ai_claude")
            for campo, valor in ai_data.items():
                if not dry_run:
                    await log_enrichment(
                        client, dev_id, name, city, portal,
                        campo=campo, resultado="exito",
                        estrategia="ai_claude",
                        valor_nuevo=str(valor)[:200], duracion_ms=ms,
                    )
            all_updates.update(ai_data)

    # --- Write updates to Supabase ---
    total_ms = int((time.time() - t_start) * 1000)
    updated_count = 0

    if all_updates and not dry_run:
        ok = await update_development(client, dev_id, all_updates)
        if ok:
            updated_count = len(all_updates)

    # --- Check if ALL fields still missing → pendientes ---
    final_remaining = [f for f in null_fields if f not in all_updates]
    if len(all_updates) == 0 and not dry_run:
        razon = "sin_url" if not source_url else (
            errors[0] if errors else "datos_no_encontrados"
        )
        await upsert_pendiente(
            client, dev_id, name, city, portal, source_url,
            campos_faltantes=final_remaining,
            razon_fallo=razon,
        )

    # --- Console output ---
    parts = []
    if "price_min_mxn" in all_updates:
        p_min = all_updates.get("price_min_mxn", 0)
        p_max = all_updates.get("price_max_mxn", p_min)
        if p_min and int(p_min) >= 1_000_000:
            parts.append(f"precio: ${int(p_min)/1e6:.1f}M-${int(p_max)/1e6:.1f}M")
        elif p_min:
            parts.append(f"precio: ${p_min:,}")
    if "lat" in all_updates:
        parts.append(f"coords: {all_updates['lat']:.2f},{all_updates['lng']:.2f}")
    if "images" in all_updates:
        parts.append("imagen OK")
    if "description_es" in all_updates:
        parts.append("desc OK")
    if "contact_phone" in all_updates:
        parts.append(f"tel: {all_updates['contact_phone']}")
    if "total_units" in all_updates:
        parts.append(f"unidades: {all_updates['total_units']}")

    detail = ", ".join(parts) if parts else "sin datos nuevos"
    portal_tag = f" [{portal}]" if portal else ""

    if updated_count > 0 and len(final_remaining) == 0:
        icon = "\u2705"  # ✅
    elif updated_count > 0:
        icon = "\u26a0\ufe0f"   # ⚠️
    else:
        icon = "\u274c"  # ❌

    status = "completo" if len(final_remaining) == 0 else (
        "parcial" if updated_count > 0 else "pendiente"
    )

    if dry_run:
        logger.info(f"[DRY-RUN] {icon} {name} ({city}) \u2192 {detail}{portal_tag}")
    else:
        logger.info(f"{icon} {name} ({city}) \u2192 {detail}{portal_tag}")

    return {
        "id": dev_id,
        "name": name,
        "city": city,
        "status": status,
        "fields_updated": list(all_updates.keys()),
        "fields_remaining": final_remaining,
        "strategies": strategies_used,
        "duration_ms": total_ms,
    }


# === Main loop ===============================================================

async def run_cycle(
    client: httpx.AsyncClient,
    geocoder: Geocoder,
    cycle_num: int,
    limit: int = BATCH_SIZE,
    dry_run: bool = False,
) -> dict:
    """Ejecuta un ciclo de enriquecimiento."""
    batch = await fetch_priority_batch(client, limit)
    if not batch:
        logger.info(f"Ciclo #{cycle_num}: sin desarrollos pendientes")
        return {"total": 0, "exitos": 0, "parciales": 0, "pendientes": 0}

    logger.info(f"Iniciando ciclo #{cycle_num} \u2014 {len(batch)} desarrollos en cola")

    exitos = 0
    parciales = 0
    pendientes = 0

    for i, dev in enumerate(batch):
        prefix = f"[{i+1}/{len(batch)}]"
        try:
            result = await process_development(client, dev, geocoder, dry_run)
            s = result.get("status", "")
            if s == "completo":
                exitos += 1
            elif s == "parcial":
                parciales += 1
            else:
                pendientes += 1
        except Exception as e:
            logger.error(f"{prefix} Error procesando {dev.get('name', '?')}: {e}")
            pendientes += 1

    logger.info(
        f"Ciclo #{cycle_num} completo: "
        f"{exitos} \u00e9xitos, {parciales} parciales, {pendientes} pendientes"
    )

    # Guardar cache de geocoding
    geocoder._save_cache()

    return {
        "total": len(batch),
        "exitos": exitos,
        "parciales": parciales,
        "pendientes": pendientes,
    }


async def run_forever(dry_run: bool = False):
    """Loop principal: ciclos cada 5 minutos."""
    async with httpx.AsyncClient(timeout=30) as client:
        # Crear tablas
        if not await ensure_tables(client):
            logger.error("No se pudieron crear tablas de log. Abortando.")
            return

        geocoder = Geocoder()
        cycle = 1

        while True:
            try:
                await run_cycle(client, geocoder, cycle, BATCH_SIZE, dry_run)
            except Exception as e:
                logger.error(f"Error en ciclo #{cycle}: {e}")

            cycle += 1
            logger.info(f"Pr\u00f3ximo ciclo en {LOOP_INTERVAL // 60} minutos...")
            await asyncio.sleep(LOOP_INTERVAL)


async def run_test(dry_run: bool = False):
    """Modo test: procesar 3 desarrollos y mostrar resultados."""
    async with httpx.AsyncClient(timeout=30) as client:
        if not await ensure_tables(client):
            logger.error("No se pudieron crear tablas de log.")
            return

        geocoder = Geocoder()
        batch = await fetch_priority_batch(client, TEST_LIMIT)

        if not batch:
            logger.info("No hay desarrollos pendientes de enriquecimiento.")
            return

        logger.info(f"Modo test: procesando {len(batch)} desarrollos\n")

        results = []
        for dev in batch:
            result = await process_development(client, dev, geocoder, dry_run)
            results.append(result)

        print("\n" + "=" * 60)
        print("RESULTADOS TEST")
        print("=" * 60)
        for r in results:
            print(json.dumps(r, indent=2, ensure_ascii=False))
        print("=" * 60)

        geocoder._save_cache()
        print(f"\n\u2705 Test OK \u2014 {len(results)} desarrollos procesados")


# === FastAPI Server (Railway) =================================================

from fastapi import FastAPI, Header, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="Propyte Enrichment Runner", version="2.0")

# Shared state
_http_client: httpx.AsyncClient | None = None
_geocoder: Geocoder | None = None
_cycle_count = 0
_last_result: dict = {}
_is_running = False


async def _get_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=30)
    return _http_client


async def _get_geocoder() -> Geocoder:
    global _geocoder
    if _geocoder is None:
        _geocoder = Geocoder()
    return _geocoder


def _check_auth(authorization: str | None) -> bool:
    if not authorization:
        return False
    token = authorization.replace("Bearer ", "")
    return token == WEBHOOK_SECRET


@app.on_event("startup")
async def startup():
    client = await _get_client()
    await ensure_tables(client)
    logger.info(f"Enrichment Runner iniciado en puerto {SERVER_PORT}")


@app.on_event("shutdown")
async def shutdown():
    global _http_client, _geocoder
    if _geocoder:
        _geocoder._save_cache()
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()


@app.get("/health")
async def health():
    return {"status": "ok", "service": "enrichment-runner", "cycle_count": _cycle_count}


@app.get("/stats")
async def stats():
    client = await _get_client()
    q = """
    SELECT
      COUNT(*) as total,
      COUNT(price_min_mxn) as con_precio,
      COUNT(lat) as con_coords,
      COUNT(description_es) as con_descripcion,
      COUNT(contact_phone) as con_telefono,
      COUNT(total_units) as con_unidades,
      COUNT(delivery_text) as con_entrega
    FROM public.developments WHERE deleted_at IS NULL
    """
    result = await execute_sql(client, q)
    if result and isinstance(result, list):
        return {"coverage": result[0], "last_cycle": _last_result}
    return {"error": "no data"}


@app.post("/webhook/enrich/{dev_id}")
async def webhook_enrich_one(
    dev_id: str,
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(None),
):
    """Trigger: enriquecer un desarrollo específico (llamado por pg_net)."""
    if not _check_auth(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    background_tasks.add_task(_enrich_single, dev_id)
    return {"accepted": True, "dev_id": dev_id}


@app.post("/webhook/enrich-batch")
async def webhook_enrich_batch(
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(None),
):
    """Cron: enriquecer lote de 25 desarrollos (llamado por pg_cron)."""
    if not _check_auth(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    global _is_running
    if _is_running:
        return {"accepted": False, "reason": "cycle already running"}

    background_tasks.add_task(_enrich_batch_task)
    return {"accepted": True, "batch_size": BATCH_SIZE}


async def _enrich_single(dev_id: str):
    """Background task: enriquecer un solo desarrollo."""
    client = await _get_client()
    geocoder = await _get_geocoder()

    # Fetch the development by ID
    q = f"SELECT * FROM public.developments WHERE id = '{_escape_sql(dev_id)}' AND deleted_at IS NULL"
    result = await execute_sql(client, q)
    if not result or not isinstance(result, list) or len(result) == 0:
        logger.warning(f"Webhook: desarrollo {dev_id} no encontrado")
        return

    dev = result[0]
    logger.info(f"Webhook trigger: enriqueciendo {dev.get('name', '?')} ({dev_id[:8]}...)")
    await process_development(client, dev, geocoder)
    geocoder._save_cache()


async def _enrich_batch_task():
    """Background task: ciclo completo de enriquecimiento."""
    global _cycle_count, _last_result, _is_running
    _is_running = True
    try:
        client = await _get_client()
        geocoder = await _get_geocoder()
        _cycle_count += 1
        _last_result = await run_cycle(client, geocoder, _cycle_count, BATCH_SIZE)
    except Exception as e:
        logger.error(f"Error en batch task: {e}")
        _last_result = {"error": str(e)}
    finally:
        _is_running = False


# === Publisher Agent =========================================================

_publisher_running = False


@app.post("/webhook/publish-batch")
async def webhook_publish_batch(
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(None),
):
    """Cron cada 2h: evalúa calidad y publica desarrollos listos en WordPress."""
    if not _check_auth(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    global _publisher_running
    if _publisher_running:
        return {"accepted": False, "reason": "publisher already running"}

    background_tasks.add_task(_publish_batch_task)
    return {"accepted": True}


async def _publish_batch_task():
    """Background task: evalúa candidatos y publica los que pasan el quality gate."""
    global _publisher_running
    _publisher_running = True
    try:
        import uuid
        from agents.publisher.publisher import PublisherAgent
        from agents.publisher.audit_log import ensure_audit_table

        client = await _get_client()
        await ensure_audit_table(client)

        agent = PublisherAgent()
        result = await agent.run_batch(client, batch_id=uuid.uuid4())
        logger.info(
            f"[publisher] Batch: {result['published']} publicados, "
            f"{result['rejected']} rechazados, "
            f"{result['ai_reviewed']} revisados por AI"
        )
    except Exception as e:
        logger.error(f"[publisher] Error en batch task: {e}")
    finally:
        _publisher_running = False


# === Entry point =============================================================

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    test_mode = "--test" in sys.argv
    loop_mode = "--loop" in sys.argv
    server_mode = not test_mode and not loop_mode

    if test_mode:
        asyncio.run(run_test(dry_run))
    elif loop_mode:
        asyncio.run(run_forever(dry_run))
    else:
        # Server mode: FastAPI + background loop cada 30 min
        import uvicorn

        async def _background_loop():
            """Loop que corre en background junto al server."""
            await asyncio.sleep(10)  # Esperar a que el server inicie
            global _cycle_count, _last_result, _is_running
            client = await _get_client()
            geocoder = await _get_geocoder()
            while True:
                if not _is_running:
                    _is_running = True
                    try:
                        _cycle_count += 1
                        _last_result = await run_cycle(
                            client, geocoder, _cycle_count, BATCH_SIZE
                        )
                    except Exception as e:
                        logger.error(f"Background loop error: {e}")
                    finally:
                        _is_running = False
                await asyncio.sleep(1800)  # 30 min

        @app.on_event("startup")
        async def start_background_loop():
            asyncio.create_task(_background_loop())

        uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT, log_level="info")
