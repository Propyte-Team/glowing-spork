#!/usr/bin/env python3
"""
Propyte Enrichment Agent V2 — Orquestador principal.

Enriquece desarrollos inmobiliarios en Supabase con:
  1. Geocoding (Nominatim)
  2. Scraping de URLs fuente
  3. Web search (DuckDuckGo)
  4. AI enrichment (Claude Haiku)

Uso:
  python -m enrichment_agent_v2.main                    # Todas las estrategias
  python -m enrichment_agent_v2.main --geocode           # Solo geocoding
  python -m enrichment_agent_v2.main --scrape            # Solo scraping URLs
  python -m enrichment_agent_v2.main --search            # Solo web search
  python -m enrichment_agent_v2.main --ai                # Solo AI enrichment
  python -m enrichment_agent_v2.main --limit 50          # Máximo 50 por estrategia
  python -m enrichment_agent_v2.main --dry-run           # Sin escrituras
  python -m enrichment_agent_v2.main --no-dashboard      # Sin dashboard web
  python -m enrichment_agent_v2.main --ciudad Mérida     # Filtrar por ciudad
  python -m enrichment_agent_v2.main --portal theredsearch  # Filtrar por portal
"""

import argparse
import asyncio
import logging
import sys
import time
from datetime import datetime

import httpx

from .config import BATCH_SIZE, DASHBOARD_PORT, LOOP_INTERVAL_SECONDS
from .gap_analyzer import (
    analyze_full_coverage,
    get_geocoding_candidates,
    get_scraping_candidates,
    get_search_candidates,
    get_ai_candidates,
)
from .geocoder import Geocoder
from .url_scraper import run_scraping_batch, run_search_batch
from .ai_enricher import run_ai_batch
from .supabase_writer import ensure_enrichment_log_table

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("enrichment_v2")

# Reduce noise from httpx
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Global dashboard broadcast function
_broadcast_fn = None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Propyte Enrichment Agent V2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Strategy flags
    parser.add_argument("--geocode", action="store_true", help="Solo geocoding")
    parser.add_argument("--scrape", action="store_true", help="Solo scraping de URLs")
    parser.add_argument("--search", action="store_true", help="Solo web search")
    parser.add_argument("--ai", action="store_true", help="Solo AI enrichment")

    # Options
    parser.add_argument("--limit", type=int, default=BATCH_SIZE, help=f"Máximo registros por estrategia (default: {BATCH_SIZE})")
    parser.add_argument("--dry-run", action="store_true", help="No escribir en Supabase")
    parser.add_argument("--no-dashboard", action="store_true", help="No iniciar dashboard web")
    parser.add_argument("--loop", action="store_true", help="Correr en loop continuo")
    parser.add_argument("--server", action="store_true", help="Solo dashboard (el agente se controla desde la web)")
    parser.add_argument("--ciudad", type=str, help="Filtrar por ciudad")
    parser.add_argument("--portal", type=str, help="Filtrar por portal fuente")
    parser.add_argument("--port", type=int, default=DASHBOARD_PORT, help=f"Puerto del dashboard (default: {DASHBOARD_PORT})")

    return parser.parse_args()


async def run_once(args, client: httpx.AsyncClient):
    """Ejecuta un ciclo completo de enriquecimiento."""
    global _broadcast_fn
    # Ensure broadcast is set (for server mode where dashboard starts first)
    if _broadcast_fn is None:
        try:
            from .dashboard.server import broadcast
            _broadcast_fn = broadcast
        except ImportError:
            pass
    start_time = time.time()
    run_all = not (args.geocode or args.scrape or args.search or args.ai)

    stats = {
        "geocoded": 0,
        "scraped": 0,
        "searched": 0,
        "ai_enriched": 0,
        "total_fields": 0,
        "errors": 0,
    }

    # --- Phase 0: Analyze gaps ---
    logger.info("=" * 60)
    logger.info("PROPYTE ENRICHMENT AGENT V2")
    logger.info("=" * 60)

    gaps = await analyze_full_coverage(client)
    total = gaps.get("total_developments", 0)
    logger.info(f"Total desarrollos: {total}")

    if gaps.get("gaps"):
        for field, info in list(gaps["gaps"].items())[:6]:
            pct = info.get("pct_faltante", 0)
            prioridad = info.get("prioridad", "?")
            logger.info(f"  {field}: {pct}% faltante [{prioridad}]")

    if _broadcast_fn:
        await _broadcast_fn({"type": "gap_update", "gaps": gaps})

    # --- Phase 1: Geocoding ---
    if run_all or args.geocode:
        logger.info("-" * 40)
        logger.info("FASE 1: Geocoding")
        candidates = await get_geocoding_candidates(client, args.limit, args.ciudad)
        logger.info(f"Candidatos para geocoding: {len(candidates)}")

        if candidates:
            geocoder = Geocoder()
            results = await geocoder.run_batch(
                client, candidates, dry_run=args.dry_run, broadcast_fn=_broadcast_fn
            )
            stats["geocoded"] = len(results)
            stats["total_fields"] += len(results) * 2  # lat + lng
            logger.info(f"Geocodificados: {len(results)}/{len(candidates)}")
            logger.info(f"  Cache: {geocoder.stats}")

    # --- Phase 2: URL Scraping ---
    if run_all or args.scrape:
        logger.info("-" * 40)
        logger.info("FASE 2: Scraping de URLs fuente")
        candidates = await get_scraping_candidates(client, args.limit, args.portal)
        logger.info(f"Candidatos para scraping: {len(candidates)}")

        if candidates:
            results = await run_scraping_batch(
                client, candidates, dry_run=args.dry_run, broadcast_fn=_broadcast_fn
            )
            stats["scraped"] = len(results)
            for r in results:
                stats["total_fields"] += len(r.get("enrichments", {}))
            logger.info(f"Scraped exitoso: {len(results)}/{len(candidates)}")

    # --- Phase 3: Web Search ---
    if run_all or args.search:
        logger.info("-" * 40)
        logger.info("FASE 3: Web Search (DuckDuckGo)")
        candidates = await get_search_candidates(client, args.limit, args.ciudad)
        logger.info(f"Candidatos para web search: {len(candidates)}")

        if candidates:
            results = await run_search_batch(
                client, candidates, dry_run=args.dry_run, broadcast_fn=_broadcast_fn
            )
            stats["searched"] = len(results)
            for r in results:
                stats["total_fields"] += len(r.get("enrichments", {}))
            logger.info(f"Search exitoso: {len(results)}/{len(candidates)}")

    # --- Phase 4: AI Enrichment ---
    if run_all or args.ai:
        logger.info("-" * 40)
        logger.info("FASE 4: AI Enrichment (Claude Haiku)")
        candidates = await get_ai_candidates(client, args.limit)
        logger.info(f"Candidatos para AI: {len(candidates)}")

        if candidates:
            results = await run_ai_batch(
                client, candidates, dry_run=args.dry_run, broadcast_fn=_broadcast_fn
            )
            stats["ai_enriched"] = len(results)
            for r in results:
                stats["total_fields"] += len(r.get("enrichments", {}))
            logger.info(f"AI enriched: {len(results)}/{len(candidates)}")

    # --- Summary ---
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("RESUMEN")
    logger.info(f"  Tiempo: {elapsed:.1f}s")
    logger.info(f"  Geocodificados: {stats['geocoded']}")
    logger.info(f"  Scraped: {stats['scraped']}")
    logger.info(f"  Web search: {stats['searched']}")
    logger.info(f"  AI enriched: {stats['ai_enriched']}")
    logger.info(f"  Total campos actualizados: {stats['total_fields']}")
    logger.info("=" * 60)

    if _broadcast_fn:
        await _broadcast_fn({
            "type": "stats_update",
            "stats": stats,
            "elapsed": elapsed,
        })

    return stats


async def main():
    global _broadcast_fn
    args = parse_args()

    # Validate config
    from .config import SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_MGMT_TOKEN
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error(
            "Missing env vars. Required: PROPYTE_SUPABASE_URL, "
            "PROPYTE_SUPABASE_SERVICE_KEY"
        )
        logger.error("Set them in glowing-spork/.env")
        sys.exit(1)
    if not SUPABASE_MGMT_TOKEN:
        logger.warning(
            "SUPABASE_MGMT_TOKEN not set — writes will fail. "
            "Add it to .env for full functionality. Dry-run mode recommended."
        )
        if not args.dry_run:
            logger.warning("Forcing --dry-run since MGMT token is missing")
            args.dry_run = True

    async with httpx.AsyncClient() as client:
        # Ensure enrichment_log table
        await ensure_enrichment_log_table(client)

        # Start dashboard if enabled
        dashboard_task = None
        port = args.port
        if not args.no_dashboard:
            try:
                from .dashboard.server import create_app, broadcast
                global _broadcast_fn
                _broadcast_fn = broadcast
                import uvicorn

                app = create_app(client)
                config = uvicorn.Config(
                    app,
                    host="0.0.0.0",
                    port=port,
                    log_level="warning",
                )
                server = uvicorn.Server(config)
                dashboard_task = asyncio.create_task(server.serve())
                logger.info(f"Dashboard: http://localhost:{port}")
            except ImportError as e:
                logger.warning(f"Dashboard not available: {e}")
                logger.warning("Install fastapi + uvicorn for the dashboard")

        # Server-only mode: just run the dashboard, agent controlled via web UI
        if args.server:
            logger.info("Server mode — agent controlled from dashboard")
            if dashboard_task:
                try:
                    await dashboard_task
                except asyncio.CancelledError:
                    pass
            return

        # Run enrichment
        if args.loop:
            logger.info(f"Running in loop mode (interval: {LOOP_INTERVAL_SECONDS}s)")
            while True:
                try:
                    await run_once(args, client)
                except Exception as e:
                    logger.error(f"Loop error: {e}")
                logger.info(f"Waiting {LOOP_INTERVAL_SECONDS}s...")
                await asyncio.sleep(LOOP_INTERVAL_SECONDS)
        else:
            await run_once(args, client)

        # Cleanup
        if dashboard_task:
            logger.info("Dashboard running. Press Ctrl+C to stop.")
            try:
                await dashboard_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
