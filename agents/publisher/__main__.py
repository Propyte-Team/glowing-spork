"""
CLI del Publishing Agent.

Uso:
  python -m agents.publisher                  # batch real
  python -m agents.publisher --dry-run        # simula sin escribir nada
  python -m agents.publisher --stats          # muestra candidatos sin publicar
  python -m agents.publisher --setup          # crea tabla publish_audit_log
"""

import asyncio
import json
import logging
import sys
import uuid

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def cmd_stats(client: httpx.AsyncClient) -> None:
    from .supabase_client import execute_sql

    candidates_sql = """\
SELECT COUNT(*) AS total_candidatos
FROM public.developments
WHERE deleted_at IS NULL
  AND published = false
  AND description_es IS NOT NULL
  AND price_min_mxn IS NOT NULL
  AND city IS NOT NULL
  AND images IS NOT NULL
  AND array_length(images, 1) >= 1"""

    published_sql = """\
SELECT COUNT(*) AS total_publicados
FROM public.developments
WHERE deleted_at IS NULL AND published = true"""

    audit_sql = """\
SELECT decision, COUNT(*) AS total
FROM public.publish_audit_log
GROUP BY decision
ORDER BY total DESC"""

    r1 = await execute_sql(client, candidates_sql)
    r2 = await execute_sql(client, published_sql)
    r3 = await execute_sql(client, audit_sql)

    print("\n── Publisher Stats ──────────────────────")
    if r1:
        print(f"  Candidatos listos para publicar: {r1[0].get('total_candidatos', '?')}")
    if r2:
        print(f"  Ya publicados en Supabase:        {r2[0].get('total_publicados', '?')}")
    if r3:
        print("  Historial de decisiones:")
        for row in r3:
            print(f"    {row.get('decision'):12} → {row.get('total')}")
    print("─────────────────────────────────────────\n")


async def cmd_setup(client: httpx.AsyncClient) -> None:
    from .audit_log import ensure_audit_table
    ok = await ensure_audit_table(client)
    print("✓ publish_audit_log tabla creada/verificada" if ok else "✗ Error creando tabla")


async def main() -> None:
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    stats_only = "--stats" in args
    setup = "--setup" in args

    async with httpx.AsyncClient(timeout=30) as client:
        if setup:
            await cmd_setup(client)
            return

        if stats_only:
            await cmd_stats(client)
            return

        from .publisher import PublisherAgent
        from .audit_log import ensure_audit_table

        # Asegurar que la tabla existe
        await ensure_audit_table(client)

        if dry_run:
            print("\n⚠  DRY RUN — no se escribirá nada en Supabase ni WP\n")

        agent = PublisherAgent()
        result = await agent.run_batch(
            client,
            batch_id=uuid.uuid4(),
            dry_run=dry_run,
        )

        print("\n── Resultado del batch ──────────────────")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print("─────────────────────────────────────────\n")


asyncio.run(main())
