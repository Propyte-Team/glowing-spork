#!/usr/bin/env python3
"""
CLI del Agente de Deduplicacion.

Uso:
  python -m agents.deduplication.run --scan              # Procesar cola pendiente
  python -m agents.deduplication.run --full-scan          # Escanear todos los registros
  python -m agents.deduplication.run --full-scan --dry-run  # Solo reportar, sin fusionar
  python -m agents.deduplication.run --report             # Ver estadisticas
  python -m agents.deduplication.run --status             # Ver estado de la cola
  python -m agents.deduplication.run --table developments # Limitar a una tabla
"""

import argparse
import asyncio
import logging
import sys
import time

import httpx

from .config import SUPABASE_MGMT_TOKEN, DEDUP_BATCH_SIZE
from .supabase_client import execute_sql, rest_get, _escape_sql

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dedup.run")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Agente de Deduplicacion Propyte — CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--scan", action="store_true", help="Procesar cola pendiente (dedup_queue)")
    parser.add_argument("--full-scan", action="store_true", help="Escanear todos los registros buscando duplicados")
    parser.add_argument("--dry-run", action="store_true", help="Solo reportar duplicados, sin fusionar")
    parser.add_argument("--report", action="store_true", help="Mostrar estadisticas de dedup_log")
    parser.add_argument("--status", action="store_true", help="Mostrar estado de dedup_queue")
    parser.add_argument("--table", type=str, help="Limitar a una tabla (ej: developments, developers)")
    parser.add_argument("--limit", type=int, default=DEDUP_BATCH_SIZE, help=f"Maximo registros (default: {DEDUP_BATCH_SIZE})")
    return parser.parse_args()


async def cmd_scan(client: httpx.AsyncClient, limit: int):
    """Procesa la cola de deduplicacion."""
    logger.info(f"Procesando cola (limite: {limit})...")

    result = await execute_sql(
        client,
        f"SELECT public.dedup_process_queue({limit}) AS merges"
    )

    if result and isinstance(result, list) and len(result) > 0:
        merges = result[0].get("merges", 0)
        logger.info(f"Fusiones realizadas: {merges}")
    else:
        logger.info("No se pudo procesar la cola")


async def cmd_full_scan(
    client: httpx.AsyncClient,
    table_filter: str | None,
    limit: int,
    dry_run: bool,
):
    """Escanea todos los registros buscando duplicados."""
    start_time = time.time()

    # Obtener configuraciones activas
    config_result = await execute_sql(
        client,
        "SELECT table_name, schema_name, similarity_threshold "
        "FROM public.dedup_config WHERE enabled = true"
    )

    if not config_result or not isinstance(config_result, list):
        logger.error("No se encontraron configuraciones activas en dedup_config")
        return

    configs = config_result
    if table_filter:
        configs = [c for c in configs if c.get("table_name") == table_filter]
        if not configs:
            logger.error(f"No hay configuracion para tabla '{table_filter}'")
            return

    total_duplicados = 0
    total_merges = 0

    for config in configs:
        table_name = config["table_name"]
        schema_name = config.get("schema_name", "public")

        logger.info("=" * 50)
        logger.info(f"ESCANEANDO: {schema_name}.{table_name}")
        logger.info("=" * 50)

        # Obtener todos los registros activos
        count_result = await execute_sql(
            client,
            f"SELECT COUNT(*) AS total FROM {schema_name}.{table_name} WHERE deleted_at IS NULL"
        )
        total = 0
        if count_result and isinstance(count_result, list):
            total = count_result[0].get("total", 0)

        logger.info(f"Registros activos: {total}")

        # Obtener IDs en lotes
        offset = 0
        processed = 0
        table_duplicados = 0
        table_merges = 0

        while offset < total and (limit == 0 or processed < limit):
            batch_limit = min(50, limit - processed if limit > 0 else 50)

            id_result = await execute_sql(
                client,
                f"SELECT id::text FROM {schema_name}.{table_name} "
                f"WHERE deleted_at IS NULL "
                f"ORDER BY created_at "
                f"LIMIT {batch_limit} OFFSET {offset}"
            )

            if not id_result or not isinstance(id_result, list) or len(id_result) == 0:
                break

            for row in id_result:
                record_id = row.get("id")
                if not record_id:
                    continue

                # Buscar duplicados
                dups = await execute_sql(
                    client,
                    f"SELECT * FROM public.dedup_find_duplicates('{_escape_sql(table_name)}', '{_escape_sql(record_id)}')"
                )

                if dups and isinstance(dups, list) and len(dups) > 0:
                    for dup in dups:
                        dup_id = dup.get("duplicate_id", "?")
                        score = dup.get("similarity_score", 0)
                        match_type = dup.get("match_type", "?")

                        table_duplicados += 1

                        if dry_run:
                            # Obtener nombres para el reporte
                            name_result = await execute_sql(
                                client,
                                f"SELECT name FROM {schema_name}.{table_name} WHERE id = '{_escape_sql(record_id)}'"
                            )
                            dup_name_result = await execute_sql(
                                client,
                                f"SELECT name FROM {schema_name}.{table_name} WHERE id = '{_escape_sql(dup_id)}'"
                            )
                            name_a = name_result[0].get("name", "?") if name_result and isinstance(name_result, list) and len(name_result) > 0 else "?"
                            name_b = dup_name_result[0].get("name", "?") if dup_name_result and isinstance(dup_name_result, list) and len(dup_name_result) > 0 else "?"

                            logger.info(
                                f"  DUPLICADO [{match_type}] score={score:.3f}\n"
                                f"    A: {name_a} ({record_id[:8]}...)\n"
                                f"    B: {name_b} ({dup_id[:8]}...)"
                            )
                        else:
                            # Fusionar
                            merge_result = await execute_sql(
                                client,
                                f"SELECT public.dedup_process_record('{_escape_sql(table_name)}', '{_escape_sql(record_id)}') AS merges"
                            )
                            if merge_result and isinstance(merge_result, list) and len(merge_result) > 0:
                                m = merge_result[0].get("merges", 0)
                                table_merges += m
                                if m > 0:
                                    logger.info(f"  Fusionado: {record_id[:8]}... (+{m} merges)")
                            # No seguir buscando dups para este record si ya se proceso
                            break

                processed += 1

            offset += batch_limit

            # Progreso
            if processed % 50 == 0:
                logger.info(f"  Progreso: {processed}/{min(total, limit) if limit > 0 else total} — {table_duplicados} duplicados")

        logger.info(f"Resultado {table_name}: {table_duplicados} duplicados encontrados, {table_merges} fusiones")
        total_duplicados += table_duplicados
        total_merges += table_merges

    elapsed = time.time() - start_time

    print()
    print("=" * 50)
    print("RESUMEN FULL SCAN")
    print(f"  Tiempo: {elapsed:.1f}s")
    print(f"  Duplicados encontrados: {total_duplicados}")
    if dry_run:
        print(f"  Modo: DRY-RUN (sin fusiones)")
    else:
        print(f"  Fusiones realizadas: {total_merges}")
    print("=" * 50)


async def cmd_report(client: httpx.AsyncClient, table_filter: str | None):
    """Muestra estadisticas del dedup_log."""
    where = ""
    if table_filter:
        where = f"WHERE table_name = '{_escape_sql(table_filter)}'"

    # Resumen por tabla
    result = await execute_sql(
        client,
        f"SELECT table_name, action, COUNT(*) AS total "
        f"FROM public.dedup_log {where} "
        f"GROUP BY table_name, action "
        f"ORDER BY table_name, action"
    )

    print()
    print("=" * 50)
    print("REPORTE DE DEDUPLICACION")
    print("=" * 50)

    if result and isinstance(result, list) and len(result) > 0:
        for row in result:
            print(f"  {row.get('table_name')}: {row.get('action')} = {row.get('total')}")
    else:
        print("  Sin registros en dedup_log")

    # Ultimas 10 operaciones
    result = await execute_sql(
        client,
        f"SELECT table_name, winner_id, loser_id, similarity_score, match_type, action, "
        f"  created_at::text "
        f"FROM public.dedup_log {where} "
        f"ORDER BY created_at DESC LIMIT 10"
    )

    print(f"\nUltimas operaciones:")
    if result and isinstance(result, list):
        for row in result:
            score = row.get("similarity_score", 0) or 0
            print(
                f"  [{row.get('action')}] {row.get('table_name')} "
                f"winner={row.get('winner_id', '?')[:8]}... "
                f"loser={row.get('loser_id', '?')[:8]}... "
                f"score={score:.3f} ({row.get('match_type')}) "
                f"@ {row.get('created_at', '?')[:19]}"
            )
    else:
        print("  Sin operaciones recientes")
    print()


async def cmd_status(client: httpx.AsyncClient):
    """Muestra estado de la cola."""
    result = await execute_sql(
        client,
        "SELECT "
        "  COUNT(*) FILTER (WHERE NOT processed) AS pendientes, "
        "  COUNT(*) FILTER (WHERE processed) AS procesados, "
        "  COUNT(*) FILTER (WHERE attempts > 1) AS con_reintentos, "
        "  COUNT(*) FILTER (WHERE error_message IS NOT NULL) AS con_errores, "
        "  COUNT(*) AS total "
        "FROM public.dedup_queue"
    )

    print()
    print("=" * 50)
    print("ESTADO DE LA COLA (dedup_queue)")
    print("=" * 50)

    if result and isinstance(result, list) and len(result) > 0:
        row = result[0]
        print(f"  Total:          {row.get('total', 0)}")
        print(f"  Pendientes:     {row.get('pendientes', 0)}")
        print(f"  Procesados:     {row.get('procesados', 0)}")
        print(f"  Con reintentos: {row.get('con_reintentos', 0)}")
        print(f"  Con errores:    {row.get('con_errores', 0)}")
    else:
        print("  Cola vacia o no accesible")
    print()


async def main():
    args = parse_args()

    if not SUPABASE_MGMT_TOKEN:
        logger.error(
            "SUPABASE_MGMT_TOKEN no configurado. "
            "Agregalo al archivo .env en la raiz de glowing-spork/"
        )
        sys.exit(1)

    # Default: mostrar status si no se pide nada
    if not any([args.scan, args.full_scan, args.report, args.status]):
        args.status = True

    async with httpx.AsyncClient() as client:
        if args.status:
            await cmd_status(client)

        if args.report:
            await cmd_report(client, args.table)

        if args.scan:
            await cmd_scan(client, args.limit)

        if args.full_scan:
            await cmd_full_scan(client, args.table, args.limit, args.dry_run)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Cancelado por el usuario")
