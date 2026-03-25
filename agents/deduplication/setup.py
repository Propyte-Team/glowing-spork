#!/usr/bin/env python3
"""
Instalador del Agente de Deduplicacion en Supabase.

Ejecuta todas las migraciones SQL via Management API.
Se corre una sola vez:
    python -m agents.deduplication.setup

Despues de esto, el agente opera solo mediante triggers.
"""

import asyncio
import logging
import os
import sys

import httpx

from .config import SUPABASE_MGMT_TOKEN, SQL_DIR
from .supabase_client import execute_sql, execute_sql_file

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dedup.setup")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def _sql_path(filename: str) -> str:
    return os.path.join(SQL_DIR, filename)


async def _check_connection(client: httpx.AsyncClient) -> bool:
    """Verifica conexion a Supabase."""
    result = await execute_sql(client, "SELECT 1 AS ok")
    if result:
        logger.info("Conexion a Supabase OK")
        return True
    logger.error("No se pudo conectar a Supabase. Verifica SUPABASE_MGMT_TOKEN en .env")
    return False


async def _check_extensions(client: httpx.AsyncClient) -> dict:
    """Verifica extensiones disponibles."""
    result = await execute_sql(
        client,
        "SELECT extname FROM pg_extension WHERE extname IN ('pg_trgm', 'fuzzystrmatch')"
    )
    installed = set()
    if result and isinstance(result, list):
        for row in result:
            installed.add(row.get("extname", ""))
    return {
        "pg_trgm": "pg_trgm" in installed,
        "fuzzystrmatch": "fuzzystrmatch" in installed,
    }


async def _detect_table_type(client: httpx.AsyncClient, schema: str, table: str) -> str:
    """Detecta si un objeto es TABLE ('r'), VIEW ('v'), o no existe (None)."""
    result = await execute_sql(
        client,
        f"SELECT c.relkind FROM pg_class c "
        f"JOIN pg_namespace n ON c.relnamespace = n.oid "
        f"WHERE n.nspname = '{schema}' AND c.relname = '{table}'"
    )
    if result and isinstance(result, list) and len(result) > 0:
        return result[0].get("relkind", "?")
    return "?"


async def _verify_config(client: httpx.AsyncClient) -> list:
    """Lee la configuracion instalada."""
    result = await execute_sql(
        client,
        "SELECT table_name, schema_name, enabled, similarity_threshold, merge_strategy "
        "FROM public.dedup_config ORDER BY table_name"
    )
    if result and isinstance(result, list):
        return result
    return []


async def _verify_triggers(client: httpx.AsyncClient) -> list:
    """Lista triggers de deduplicacion instalados."""
    result = await execute_sql(
        client,
        "SELECT tgname, relname "
        "FROM pg_trigger t "
        "JOIN pg_class c ON t.tgrelid = c.oid "
        "WHERE tgname LIKE 'trg_dedup_%' "
        "ORDER BY tgname"
    )
    if result and isinstance(result, list):
        return result
    return []


async def _verify_functions(client: httpx.AsyncClient) -> list:
    """Lista funciones de deduplicacion instaladas."""
    result = await execute_sql(
        client,
        "SELECT proname FROM pg_proc "
        "WHERE proname LIKE 'dedup_%' "
        "ORDER BY proname"
    )
    if result and isinstance(result, list):
        return result
    return []


async def install(client: httpx.AsyncClient) -> bool:
    """Ejecuta la instalacion completa del agente de deduplicacion."""

    print()
    print("=" * 60)
    print("  AGENTE DE DEDUPLICACION — INSTALACION")
    print("=" * 60)
    print()

    # --- Paso 1: Verificar conexion ---
    print("[1/8] Verificando conexion a Supabase...")
    if not await _check_connection(client):
        return False

    # --- Paso 2: Verificar extensiones ---
    print("[2/8] Verificando extensiones...")
    exts = await _check_extensions(client)
    for ext_name, installed in exts.items():
        status = "OK" if installed else "FALTA — se instalara"
        print(f"  {ext_name}: {status}")

    # --- Paso 3: Instalar extensiones ---
    print("[3/8] Instalando extensiones (pg_trgm, fuzzystrmatch)...")
    ok = await execute_sql_file(client, _sql_path("001_extensions.sql"))
    if not ok:
        logger.warning(
            "No se pudieron instalar extensiones via Management API. "
            "Ejecuta manualmente en Supabase SQL Editor:\n"
            "  CREATE EXTENSION IF NOT EXISTS pg_trgm;\n"
            "  CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;"
        )
    else:
        print("  Extensiones OK")

    # --- Paso 4: Detectar tipo de developments ---
    print("[4/8] Detectando tipo de public.developments...")
    dev_type = await _detect_table_type(client, "public", "developments")
    type_label = {"r": "TABLE", "v": "VIEW", "m": "MATERIALIZED VIEW"}.get(dev_type, f"DESCONOCIDO ({dev_type})")
    print(f"  public.developments es: {type_label}")

    if dev_type == "v":
        print("  Los triggers iran en real_estate_hub.\"Propyte_desarrollos\"")
        hub_type = await _detect_table_type(client, "real_estate_hub", "Propyte_desarrollos")
        if hub_type != "r":
            logger.warning(f"real_estate_hub.Propyte_desarrollos no es tabla (relkind={hub_type})")

    # --- Paso 5: Crear tablas de soporte ---
    print("[5/8] Creando tablas (dedup_config, dedup_log, dedup_queue)...")
    ok = await execute_sql_file(client, _sql_path("002_dedup_tables.sql"))
    if ok:
        print("  Tablas creadas OK")
    else:
        logger.error("Fallo al crear tablas de soporte")
        return False

    # --- Paso 6: Crear funciones ---
    print("[6/8] Creando funciones PL/pgSQL...")
    ok = await execute_sql_file(client, _sql_path("003_dedup_functions.sql"))
    if ok:
        print("  Funciones creadas OK")
    else:
        logger.error("Fallo al crear funciones")
        return False

    # --- Paso 7: Crear triggers ---
    print("[7/8] Creando triggers...")
    ok = await execute_sql_file(client, _sql_path("004_triggers.sql"))
    if ok:
        print("  Triggers creados OK")
    else:
        logger.warning(
            "No se pudieron crear triggers via Management API.\n"
            "Ejecuta manualmente en Supabase SQL Editor el contenido de:\n"
            f"  {_sql_path('004_triggers.sql')}"
        )

    # --- Paso 8: Seed config ---
    print("[8/8] Insertando configuracion inicial...")
    ok = await execute_sql_file(client, _sql_path("005_seed_config.sql"))
    if ok:
        print("  Configuracion insertada OK")
    else:
        logger.error("Fallo al insertar configuracion")

    # --- Verificacion final ---
    print()
    print("=" * 60)
    print("  REPORTE DE INSTALACION")
    print("=" * 60)

    # Funciones
    funcs = await _verify_functions(client)
    print(f"\nFunciones instaladas: {len(funcs)}")
    for f in funcs:
        print(f"  - {f.get('proname', '?')}")

    # Triggers
    triggers = await _verify_triggers(client)
    print(f"\nTriggers instalados: {len(triggers)}")
    for t in triggers:
        print(f"  - {t.get('tgname', '?')} en {t.get('relname', '?')}")

    # Config
    configs = await _verify_config(client)
    print(f"\nConfiguraciones activas: {len(configs)}")
    for c in configs:
        print(
            f"  - {c.get('table_name')}: "
            f"threshold={c.get('similarity_threshold')}, "
            f"strategy={c.get('merge_strategy')}, "
            f"enabled={c.get('enabled')}"
        )

    print()
    if len(funcs) >= 5 and len(configs) >= 2:
        print("INSTALACION COMPLETA")
        if len(triggers) == 0:
            print("ADVERTENCIA: No se detectaron triggers. Ejecuta 004_triggers.sql manualmente.")
        print("\nSiguientes pasos:")
        print("  1. python -m agents.deduplication.run --full-scan --dry-run")
        print("  2. python -m agents.deduplication.run --full-scan")
        return True
    else:
        print("INSTALACION INCOMPLETA — revisa los errores arriba")
        return False


async def main():
    if not SUPABASE_MGMT_TOKEN:
        logger.error(
            "SUPABASE_MGMT_TOKEN no configurado. "
            "Agregalo al archivo .env en la raiz de glowing-spork/"
        )
        sys.exit(1)

    async with httpx.AsyncClient() as client:
        success = await install(client)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Cancelado por el usuario")
