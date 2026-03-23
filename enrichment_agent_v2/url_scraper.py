"""
Scraping asíncrono de URLs fuente con estrategias específicas por portal.
"""

import asyncio
import json
import logging
import re
from urllib.parse import urlparse, quote_plus

import httpx
from bs4 import BeautifulSoup

from .config import HEADERS_HTTP, REDSEARCH_EMAIL, REDSEARCH_PASS, MAX_CONCURRENT
from .extractors import (
    extraer_precio,
    extraer_superficie,
    extraer_num_unidades,
    extraer_desarrolladora,
    extraer_amenidades_lista,
    extraer_fecha_entrega,
    extraer_contacto,
    extraer_imagen,
    extraer_coordenadas,
    extraer_json_ld,
    campo_vacio,
)
from .supabase_writer import update_development, log_enrichment

logger = logging.getLogger("enrichment_v2.url_scraper")

# Rate limiters per domain
_domain_semaphores: dict[str, asyncio.Semaphore] = {}
_global_semaphore = asyncio.Semaphore(MAX_CONCURRENT)


def _get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return "unknown"


def _get_domain_semaphore(domain: str) -> asyncio.Semaphore:
    if domain not in _domain_semaphores:
        _domain_semaphores[domain] = asyncio.Semaphore(1)
    return _domain_semaphores[domain]


def _identify_portal(url: str) -> str:
    """Identifica portal por dominio de la URL."""
    domain = _get_domain(url)
    portal_map = {
        "lahaus.mx": "lahaus",
        "lahaus.com": "lahaus",
        "theredsearch.com": "theredsearch",
        "lamudi.com.mx": "lamudi",
        "behome.mx": "behome",
        "inmuebles24.com": "inmuebles24",
        "propiedades.com": "propiedades",
        "casasyterrenos.com": "casasyterrenos",
        "factorinmobiliario.mx": "factorinmobiliario",
        "luumorealestate.com": "luumo",
        "trovit.com.mx": "trovit",
        "monterreyskyline.com": "monterreyskyline",
        "proyectos-inmobiliarios.com": "proyectos-inmobiliarios",
    }
    for key, name in portal_map.items():
        if key in domain:
            return name
    return "generic"


# =============================================================================
# Portal-specific scrapers
# =============================================================================

async def _fetch_html(
    client: httpx.AsyncClient, url: str, timeout: int = 20
) -> str | None:
    """Fetch HTML con rate limiting por dominio."""
    domain = _get_domain(url)
    domain_sem = _get_domain_semaphore(domain)

    async with _global_semaphore:
        async with domain_sem:
            try:
                resp = await client.get(
                    url, headers=HEADERS_HTTP, timeout=timeout, follow_redirects=True
                )
                if resp.status_code >= 400:
                    logger.warning(f"HTTP {resp.status_code} for {url[:80]}")
                    return None
                # Delay entre requests al mismo dominio
                await asyncio.sleep(1.5)
                return resp.text
            except Exception as e:
                logger.error(f"Fetch error {url[:60]}: {e}")
                return None


def _scrape_generic(html: str, url: str) -> dict:
    """Estrategia genérica: extrae todo lo posible del HTML."""
    soup = BeautifulSoup(html, "html.parser")
    texto = soup.get_text(separator=" ", strip=True)
    datos = {}

    # Precios
    price_min, price_max = extraer_precio(texto)
    if price_min:
        datos["price_min_mxn"] = price_min
    if price_max and price_max != price_min:
        datos["price_max_mxn"] = price_max

    # Unidades
    num_units = extraer_num_unidades(texto)
    if num_units:
        datos["total_units"] = num_units

    # Fecha entrega
    delivery = extraer_fecha_entrega(texto)
    if delivery:
        datos["delivery_text"] = delivery

    # Amenidades — extraer como lista de texto (no boolean columns, no existen en la tabla)
    amenidades_lista = extraer_amenidades_lista(texto)
    if amenidades_lista:
        datos["amenities"] = amenidades_lista

    # Contacto
    telefono, email = extraer_contacto(texto)
    if telefono:
        datos["contact_phone"] = telefono

    # Imagen
    imagen = extraer_imagen(soup, url)
    if imagen:
        datos["images"] = [imagen]

    # Coordenadas (buscar en scripts)
    scripts_text = " ".join(s.string or "" for s in soup.find_all("script"))
    lat, lng = extraer_coordenadas(scripts_text)
    if lat:
        datos["lat"] = lat
        datos["lng"] = lng

    # Descripción (primer párrafo relevante)
    for p in soup.find_all("p"):
        txt = p.get_text(strip=True)
        if len(txt) > 80 and any(kw in txt.lower() for kw in
            ["desarrollo", "proyecto", "preventa", "residencial", "departamento",
             "ubicado", "construi", "amenidades", "inversión", "vivienda"]):
            datos["description_es"] = txt[:2000]
            break

    return datos


def _scrape_json_ld(html: str, url: str) -> dict:
    """Estrategia JSON-LD para LaHaus y similares."""
    soup = BeautifulSoup(html, "html.parser")
    datos = extraer_json_ld(soup)

    # Complementar con scraping genérico para campos faltantes
    if not datos.get("price_min_mxn") or not datos.get("description_es"):
        generic = _scrape_generic(html, url)
        for key, val in generic.items():
            if key not in datos:
                datos[key] = val

    return datos


def _scrape_lamudi(html: str, url: str) -> dict:
    """Estrategia específica para Lamudi."""
    soup = BeautifulSoup(html, "html.parser")
    datos = {}

    # Precio específico Lamudi
    price_el = soup.select_one(".listing-price, [class*='price'], [data-testid='price']")
    if price_el:
        price_text = price_el.get_text(strip=True)
        p_min, p_max = extraer_precio(price_text)
        if p_min:
            datos["price_min_mxn"] = p_min
        if p_max and p_max != p_min:
            datos["price_max_mxn"] = p_max

    # Complementar con genérico
    generic = _scrape_generic(html, url)
    for key, val in generic.items():
        if key not in datos:
            datos[key] = val

    return datos


# =============================================================================
# DuckDuckGo fallback
# =============================================================================

PORTAL_BLACKLIST = [
    "duckduckgo", "google", "facebook", "twitter", "instagram",
    "youtube", "wikipedia", "linkedin",
]

async def _search_duckduckgo(
    client: httpx.AsyncClient, query: str, max_results: int = 10
) -> list[dict]:
    """Busca en DuckDuckGo HTML y retorna [{title, url, snippet}]."""
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    html = await _fetch_html(client, url, timeout=15)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    results = []
    for div in soup.select("div.result, div.web-result"):
        link_el = div.select_one("a.result__a, h2 a")
        snippet_el = div.select_one("a.result__snippet, .result__snippet")
        if not link_el:
            continue

        href = link_el.get("href", "")
        # DuckDuckGo wraps URLs
        uddg = re.search(r"uddg=([^&]+)", href)
        if uddg:
            from urllib.parse import unquote
            href = unquote(uddg.group(1))

        # Skip portales no útiles
        if any(bl in href.lower() for bl in PORTAL_BLACKLIST):
            continue

        results.append({
            "title": link_el.get_text(strip=True),
            "url": href,
            "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
        })
        if len(results) >= max_results:
            break

    await asyncio.sleep(3)  # Rate limit DuckDuckGo
    return results


async def scrape_via_search(
    client: httpx.AsyncClient, dev: dict
) -> dict:
    """Enriquece un desarrollo buscando info en DuckDuckGo."""
    name = dev.get("name", "")
    city = dev.get("city", "")
    if not name:
        return {}

    datos = {}
    queries = [
        f'"{name}" {city} desarrollo inmobiliario',
    ]
    if not dev.get("price_min_mxn"):
        queries.append(f'"{name}" {city} precio preventa')

    for query in queries:
        results = await _search_duckduckgo(client, query)
        for r in results:
            combined = f"{r['title']} {r['snippet']}"
            if not datos.get("price_min_mxn"):
                p_min, p_max = extraer_precio(combined)
                if p_min:
                    datos["price_min_mxn"] = p_min
                if p_max and p_max != p_min:
                    datos["price_max_mxn"] = p_max

            if not datos.get("total_units"):
                units = extraer_num_unidades(combined)
                if units:
                    datos["total_units"] = units

            if not datos.get("delivery_text"):
                delivery = extraer_fecha_entrega(combined)
                if delivery:
                    datos["delivery_text"] = delivery

            # Capturar URL del proyecto si parece legítima
            if not datos.get("source_url") and r["url"]:
                if not any(bl in r["url"].lower() for bl in PORTAL_BLACKLIST + ["trovit"]):
                    datos["source_url"] = r["url"]

    return datos


# =============================================================================
# Main scraper orchestrator
# =============================================================================

async def scrape_development(
    client: httpx.AsyncClient, dev: dict
) -> dict:
    """Scrape una URL fuente y retorna campos extraídos."""
    url = dev.get("source_url", "")
    if not url or campo_vacio(url):
        return {}

    portal = _identify_portal(url)
    logger.debug(f"Scraping {dev.get('name', '')} [{portal}]: {url[:80]}")

    html = await _fetch_html(client, url)
    if not html:
        return {}

    # Seleccionar estrategia
    if portal in ("lahaus",):
        datos = _scrape_json_ld(html, url)
    elif portal == "lamudi":
        datos = _scrape_lamudi(html, url)
    else:
        datos = _scrape_generic(html, url)

    # Filtrar: solo campos que existen en la tabla y que el dev no tiene
    VALID_COLUMNS = {
        "price_min_mxn", "price_max_mxn", "lat", "lng", "images",
        "contact_phone", "description_es", "amenities", "total_units",
        "delivery_text", "developer_id", "source_url", "zone",
    }
    filtered = {}
    for key, val in datos.items():
        if val is not None and key in VALID_COLUMNS and dev.get(key) is None:
            filtered[key] = val

    return filtered


async def scrape_and_update(
    client: httpx.AsyncClient,
    dev: dict,
    dry_run: bool = False,
) -> dict | None:
    """Scrape + update en Supabase. Retorna campos actualizados."""
    dev_id = dev.get("id", "")
    dev_name = dev.get("name", "")
    portal = dev.get("detection_source", "")
    source_url = dev.get("source_url", "")

    enrichments = await scrape_development(client, dev)
    if not enrichments:
        return None

    if dry_run:
        logger.info(f"[DRY-RUN] Would update {dev_name}: {list(enrichments.keys())}")
        return enrichments

    # Single SQL: UPDATE + one LOG entry
    from .supabase_writer import execute_sql, build_set_clause, _escape_sql
    set_clause = build_set_clause(enrichments)
    fields_str = ",".join(enrichments.keys())
    query = (
        f"UPDATE public.developments SET {set_clause}, updated_at = NOW() "
        f"WHERE id = '{_escape_sql(dev_id)}';\n"
        f"INSERT INTO public.enrichment_log (development_id, development_name, field_name, strategy, source_url, portal) "
        f"VALUES ('{_escape_sql(dev_id)}', '{_escape_sql(dev_name)}', '{_escape_sql(fields_str)}', 'url_scrape', '{_escape_sql(source_url)}', '{_escape_sql(portal)}')"
    )
    result = await execute_sql(client, query)
    if result is not None:
        logger.info(f"Scraped {dev_name}: {list(enrichments.keys())}")
        return enrichments
    return None


async def search_and_update(
    client: httpx.AsyncClient,
    dev: dict,
    dry_run: bool = False,
) -> dict | None:
    """Web search + update en Supabase."""
    dev_id = dev.get("id", "")
    dev_name = dev.get("name", "")

    enrichments = await scrape_via_search(client, dev)
    if not enrichments:
        return None

    if dry_run:
        logger.info(f"[DRY-RUN] Would update {dev_name} (search): {list(enrichments.keys())}")
        return enrichments

    from .supabase_writer import execute_sql, build_set_clause, _escape_sql
    set_clause = build_set_clause(enrichments)
    fields_str = ",".join(enrichments.keys())
    query = (
        f"UPDATE public.developments SET {set_clause}, updated_at = NOW() "
        f"WHERE id = '{_escape_sql(dev_id)}';\n"
        f"INSERT INTO public.enrichment_log (development_id, development_name, field_name, strategy) "
        f"VALUES ('{_escape_sql(dev_id)}', '{_escape_sql(dev_name)}', '{_escape_sql(fields_str)}', 'web_search')"
    )
    result = await execute_sql(client, query)
    if result is not None:
        logger.info(f"Search enriched {dev_name}: {list(enrichments.keys())}")
        return enrichments
    return None


async def run_scraping_batch(
    client: httpx.AsyncClient,
    developments: list[dict],
    dry_run: bool = False,
    broadcast_fn=None,
) -> list[dict]:
    """Scrape un batch de desarrollos con sus URLs fuente."""
    results = []
    for i, dev in enumerate(developments):
        result = await scrape_and_update(client, dev, dry_run=dry_run)
        if result:
            results.append({"dev": dev, "enrichments": result})
            if broadcast_fn:
                await broadcast_fn({
                    "type": "enrichment",
                    "strategy": "url_scrape",
                    "dev_name": dev.get("name", ""),
                    "city": dev.get("city", ""),
                    "portal": dev.get("detection_source", ""),
                    "fields": list(result.keys()),
                    "processed": i + 1,
                    "total": len(developments),
                })

        if (i + 1) % 10 == 0:
            logger.info(f"Scraping progress: {i + 1}/{len(developments)}")

    return results


async def run_search_batch(
    client: httpx.AsyncClient,
    developments: list[dict],
    dry_run: bool = False,
    broadcast_fn=None,
) -> list[dict]:
    """Web search un batch de desarrollos."""
    results = []
    for i, dev in enumerate(developments):
        result = await search_and_update(client, dev, dry_run=dry_run)
        if result:
            results.append({"dev": dev, "enrichments": result})
            if broadcast_fn:
                await broadcast_fn({
                    "type": "enrichment",
                    "strategy": "web_search",
                    "dev_name": dev.get("name", ""),
                    "city": dev.get("city", ""),
                    "fields": list(result.keys()),
                    "processed": i + 1,
                    "total": len(developments),
                })

        if (i + 1) % 10 == 0:
            logger.info(f"Search progress: {i + 1}/{len(developments)}")

    return results
