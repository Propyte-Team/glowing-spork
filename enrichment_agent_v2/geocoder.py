"""
Geocodificación asíncrona via Nominatim (OpenStreetMap).
Gratuito, sin API key. Límite: 1 request/segundo.
"""

import asyncio
import json
import logging
import os

import httpx

from .config import NOMINATIM_DELAY
from .supabase_writer import update_development, log_enrichment

logger = logging.getLogger("enrichment_v2.geocoder")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(BASE_DIR, "geocoding_cache.json")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {
    "User-Agent": "PropyteEnrichmentAgent/2.0 (contacto@propyte.com)",
    "Accept": "application/json",
}

# Bounding box México
LAT_MIN, LAT_MAX = 14.0, 33.0
LNG_MIN, LNG_MAX = -118.0, -86.0


class Geocoder:
    def __init__(self):
        self.cache: dict[str, tuple[float, float] | None] = {}
        self._load_cache()
        self._semaphore = asyncio.Semaphore(1)  # 1 request at a time
        self.stats = {"total": 0, "cached": 0, "found": 0, "not_found": 0, "errors": 0}

    def _load_cache(self):
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for k, v in data.items():
                        if v is not None:
                            self.cache[k] = tuple(v)
                        else:
                            self.cache[k] = None
                logger.info(f"Geocoding cache loaded: {len(self.cache)} entries")
            except (json.JSONDecodeError, IOError):
                pass

    def _save_cache(self):
        try:
            serializable = {}
            for k, v in self.cache.items():
                serializable[k] = list(v) if v is not None else None
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(serializable, f, indent=2, ensure_ascii=False)
        except IOError as e:
            logger.error(f"Error saving geocoding cache: {e}")

    def _cache_key(self, city: str, zone: str | None = None) -> str:
        return f"{(city or '').strip().lower()}|{(zone or '').strip().lower()}"

    async def _nominatim_query(
        self, client: httpx.AsyncClient, query: str
    ) -> tuple[float, float] | None:
        """Hace una query a Nominatim y retorna (lat, lng) o None."""
        async with self._semaphore:
            try:
                resp = await client.get(
                    NOMINATIM_URL,
                    headers=NOMINATIM_HEADERS,
                    params={
                        "q": query,
                        "format": "json",
                        "limit": "1",
                        "countrycodes": "mx",
                    },
                    timeout=15,
                )
                await asyncio.sleep(NOMINATIM_DELAY)

                if resp.status_code != 200:
                    return None

                results = resp.json()
                if not results:
                    return None

                lat = float(results[0]["lat"])
                lng = float(results[0]["lon"])

                # Validar bounding box México
                if LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX:
                    return (lat, lng)
                return None

            except Exception as e:
                logger.error(f"Nominatim error for '{query}': {e}")
                return None

    async def geocode(
        self,
        client: httpx.AsyncClient,
        city: str,
        state: str | None = None,
        zone: str | None = None,
    ) -> tuple[float, float] | None:
        """Geocodifica una ubicación. Usa cache y cascada de queries."""
        self.stats["total"] += 1
        key = self._cache_key(city, zone)

        # Check cache
        if key in self.cache:
            self.stats["cached"] += 1
            return self.cache[key]

        # Cascada de queries (más específico → menos)
        queries = []
        if zone and zone.strip():
            queries.append(f"{zone}, {city}, {state or ''}, Mexico")
        if state:
            queries.append(f"{city}, {state}, Mexico")
        queries.append(f"{city}, Mexico")

        for q in queries:
            result = await self._nominatim_query(client, q)
            if result:
                self.cache[key] = result
                self.stats["found"] += 1
                return result

        # No encontrado
        self.cache[key] = None
        self.stats["not_found"] += 1
        return None

    async def geocode_development(
        self,
        client: httpx.AsyncClient,
        dev: dict,
        dry_run: bool = False,
    ) -> dict | None:
        """Geocodifica un desarrollo y lo actualiza en Supabase.
        Retorna dict con lat/lng si exitoso, None si no."""
        city = dev.get("city", "")
        state = dev.get("state", "")
        zone = dev.get("zone", "")
        dev_id = dev.get("id", "")
        dev_name = dev.get("name", "")

        if not city:
            return None

        coords = await self.geocode(client, city, state, zone)
        if not coords:
            logger.debug(f"No coords for {dev_name} ({city}, {state})")
            return None

        lat, lng = coords
        updates = {"lat": lat, "lng": lng}

        if dry_run:
            logger.info(f"[DRY-RUN] Would update {dev_name}: lat={lat}, lng={lng}")
            return updates

        # Single SQL call: UPDATE + LOG combined
        from .supabase_writer import execute_sql, _escape_sql
        portal = dev.get("detection_source", "") or ""
        query = (
            f"UPDATE public.developments SET lat = {lat}, lng = {lng}, updated_at = NOW() "
            f"WHERE id = '{_escape_sql(dev_id)}';\n"
            f"INSERT INTO public.enrichment_log (development_id, development_name, field_name, new_value, strategy, portal) "
            f"VALUES ('{_escape_sql(dev_id)}', '{_escape_sql(dev_name)}', 'lat+lng', '{lat},{lng}', 'geocoding', '{_escape_sql(portal)}')"
        )
        result = await execute_sql(client, query)
        if result is not None:
            logger.info(f"Geocoded {dev_name} ({city}): {lat}, {lng}")
            return updates
        else:
            self.stats["errors"] += 1
            logger.error(f"Failed to update coords for {dev_name}")
            return None

    async def run_batch(
        self,
        client: httpx.AsyncClient,
        developments: list[dict],
        dry_run: bool = False,
        broadcast_fn=None,
    ) -> list[dict]:
        """Geocodifica un batch de desarrollos. Retorna lista de resultados."""
        results = []
        for i, dev in enumerate(developments):
            result = await self.geocode_development(client, dev, dry_run=dry_run)
            if result:
                results.append({"dev": dev, "coords": result})
                if broadcast_fn:
                    await broadcast_fn({
                        "type": "enrichment",
                        "strategy": "geocoding",
                        "dev_name": dev.get("name", ""),
                        "city": dev.get("city", ""),
                        "fields": ["lat", "lng"],
                        "values": result,
                        "processed": i + 1,
                        "total": len(developments),
                    })

            if (i + 1) % 50 == 0:
                self._save_cache()
                logger.info(f"Geocoding progress: {i + 1}/{len(developments)}")

        self._save_cache()
        return results
