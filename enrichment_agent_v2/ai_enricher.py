"""
Enriquecimiento con Claude Haiku via API directa (httpx, sin SDK).
3 casos: extracción de precio de HTML, generación de descripción, clasificación de amenidades.
"""

import json
import logging

import httpx

from .config import ANTHROPIC_API_KEY, AMENIDAD_COLUMNS
from .supabase_writer import update_development, log_enrichment

logger = logging.getLogger("enrichment_v2.ai_enricher")

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-haiku-4-5-20251001"
MAX_RETRIES = 3


async def _call_haiku(
    client: httpx.AsyncClient,
    system: str,
    user_message: str,
    max_tokens: int = 1024,
) -> str | None:
    """Llama a Claude Haiku y retorna el texto de respuesta."""
    if not ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY not set")
        return None

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    payload = {
        "model": MODEL,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user_message}],
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = await client.post(
                ANTHROPIC_URL,
                headers=headers,
                json=payload,
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("content", [])
                if content and content[0].get("type") == "text":
                    return content[0]["text"]
                return None
            elif resp.status_code == 429:
                # Rate limited — backoff
                wait = 2 ** (attempt + 1)
                logger.warning(f"Rate limited, waiting {wait}s...")
                import asyncio
                await asyncio.sleep(wait)
                continue
            else:
                logger.error(f"Haiku API {resp.status_code}: {resp.text[:200]}")
                return None
        except Exception as e:
            logger.error(f"Haiku API error (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                import asyncio
                await asyncio.sleep(2 ** (attempt + 1))
    return None


async def extract_price_from_html(
    client: httpx.AsyncClient, html_snippet: str, dev_name: str, city: str
) -> dict:
    """Usa Haiku para extraer precio, unidades y entrega de HTML crudo."""
    system = (
        "Eres un asistente que extrae datos de páginas de desarrollos inmobiliarios en México. "
        "Responde SOLO con JSON válido, sin explicaciones ni markdown."
    )
    user_msg = (
        f"De este texto de una página de desarrollo inmobiliario en {city}, "
        f"proyecto '{dev_name}', extrae:\n"
        f"- precio mínimo en MXN (entero)\n"
        f"- precio máximo en MXN (entero)\n"
        f"- número total de unidades (entero)\n"
        f"- fecha de entrega estimada (texto)\n"
        f"\nResponde SOLO en JSON con este formato:\n"
        f'{{"price_min": int|null, "price_max": int|null, "total_units": int|null, '
        f'"delivery_text": str|null}}\n\n'
        f"Texto (primeros 4000 chars):\n{html_snippet[:4000]}"
    )

    response = await _call_haiku(client, system, user_msg, max_tokens=256)
    if not response:
        return {}

    try:
        # Limpiar posible markdown
        text = response.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text
            text = text.rsplit("```", 1)[0]
        data = json.loads(text)

        result = {}
        if data.get("price_min") and isinstance(data["price_min"], (int, float)):
            val = int(data["price_min"])
            if val > 100_000:  # Sanity check
                result["price_min_mxn"] = val
        if data.get("price_max") and isinstance(data["price_max"], (int, float)):
            val = int(data["price_max"])
            if val > 100_000:
                result["price_max_mxn"] = val
        if data.get("total_units") and isinstance(data["total_units"], int):
            if 2 <= data["total_units"] <= 5000:
                result["total_units"] = data["total_units"]
        if data.get("delivery_text") and isinstance(data["delivery_text"], str):
            result["delivery_text"] = data["delivery_text"][:100]

        return result
    except (json.JSONDecodeError, TypeError, KeyError) as e:
        logger.warning(f"Failed to parse Haiku price response: {e}")
        return {}


async def generate_description(
    client: httpx.AsyncClient, dev: dict
) -> str | None:
    """Genera descripción en español para un desarrollo."""
    name = dev.get("name", "")
    city = dev.get("city", "")
    state = dev.get("state", "")
    stage = dev.get("stage", "")
    price_min = dev.get("price_min_mxn")
    price_max = dev.get("price_max_mxn")
    units = dev.get("total_units")
    amenities = dev.get("amenities")
    surface = dev.get("surface_min_m2")

    # Construir contexto
    context_parts = [f"Nombre: {name}", f"Ciudad: {city}, {state}"]
    if stage:
        context_parts.append(f"Etapa: {stage}")
    if price_min:
        price_str = f"Desde ${price_min:,.0f} MXN"
        if price_max and price_max != price_min:
            price_str += f" hasta ${price_max:,.0f} MXN"
        context_parts.append(f"Precio: {price_str}")
    if units:
        context_parts.append(f"Unidades totales: {units}")
    if amenities and isinstance(amenities, list):
        context_parts.append(f"Amenidades: {', '.join(amenities[:10])}")
    if surface:
        context_parts.append(f"Superficie desde: {surface} m²")

    system = (
        "Eres un copywriter inmobiliario mexicano profesional. "
        "Escribes descripciones comerciales atractivas, concisas y orientadas a compradores mexicanos. "
        "Tono profesional y aspiracional. No inventes datos que no tengas."
    )
    user_msg = (
        f"Escribe una descripción comercial de 100-150 palabras para este desarrollo inmobiliario:\n\n"
        f"{chr(10).join(context_parts)}\n\n"
        f"La descripción debe ser atractiva, resaltar ubicación y características principales. "
        f"No incluyas saltos de línea excesivos. Solo el texto de la descripción, sin comillas."
    )

    response = await _call_haiku(client, system, user_msg, max_tokens=512)
    if response:
        # Limpiar respuesta
        text = response.strip().strip('"').strip("'")
        if 50 <= len(text) <= 2000:
            return text
    return None


async def classify_amenities(
    client: httpx.AsyncClient, text: str
) -> dict[str, bool]:
    """Clasifica amenidades desde texto libre usando Haiku."""
    amenity_names = [col.replace("amenidad_", "").replace("_", " ") for col in AMENIDAD_COLUMNS]

    system = (
        "Eres un clasificador de amenidades de desarrollos inmobiliarios. "
        "Responde SOLO con JSON válido."
    )
    user_msg = (
        f"Del siguiente texto de un desarrollo inmobiliario, identifica cuáles amenidades aplican "
        f"de esta lista: {', '.join(amenity_names)}.\n\n"
        f"Responde SOLO en JSON: un objeto con las amenidades encontradas como keys (en el formato "
        f"original con guiones bajos, prefijo amenidad_) y valor true.\n"
        f"Ejemplo: {{\"amenidad_alberca_comunitaria\": true, \"amenidad_gym\": true}}\n\n"
        f"Texto:\n{text[:3000]}"
    )

    response = await _call_haiku(client, system, user_msg, max_tokens=256)
    if not response:
        return {}

    try:
        text_clean = response.strip()
        if text_clean.startswith("```"):
            text_clean = text_clean.split("\n", 1)[1] if "\n" in text_clean else text_clean
            text_clean = text_clean.rsplit("```", 1)[0]
        data = json.loads(text_clean)
        # Validar que solo incluya columnas válidas
        return {k: True for k, v in data.items() if k in AMENIDAD_COLUMNS and v is True}
    except (json.JSONDecodeError, TypeError):
        logger.warning("Failed to parse Haiku amenities response")
        return {}


async def enrich_with_ai(
    client: httpx.AsyncClient,
    dev: dict,
    dry_run: bool = False,
) -> dict | None:
    """Enriquece un desarrollo con AI (descripción + amenidades si aplica)."""
    dev_id = dev.get("id", "")
    dev_name = dev.get("name", "")
    enrichments = {}

    # Generar descripción si falta
    if dev.get("description_es") is None and dev_name:
        desc = await generate_description(client, dev)
        if desc:
            enrichments["description_es"] = desc

    if not enrichments:
        return None

    if dry_run:
        logger.info(f"[DRY-RUN] Would AI-enrich {dev_name}: {list(enrichments.keys())}")
        return enrichments

    from .supabase_writer import execute_sql, build_set_clause, _escape_sql
    set_clause = build_set_clause(enrichments)
    fields_str = ",".join(enrichments.keys())
    query = (
        f"UPDATE public.developments SET {set_clause}, updated_at = NOW() "
        f"WHERE id = '{_escape_sql(dev_id)}';\n"
        f"INSERT INTO public.enrichment_log (development_id, development_name, field_name, strategy) "
        f"VALUES ('{_escape_sql(dev_id)}', '{_escape_sql(dev_name)}', '{_escape_sql(fields_str)}', 'ai_haiku')"
    )
    result = await execute_sql(client, query)
    if result is not None:
        logger.info(f"AI enriched {dev_name}: {list(enrichments.keys())}")
        return enrichments
    return None


async def run_ai_batch(
    client: httpx.AsyncClient,
    developments: list[dict],
    dry_run: bool = False,
    broadcast_fn=None,
) -> list[dict]:
    """Enriquece un batch de desarrollos con AI."""
    results = []
    for i, dev in enumerate(developments):
        result = await enrich_with_ai(client, dev, dry_run=dry_run)
        if result:
            results.append({"dev": dev, "enrichments": result})
            if broadcast_fn:
                await broadcast_fn({
                    "type": "enrichment",
                    "strategy": "ai_haiku",
                    "dev_name": dev.get("name", ""),
                    "city": dev.get("city", ""),
                    "fields": list(result.keys()),
                    "processed": i + 1,
                    "total": len(developments),
                })

        if (i + 1) % 10 == 0:
            logger.info(f"AI enrichment progress: {i + 1}/{len(developments)}")

    return results
