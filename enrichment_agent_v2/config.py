"""
Configuración central del Enrichment Agent V2.
Lee credenciales del .env existente de glowing-spork.
"""

import os
import re

# --- Cargar .env desde la raíz del repo (../) ---
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
if os.path.exists(_env_path):
    with open(_env_path, "r") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _key, _val = _line.split("=", 1)
                os.environ.setdefault(_key.strip(), _val.strip())

# --- Supabase ---
SUPABASE_URL = os.environ.get("PROPYTE_SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("PROPYTE_SUPABASE_SERVICE_KEY", "")
SUPABASE_MGMT_TOKEN = os.environ.get("SUPABASE_MGMT_TOKEN", "")
SUPABASE_PROJECT_REF = os.environ.get("SUPABASE_PROJECT_REF", "yjbrynsykkycozeybykj")

# --- APIs ---
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# --- TheRedSearch ---
REDSEARCH_EMAIL = os.environ.get("REDSEARCH_EMAIL", "")
REDSEARCH_PASS = os.environ.get("REDSEARCH_PASS", "")

# --- HTTP ---
HEADERS_HTTP = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
}

# --- Supabase REST headers ---
SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}

# --- Enrichment config ---
BATCH_SIZE = 50
MAX_CONCURRENT = 5
NOMINATIM_DELAY = 1.1  # seconds between Nominatim requests (policy: max 1/sec)
LOOP_INTERVAL_SECONDS = 60
DASHBOARD_PORT = 8080

# --- Campos a enriquecer en orden de prioridad ---
ENRICHMENT_PRIORITY = [
    "price_min_mxn",
    "price_max_mxn",
    "lat",
    "lng",
    "images",
    "contact_phone",
    "description_es",
    "amenities",
    "total_units",
    "delivery_text",
]

# --- Etapa CSV → Supabase stage ---
ETAPA_MAP = {
    "preventa": "preventa",
    "pre-venta": "preventa",
    "lanzamiento": "preventa",
    "proximamente": "preventa",
    "en construccion": "construccion",
    "entrega inmediata": "entrega_inmediata",
}

# --- Mapeo de keywords de amenidades a columnas boolean de Supabase ---
AMENIDAD_MAP = {
    "alberca": "amenidad_alberca_comunitaria",
    "piscina": "amenidad_alberca_comunitaria",
    "pool": "amenidad_alberca_comunitaria",
    "gimnasio": "amenidad_gym",
    "gym": "amenidad_gym",
    "roof garden": "amenidad_rooftop",
    "rooftop": "amenidad_rooftop",
    "coworking": "amenidad_coworking",
    "co-working": "amenidad_coworking",
    "lobby": "amenidad_lobby",
    "seguridad 24": "amenidad_seguridad_24h",
    "vigilancia": "amenidad_seguridad_24h",
    "spa": "amenidad_spa",
    "restaurante": "amenidad_restaurante",
    "concierge": "amenidad_concierge",
    "elevador": "amenidad_elevador",
    "elevator": "amenidad_elevador",
    "bodega": "amenidad_bodega",
    "storage": "amenidad_bodega",
    "pet friendly": "amenidad_pet_zone",
    "dog park": "amenidad_pet_zone",
    "cancha": "amenidad_cancha",
    "paddle": "amenidad_cancha",
    "padel": "amenidad_cancha",
    "tenis": "amenidad_cancha",
    "area ninos": "amenidad_area_ninos",
    "kids": "amenidad_area_ninos",
    "playground": "amenidad_area_ninos",
    "salon de eventos": "amenidad_salon_eventos",
    "salon de fiestas": "amenidad_salon_eventos",
    "jardin": "amenidad_jardin_comunitario",
    "area verde": "amenidad_jardin_comunitario",
    "yoga": "amenidad_yoga",
    "meditacion": "amenidad_yoga",
    "fire pit": "amenidad_fire_pit",
    "asador": "amenidad_fire_pit",
    "grill": "amenidad_fire_pit",
    "acceso controlado": "amenidad_acceso_controlado",
    "cctv": "amenidad_cctv",
}

# --- Amenidades lista de columnas únicas ---
AMENIDAD_COLUMNS = sorted(set(AMENIDAD_MAP.values()))
