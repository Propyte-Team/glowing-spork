"""
Configuracion del Agente de Deduplicacion.
Lee credenciales del .env existente de glowing-spork.
"""

import os

# --- Cargar .env desde la raiz del repo (../../.env) ---
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")
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

# --- Supabase REST headers ---
SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}

# --- Management API ---
MGMT_API_URL = f"https://api.supabase.com/v1/projects/{SUPABASE_PROJECT_REF}/database/query"

# --- Dedup config ---
DEDUP_BATCH_SIZE = 100
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
