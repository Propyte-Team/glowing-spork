"""
Configuración del Publishing Agent.
Hereda credenciales del .env central de glowing-spork.
"""

import os

# --- Cargar .env desde raíz del repo ---
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")
if os.path.exists(_env_path):
    with open(_env_path, "r") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _key, _val = _line.split("=", 1)
                os.environ.setdefault(_key.strip(), _val.strip())

# --- Supabase (mismas vars que enrichment_agent_v2) ---
SUPABASE_URL = os.environ.get("PROPYTE_SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("PROPYTE_SUPABASE_SERVICE_KEY", "")
SUPABASE_MGMT_TOKEN = os.environ.get("SUPABASE_MGMT_TOKEN", "")
SUPABASE_PROJECT_REF = os.environ.get("SUPABASE_PROJECT_REF", "yjbrynsykkycozeybykj")

# --- Anthropic ---
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# --- WordPress ---
WP_SITE_URL = os.environ.get("WP_SITE_URL", "https://propyte.com").rstrip("/")

# --- Publisher thresholds ---
PUBLISH_BATCH_SIZE = int(os.environ.get("PUBLISH_BATCH_SIZE", "20"))
MIN_SCORE_AUTO_PUBLISH = 80   # publish directo, sin AI
MIN_SCORE_AI_REVIEW = 60      # borderline: pedir revisión a Haiku
MIN_DESCRIPTION_LEN = 80      # chars mínimos de description_es
MIN_PRICE_MXN = 500_000       # precio mínimo plausible

# --- Supabase Management API ---
MGMT_API_URL = (
    f"https://api.supabase.com/v1/projects/{SUPABASE_PROJECT_REF}/database/query"
)
