#!/usr/bin/env python3
"""
Agente de Enriquecimiento de Datos Inmobiliarios + Supabase Upload
===================================================================
Busca datos faltantes de desarrollos en lanzamientos.csv, los enriquece
con busqueda web + parsing, y los sube directo a Supabase.

Requisitos (env vars):
    PROPYTE_SUPABASE_URL=https://yjbrynsykkcozeybykj.supabase.co
    PROPYTE_SUPABASE_SERVICE_KEY=eyJ...  (service_role key para writes)

Uso:
    python agente_enriquecimiento.py                      # Enriquecer + subir TODO
    python agente_enriquecimiento.py --limit 10            # Solo primeros 10
    python agente_enriquecimiento.py --ciudad CDMX         # Solo una ciudad
    python agente_enriquecimiento.py --dry-run             # Solo diagnostico
    python agente_enriquecimiento.py --no-upload           # Enriquecer sin subir
    python agente_enriquecimiento.py --only-upload         # Subir log existente
    python agente_enriquecimiento.py --match-existing      # Match con desarrollos existentes
"""

import csv
import json
import os
import re
import sys
import time
import random
import argparse
from datetime import datetime
from urllib.parse import quote_plus, urljoin

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    os.system("pip install requests beautifulsoup4")
    import requests
    from bs4 import BeautifulSoup

# Fix encoding Windows (cp1252 no soporta todos los caracteres Unicode)
if sys.stdout.encoding != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Cargar .env si existe
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path, "r") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _key, _val = _line.split("=", 1)
                os.environ.setdefault(_key.strip(), _val.strip())

# === Configuracion ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LANZAMIENTOS_CSV = os.path.join(BASE_DIR, "lanzamientos.csv")
ENRICHED_CSV = os.path.join(BASE_DIR, "lanzamientos_enriched.csv")
ENRICHMENT_LOG = os.path.join(BASE_DIR, "enrichment_log.json")

# Supabase
SUPABASE_URL = os.environ.get("PROPYTE_SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("PROPYTE_SUPABASE_SERVICE_KEY", "")
SUPABASE_SCHEMA = "public"
TABLE_DESARROLLOS = "developments"
TABLE_DESARROLLADORES = "developers"

ENRICHED_HEADERS = [
    "nombre_proyecto", "desarrolladora", "ciudad", "estado_republica", "zona",
    "tipo_desarrollo", "tipo_unidades", "rango_precios", "num_unidades",
    "fecha_entrega_estimada", "etapa", "url_fuente", "portal_fuente",
    "fecha_deteccion", "notas",
    "amenidades", "descripcion", "latitud", "longitud",
    "superficie_min_m2", "superficie_max_m2",
    "precio_min_mxn", "precio_max_mxn",
    "imagen_url", "telefono_contacto", "email_contacto",
    "sitio_web_proyecto", "fecha_enriquecimiento", "fuente_enriquecimiento",
    "confianza_datos",
]

CAMPOS_CRITICOS = [
    "desarrolladora", "rango_precios", "num_unidades",
    "fecha_entrega_estimada", "zona",
]

CAMPOS_ENRIQUECIMIENTO = [
    "amenidades", "descripcion", "superficie_min_m2", "superficie_max_m2",
    "precio_min_mxn", "precio_max_mxn", "imagen_url",
    "latitud", "longitud", "sitio_web_proyecto",
]

# Mapeo de keywords de amenidades a columnas boolean de Supabase
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

HEADERS_HTTP = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
}

# Etapa del CSV -> etapa_construccion de Supabase
ETAPA_MAP = {
    "preventa": "preventa",
    "pre-venta": "preventa",
    "lanzamiento": "preventa",
    "proximamente": "preventa",
    "en construccion": "construccion",
    "entrega inmediata": "entrega_inmediata",
}


# === Utilidades ===============================================================

def delay(min_s=2, max_s=4):
    time.sleep(random.uniform(min_s, max_s))


def normalizar(texto):
    if not texto:
        return ""
    import unicodedata
    texto = unicodedata.normalize("NFKD", texto.lower().strip())
    texto = re.sub(r"[\u0300-\u036f]", "", texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto


def campo_vacio(valor):
    if valor is None:
        return True
    s = str(valor).strip()
    return s == "" or s.lower() in ("nan", "n/a", "none", "sin datos")


def calcular_faltantes(row):
    return [c for c in CAMPOS_CRITICOS if campo_vacio(row.get(c))]


def cargar_log():
    if os.path.exists(ENRICHMENT_LOG):
        with open(ENRICHMENT_LOG, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def guardar_log(log):
    with open(ENRICHMENT_LOG, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)


# === Supabase REST API ========================================================

class SupabaseClient:
    """Cliente ligero para Supabase REST API (PostgREST)."""

    def __init__(self, url, key, schema="real_estate_hub"):
        self.base_url = url.rstrip("/")
        self.key = key
        self.schema = schema
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Accept-Profile": schema,
            "Content-Profile": schema,
        }
        self._session = requests.Session()
        self._session.headers.update(self.headers)

    def test_connection(self):
        """Verifica la conexion a Supabase."""
        try:
            url = f"{self.base_url}/rest/v1/{TABLE_DESARROLLOS}?select=id&limit=1"
            resp = self._session.get(url, timeout=10)
            if resp.status_code == 200:
                return True, f"OK - tabla {TABLE_DESARROLLOS} accesible"
            return False, f"Error {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            return False, str(e)

    def get_existing_developments(self):
        """Carga desarrollos existentes para matching."""
        url = f"{self.base_url}/rest/v1/{TABLE_DESARROLLOS}"
        params = {
            "select": "id,name,city,state,source_url,detection_source,slug",
            "deleted_at": "is.null",
            "limit": 5000,
        }
        resp = self._session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_developer_id(self, developer_name):
        """Busca un desarrollador por nombre y retorna su ID. Crea uno si no existe."""
        if not developer_name or campo_vacio(developer_name):
            return None

        # Buscar existente
        url = f"{self.base_url}/rest/v1/{TABLE_DESARROLLADORES}"
        params = {
            "select": "id,name",
            "name": f"ilike.%{developer_name}%",
            "limit": 1,
        }
        resp = self._session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                return data[0]["id"]

        # Crear nuevo desarrollador
        slug = normalizar(developer_name).replace(" ", "-")[:100]
        payload = {
            "name": developer_name,
            "slug": slug,
        }
        resp = self._session.post(
            url,
            json=payload,
            headers={**self.headers, "Prefer": "return=representation"},
            timeout=10,
        )
        if resp.status_code == 201:
            data = resp.json()
            if data:
                print(f"      + Desarrollador creado: {developer_name}")
                return data[0]["id"]

        return None

    def upsert_development(self, row):
        """Inserta o actualiza un desarrollo en Supabase."""
        url = f"{self.base_url}/rest/v1/{TABLE_DESARROLLOS}"
        headers = {
            **self.headers,
            "Prefer": "resolution=merge-duplicates,return=representation",
        }
        resp = self._session.post(url, json=row, headers=headers, timeout=15)
        return resp.status_code, resp.text[:300] if resp.status_code >= 400 else ""

    def update_development(self, dev_id, data):
        """Actualiza campos de un desarrollo existente (solo campos no nulos)."""
        url = f"{self.base_url}/rest/v1/{TABLE_DESARROLLOS}?id=eq.{dev_id}"
        headers = {
            **self.headers,
            "Prefer": "return=representation",
        }
        resp = self._session.patch(url, json=data, headers=headers, timeout=15)
        return resp.status_code, resp.text[:300] if resp.status_code >= 400 else ""

    def insert_development(self, data):
        """Inserta un nuevo desarrollo."""
        url = f"{self.base_url}/rest/v1/{TABLE_DESARROLLOS}"
        headers = {
            **self.headers,
            "Prefer": "return=representation",
        }
        resp = self._session.post(url, json=data, headers=headers, timeout=15)
        return resp.status_code, resp.text[:300] if resp.status_code >= 400 else ""


# === Motor de Busqueda Web ====================================================

def buscar_duckduckgo(session, query, max_results=8):
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    headers = {**HEADERS_HTTP, "Referer": "https://duckduckgo.com/"}
    try:
        resp = session.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        resultados = []
        for result in soup.select("div.result, div.web-result")[:max_results]:
            titulo_el = result.select_one("a.result__a, h2 a")
            snippet_el = result.select_one("a.result__snippet, .result__snippet")
            if not titulo_el:
                continue
            titulo = titulo_el.get_text(strip=True)
            link = titulo_el.get("href", "")
            snippet = snippet_el.get_text(strip=True) if snippet_el else ""
            if "uddg=" in link:
                match = re.search(r"uddg=([^&]+)", link)
                if match:
                    from urllib.parse import unquote
                    link = unquote(match.group(1))
            resultados.append({"titulo": titulo, "url": link, "snippet": snippet})
        return resultados
    except Exception as e:
        print(f"    Error DuckDuckGo: {e}")
        return []


def fetch_page(url, session, timeout=15):
    try:
        resp = session.get(url, headers=HEADERS_HTTP, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"    Error fetching {url[:60]}: {e}")
        return None


# === Extractores de Datos =====================================================

def extraer_precio(texto):
    texto = texto.lower().replace(",", "").replace(" ", "")
    precios_mxn = re.findall(r"\$(\d{6,9})", texto)
    if precios_mxn:
        nums = sorted([int(p) for p in precios_mxn])
        return nums[0], nums[-1] if len(nums) > 1 else nums[0]
    mdp = re.findall(r"(\d+\.?\d*)\s*(?:mdp|millones)", texto)
    if mdp:
        nums = sorted([float(m) * 1_000_000 for m in mdp])
        return int(nums[0]), int(nums[-1]) if len(nums) > 1 else int(nums[0])
    desde = re.search(r"desde\s*\$?\s*(\d[\d,.]*)", texto)
    if desde:
        val = int(desde.group(1).replace(",", "").replace(".", ""))
        if val < 1000:
            val = val * 1_000_000
        return val, None
    return None, None


def extraer_precio_de_rango(rango_str):
    """Parsea el campo rango_precios del CSV (ej: 'Desde $2,727,000 MXN')."""
    if campo_vacio(rango_str):
        return None, None
    texto = str(rango_str).replace(",", "").replace(" ", "").lower()
    nums = re.findall(r"\$?(\d{6,10})", texto)
    if nums:
        sorted_nums = sorted([int(n) for n in nums])
        return sorted_nums[0], sorted_nums[-1] if len(sorted_nums) > 1 else sorted_nums[0]
    return None, None


def extraer_superficie(texto):
    texto = texto.lower()
    rango = re.findall(r"(\d+\.?\d*)\s*(?:m2|m\xb2|metros?\s*cuadrados?)", texto)
    if rango:
        nums = sorted([float(s) for s in rango])
        nums = [n for n in nums if 10 <= n <= 2000]
        if nums:
            return nums[0], nums[-1] if len(nums) > 1 else nums[0]
    return None, None


def extraer_num_unidades(texto):
    texto = texto.lower()
    patterns = [
        r"(\d+)\s*(?:unidades|departamentos|deptos?|casas|lotes|locales|oficinas)",
        r"(?:total|inventario|proyecto)\s*(?:de)?\s*(\d+)\s*(?:unidades)?",
    ]
    for pattern in patterns:
        match = re.search(pattern, texto)
        if match:
            num = int(match.group(1))
            if 2 <= num <= 5000:
                return num
    return None


def extraer_desarrolladora(texto):
    # Palabras que NO son desarrolladoras (falsos positivos comunes)
    blacklist = [
        "sin antecedentes", "sin sanciones", "sin problema", "no aplica",
        "no disponible", "sin informacion", "no especificado", "ver mas",
        "leer mas", "mas informacion", "conoce mas", "no identificado",
        "se encuentra", "ubicado", "complejo", "se ubica", "el proyecto",
        "el desarrollo", "cuenta con", "ofrece", "incluye", "dispone",
        "diseñado", "construido en", "localizado",
    ]

    patterns = [
        r"(?:desarrollad(?:or|ora)|developer)\s*:?\s*([A-Z\u00c1-\u00da][a-z\u00e1-\u00fa\u00f1A-Z\u00c1-\u00da\u00d1\s&.,]+?)(?:\.|,|\n|$)",
        r"(?:grupo|constructora|inmobiliaria)\s+([A-Z\u00c1-\u00da][a-z\u00e1-\u00fa\u00f1A-Z\u00c1-\u00da\u00d1\s&]+?)(?:\.|,|\n|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, texto, re.IGNORECASE)
        if match:
            nombre = match.group(1).strip()
            if 3 <= len(nombre) <= 60:
                # Verificar contra blacklist
                nombre_lower = nombre.lower()
                if any(bl in nombre_lower for bl in blacklist):
                    continue
                # Verificar que tenga al menos una palabra que parezca nombre propio
                if re.search(r"[A-Z\u00c1-\u00da]", nombre):
                    return nombre
    return None


def extraer_amenidades(texto):
    texto = normalizar(texto)
    amenidades_encontradas = {}
    for keyword, col in AMENIDAD_MAP.items():
        if keyword in texto:
            amenidades_encontradas[col] = True
    return amenidades_encontradas


def extraer_amenidades_lista(texto):
    """Version texto para el CSV."""
    texto = texto.lower()
    amenidades_conocidas = [
        "alberca", "piscina", "gimnasio", "gym", "roof garden", "rooftop",
        "coworking", "lobby", "seguridad 24", "spa", "restaurante",
        "concierge", "elevador", "bodega", "pet friendly", "cancha",
        "area ninos", "yoga", "fire pit", "asador", "salon de eventos",
    ]
    encontradas = [a.capitalize() for a in amenidades_conocidas if a in texto]
    return ", ".join(sorted(set(encontradas))) if encontradas else ""


def extraer_fecha_entrega(texto):
    texto = texto.lower()
    q_match = re.search(r"(?:q|t)(\d)\s*(?:de\s*)?(\d{4})", texto)
    if q_match:
        return f"{q_match.group(2)}-Q{q_match.group(1)}"
    year_match = re.search(r"(?:entrega|delivery|terminacion)\s*(?:en|:)?\s*(\d{4})", texto)
    if year_match:
        return year_match.group(1)
    sem_match = re.search(r"(primer|segundo)\s*semestre\s*(\d{4})", texto)
    if sem_match:
        q = "Q2" if sem_match.group(1) == "primer" else "Q4"
        return f"{sem_match.group(2)}-{q}"
    meses = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
    }
    for mes, num in meses.items():
        m = re.search(rf"{mes}\s*(?:de\s*)?(\d{{4}})", texto)
        if m:
            return f"{m.group(1)}-{num}"
    return None


def extraer_contacto(texto):
    telefono = None
    email = None
    tel_match = re.search(r"(?:\+?52\s*)?(?:\(?\d{2,3}\)?\s*)?(\d{4}[\s-]?\d{4})", texto)
    if tel_match:
        telefono = tel_match.group(0).strip()
    email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", texto)
    if email_match:
        email = email_match.group(0).strip()
    return telefono, email


def extraer_imagen(soup, base_url=""):
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return og["content"]
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if not src:
            continue
        if any(skip in src.lower() for skip in ["logo", "icon", "avatar", "pixel", "tracking"]):
            continue
        width = img.get("width")
        if width and str(width).isdigit() and int(width) < 200:
            continue
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = urljoin(base_url, src)
        return src
    return None


def extraer_coordenadas(texto):
    coord_match = re.search(r"(-?\d{1,3}\.\d{4,8})\s*,\s*(-?\d{1,3}\.\d{4,8})", texto)
    if coord_match:
        lat = float(coord_match.group(1))
        lng = float(coord_match.group(2))
        if 14 <= lat <= 33 and -118 <= lng <= -86:
            return lat, lng
    return None, None


# === Estrategias de Busqueda ==================================================

def estrategia_url_fuente(session, proyecto):
    url = proyecto.get("url_fuente", "")
    if not url or campo_vacio(url):
        return {}

    print(f"    URL fuente: {url[:60]}...")
    html = fetch_page(url, session)
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    texto = soup.get_text(separator=" ", strip=True)
    datos = {}

    if campo_vacio(proyecto.get("rango_precios")):
        p_min, p_max = extraer_precio(texto)
        if p_min:
            datos["precio_min_mxn"] = p_min
            if p_max:
                datos["precio_max_mxn"] = p_max
            datos["rango_precios"] = f"Desde ${p_min:,.0f} MXN" + (f" hasta ${p_max:,.0f}" if p_max else "")

    if campo_vacio(proyecto.get("desarrolladora")):
        dev = extraer_desarrolladora(texto)
        if dev:
            datos["desarrolladora"] = dev

    if campo_vacio(proyecto.get("num_unidades")):
        num = extraer_num_unidades(texto)
        if num:
            datos["num_unidades"] = num

    if campo_vacio(proyecto.get("fecha_entrega_estimada")):
        fecha = extraer_fecha_entrega(texto)
        if fecha:
            datos["fecha_entrega_estimada"] = fecha

    sup_min, sup_max = extraer_superficie(texto)
    if sup_min:
        datos["superficie_min_m2"] = sup_min
        if sup_max:
            datos["superficie_max_m2"] = sup_max

    datos["amenidades_bool"] = extraer_amenidades(texto)
    amenidades_txt = extraer_amenidades_lista(texto)
    if amenidades_txt:
        datos["amenidades"] = amenidades_txt

    img = extraer_imagen(soup, url)
    if img:
        datos["imagen_url"] = img

    for script in soup.find_all("script"):
        script_text = script.string or ""
        lat, lng = extraer_coordenadas(script_text)
        if lat:
            datos["latitud"] = lat
            datos["longitud"] = lng
            break

    tel, email = extraer_contacto(texto)
    if tel:
        datos["telefono_contacto"] = tel
    if email:
        datos["email_contacto"] = email

    for p in soup.find_all("p"):
        p_text = p.get_text(strip=True)
        if len(p_text) > 80 and any(kw in p_text.lower() for kw in [
            "desarrollo", "proyecto", "departamento", "residencial",
            "ubicado", "inversio", "preventa"
        ]):
            datos["descripcion"] = p_text[:500]
            break

    return datos


def estrategia_busqueda_web(session, proyecto):
    nombre = proyecto.get("nombre_proyecto", "")
    ciudad = proyecto.get("ciudad", "")
    if not nombre:
        return {}

    queries = [f'"{nombre}" {ciudad} desarrollo inmobiliario']
    if campo_vacio(proyecto.get("rango_precios")):
        queries.append(f'"{nombre}" {ciudad} precio preventa')

    datos = {}
    for query in queries[:2]:
        print(f"    Buscando: {query[:50]}...")
        resultados = buscar_duckduckgo(session, query, max_results=5)

        for r in resultados:
            texto = f"{r['titulo']} {r['snippet']}"
            if campo_vacio(datos.get("desarrolladora")) and campo_vacio(proyecto.get("desarrolladora")):
                dev = extraer_desarrolladora(texto)
                if dev:
                    datos["desarrolladora"] = dev
            if campo_vacio(datos.get("rango_precios")) and campo_vacio(proyecto.get("rango_precios")):
                p_min, p_max = extraer_precio(texto)
                if p_min:
                    datos["precio_min_mxn"] = p_min
                    if p_max:
                        datos["precio_max_mxn"] = p_max
                    datos["rango_precios"] = f"Desde ${p_min:,.0f} MXN"
            if campo_vacio(datos.get("num_unidades")) and campo_vacio(proyecto.get("num_unidades")):
                num = extraer_num_unidades(texto)
                if num:
                    datos["num_unidades"] = num
            if campo_vacio(datos.get("fecha_entrega_estimada")) and campo_vacio(proyecto.get("fecha_entrega_estimada")):
                fecha = extraer_fecha_entrega(texto)
                if fecha:
                    datos["fecha_entrega_estimada"] = fecha
            url_r = r.get("url", "")
            if url_r and not campo_vacio(url_r):
                portales = ["lahaus", "inmuebles24", "lamudi", "behome", "casasyterrenos",
                           "duckduckgo", "google", "trovit"]
                if not any(p in url_r.lower() for p in portales):
                    datos["sitio_web_proyecto"] = url_r

        delay(3, 5)
    return datos


def estrategia_portal_detalle(session, proyecto):
    url = proyecto.get("url_fuente", "")
    if not url or campo_vacio(url):
        return {}
    if "lahaus.mx" not in url and "theredsearch" not in url:
        return {}

    print(f"    Portal detalle: {url[:60]}...")
    html = fetch_page(url, session)
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    datos = {}

    if "lahaus" in url:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string)
                if isinstance(ld, dict):
                    if ld.get("@type") in ("Product", "RealEstateListing", "Residence"):
                        if "offers" in ld:
                            offers = ld["offers"]
                            if isinstance(offers, dict):
                                if "lowPrice" in offers:
                                    datos["precio_min_mxn"] = int(float(offers["lowPrice"]))
                                if "highPrice" in offers:
                                    datos["precio_max_mxn"] = int(float(offers["highPrice"]))
                        if "geo" in ld:
                            geo = ld["geo"]
                            if "latitude" in geo:
                                datos["latitud"] = float(geo["latitude"])
                                datos["longitud"] = float(geo["longitude"])
                        if "description" in ld:
                            datos["descripcion"] = str(ld["description"])[:500]
                        if "image" in ld:
                            img = ld["image"]
                            if isinstance(img, list):
                                img = img[0]
                            if isinstance(img, str):
                                datos["imagen_url"] = img
            except (json.JSONDecodeError, TypeError, KeyError):
                continue
    return datos


# === Mapeo a Supabase =========================================================

def proyecto_a_supabase(proyecto_enriched, developer_id=None):
    """Convierte un proyecto enriquecido al schema de public.developments."""
    nombre = proyecto_enriched.get("nombre_proyecto", "")
    slug = normalizar(nombre).replace(" ", "-")[:200]

    # Parsear precios del campo rango_precios si no hay precio_min
    precio_min = proyecto_enriched.get("precio_min_mxn")
    precio_max = proyecto_enriched.get("precio_max_mxn")
    if not precio_min:
        precio_min, precio_max = extraer_precio_de_rango(proyecto_enriched.get("rango_precios"))

    # Etapa -> stage
    etapa_raw = normalizar(proyecto_enriched.get("etapa", ""))
    etapa_sb = ETAPA_MAP.get(etapa_raw, "preventa")

    # Num unidades
    num_uds = proyecto_enriched.get("num_unidades")
    if num_uds and not campo_vacio(num_uds):
        try:
            num_uds = int(float(str(num_uds)))
        except (ValueError, TypeError):
            num_uds = None
    else:
        num_uds = None

    row = {
        "name": nombre,
        "slug": slug,
        "city": proyecto_enriched.get("ciudad", ""),
        "state": proyecto_enriched.get("estado_republica", ""),
        "zone": proyecto_enriched.get("zona", "") if not campo_vacio(proyecto_enriched.get("zona")) else None,
        "stage": etapa_sb,
        "delivery_text": proyecto_enriched.get("fecha_entrega_estimada", "") if not campo_vacio(proyecto_enriched.get("fecha_entrega_estimada")) else None,
        "total_units": num_uds,
        "price_min_mxn": precio_min,
        "price_max_mxn": precio_max if precio_max and precio_max != precio_min else None,
        "currency": "MXN",
        "source_url": proyecto_enriched.get("url_fuente", ""),
        "detection_source": proyecto_enriched.get("portal_fuente", ""),
        "detected_at": proyecto_enriched.get("fecha_deteccion", "") + "T00:00:00Z" if not campo_vacio(proyecto_enriched.get("fecha_deteccion")) else None,
        "published": True,
        "status": "ACTIVO",
    }

    # Desarrollador
    if developer_id:
        row["developer_id"] = developer_id

    # Descripcion
    desc = proyecto_enriched.get("descripcion")
    if desc and not campo_vacio(desc):
        row["description_es"] = str(desc)[:2000]

    # Coordenadas
    lat = proyecto_enriched.get("latitud")
    lng = proyecto_enriched.get("longitud")
    if lat and lng:
        try:
            row["lat"] = float(lat)
            row["lng"] = float(lng)
        except (ValueError, TypeError):
            pass

    # Imagen
    img = proyecto_enriched.get("imagen_url")
    if img and not campo_vacio(img):
        row["images"] = [str(img)]

    # Contacto
    tel = proyecto_enriched.get("telefono_contacto")
    if tel and not campo_vacio(tel):
        row["contact_phone"] = str(tel)

    # Amenidades como array
    amenidades_list = []
    amenidades_bool = proyecto_enriched.get("amenidades_bool", {})
    if isinstance(amenidades_bool, dict):
        for keyword, col in amenidades_bool.items():
            amenidades_list.append(keyword.replace("amenidad_", "").replace("_", " "))
    amenidades_txt = proyecto_enriched.get("amenidades", "")
    if amenidades_txt and not campo_vacio(amenidades_txt):
        for a in str(amenidades_txt).split(","):
            a = a.strip().lower()
            if a and a not in amenidades_list:
                amenidades_list.append(a)
    if amenidades_list:
        row["amenities"] = amenidades_list

    # Tipo desarrollo -> property_types
    tipo = proyecto_enriched.get("tipo_unidades", "")
    if not campo_vacio(tipo):
        tipo_lower = tipo.lower()
        if "casa" in tipo_lower:
            row["property_types"] = ["casa"]
        elif "lote" in tipo_lower or "terreno" in tipo_lower:
            row["property_types"] = ["terreno"]
        elif "oficina" in tipo_lower:
            row["property_types"] = ["oficina"]
        elif "local" in tipo_lower or "comercial" in tipo_lower:
            row["property_types"] = ["comercial"]
        else:
            row["property_types"] = ["departamento"]

    # Limpiar None values
    row = {k: v for k, v in row.items() if v is not None and not campo_vacio(str(v) if not isinstance(v, (list, bool, int, float)) else "x")}

    return row


# === Matching con existentes ==================================================

def match_desarrollo(proyecto, existentes):
    """Busca si el proyecto ya existe en Supabase. Retorna ID o None."""
    nombre = normalizar(proyecto.get("nombre_proyecto", ""))
    ciudad = normalizar(proyecto.get("ciudad", ""))
    url = proyecto.get("url_fuente", "")

    for dev in existentes:
        # Match por URL
        if url and dev.get("source_url") and url == dev["source_url"]:
            return dev["id"]

        # Match por nombre + ciudad
        dev_nombre = normalizar(dev.get("name", ""))
        dev_ciudad = normalizar(dev.get("city", ""))

        if dev_nombre and nombre:
            # Match exacto
            if dev_nombre == nombre and dev_ciudad == ciudad:
                return dev["id"]
            # Match parcial (el nombre del desarrollo contiene el nombre del proyecto)
            if len(nombre) > 5 and (nombre in dev_nombre or dev_nombre in nombre):
                if ciudad == dev_ciudad or not dev_ciudad:
                    return dev["id"]

    return None


# === Orquestador ==============================================================

def enriquecer_proyecto(session, proyecto, log):
    nombre = proyecto.get("nombre_proyecto", "")
    ciudad = proyecto.get("ciudad", "")
    clave = normalizar(f"{nombre}|{ciudad}")

    if clave in log:
        prev = log[clave]
        if prev.get("status") == "ok" and prev.get("datos"):
            print(f"  [SKIP] {nombre} ({ciudad}) -- ya enriquecido")
            return prev.get("datos", {})

    print(f"\n  [{nombre}] ({ciudad}) -- {len(calcular_faltantes(proyecto))} campos faltantes")
    faltantes = calcular_faltantes(proyecto)
    if faltantes:
        print(f"    Faltantes: {', '.join(faltantes)}")

    datos_combinados = {}

    # Estrategia 1: URL fuente
    try:
        datos1 = estrategia_url_fuente(session, proyecto)
        datos_combinados.update(datos1)
        if datos1:
            print(f"    URL fuente: +{len(datos1)} campos")
    except Exception as e:
        print(f"    Error URL fuente: {e}")

    delay(1, 2)

    # Estrategia 2: Busqueda web
    faltantes_despues = [c for c in CAMPOS_CRITICOS
                         if campo_vacio(datos_combinados.get(c)) and campo_vacio(proyecto.get(c))]
    if faltantes_despues:
        try:
            datos2 = estrategia_busqueda_web(session, proyecto)
            for k, v in datos2.items():
                if k not in datos_combinados:
                    datos_combinados[k] = v
            if datos2:
                print(f"    Busqueda web: +{len(datos2)} campos")
        except Exception as e:
            print(f"    Error busqueda web: {e}")

    delay(1, 2)

    # Estrategia 3: Portal detalle
    try:
        datos3 = estrategia_portal_detalle(session, proyecto)
        for k, v in datos3.items():
            if k not in datos_combinados:
                datos_combinados[k] = v
        if datos3:
            print(f"    Portal detalle: +{len(datos3)} campos")
    except Exception as e:
        print(f"    Error portal detalle: {e}")

    campos_encontrados = len([k for k, v in datos_combinados.items()
                              if not campo_vacio(v) and k != "amenidades_bool"])
    confianza = "alta" if campos_encontrados >= 5 else "media" if campos_encontrados >= 2 else "baja"

    datos_combinados["confianza_datos"] = confianza
    datos_combinados["fecha_enriquecimiento"] = datetime.now().strftime("%Y-%m-%d")
    datos_combinados["fuente_enriquecimiento"] = "agente_enriquecimiento_v2"

    log[clave] = {
        "nombre": nombre,
        "ciudad": ciudad,
        "status": "ok",
        "campos_encontrados": campos_encontrados,
        "datos": {k: v for k, v in datos_combinados.items() if k != "amenidades_bool"},
        "timestamp": datetime.now().isoformat(),
    }

    print(f"    Total: +{campos_encontrados} campos ({confianza})")
    return datos_combinados


# === Main =====================================================================

def main():
    parser = argparse.ArgumentParser(description="Agente de enriquecimiento + Supabase upload")
    parser.add_argument("--limit", type=int, default=0, help="Limitar cantidad de proyectos")
    parser.add_argument("--ciudad", type=str, default="", help="Filtrar por ciudad")
    parser.add_argument("--estado", type=str, default="", help="Filtrar por estado")
    parser.add_argument("--portal", type=str, default="", help="Filtrar por portal fuente")
    parser.add_argument("--dry-run", action="store_true", help="Solo diagnostico")
    parser.add_argument("--force", action="store_true", help="Re-enriquecer todo")
    parser.add_argument("--only-missing", action="store_true", help="Solo con campos faltantes")
    parser.add_argument("--no-upload", action="store_true", help="Enriquecer sin subir a Supabase")
    parser.add_argument("--only-upload", action="store_true", help="Subir log existente sin re-enriquecer")
    parser.add_argument("--match-existing", action="store_true",
                        help="Actualizar desarrollos existentes en vez de crear nuevos")
    args = parser.parse_args()

    print("=" * 70)
    print("AGENTE DE ENRIQUECIMIENTO + SUPABASE UPLOAD")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    # Verificar Supabase
    upload_enabled = not args.no_upload and not args.dry_run
    sb = None

    if upload_enabled:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("\n[!] Variables de entorno no configuradas:")
            print("    set PROPYTE_SUPABASE_URL=https://yjbrynsykkcozeybykj.supabase.co")
            print("    set PROPYTE_SUPABASE_SERVICE_KEY=eyJ...")
            print("\n    Ejecutando solo enriquecimiento (sin upload).\n")
            upload_enabled = False
        else:
            sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY, SUPABASE_SCHEMA)
            ok, msg = sb.test_connection()
            if ok:
                print(f"\nSupabase: {msg}")
            else:
                print(f"\n[!] Error Supabase: {msg}")
                print("    Ejecutando solo enriquecimiento (sin upload).\n")
                upload_enabled = False
                sb = None

    # Cargar datos
    if not os.path.exists(LANZAMIENTOS_CSV):
        print(f"Error: No se encontro {LANZAMIENTOS_CSV}")
        sys.exit(1)

    with open(LANZAMIENTOS_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        proyectos = list(reader)

    print(f"\nProyectos en lanzamientos.csv: {len(proyectos)}")

    # Filtros
    if args.ciudad:
        proyectos = [p for p in proyectos if normalizar(args.ciudad) in normalizar(p.get("ciudad", ""))]
        print(f"Filtro ciudad '{args.ciudad}': {len(proyectos)}")
    if args.estado:
        proyectos = [p for p in proyectos if normalizar(args.estado) in normalizar(p.get("estado_republica", ""))]
        print(f"Filtro estado '{args.estado}': {len(proyectos)}")
    if args.portal:
        proyectos = [p for p in proyectos if normalizar(args.portal) in normalizar(p.get("portal_fuente", ""))]
        print(f"Filtro portal '{args.portal}': {len(proyectos)}")
    if args.only_missing:
        proyectos = [p for p in proyectos if len(calcular_faltantes(p)) > 0]
        print(f"Solo con campos faltantes: {len(proyectos)}")
    if args.limit > 0:
        proyectos = proyectos[:args.limit]
        print(f"Limitado a: {len(proyectos)}")

    # Diagnostico
    print(f"\n{'-'*70}")
    print("DIAGNOSTICO DE DATOS FALTANTES")
    print(f"{'-'*70}")

    total = len(proyectos)
    for campo in CAMPOS_CRITICOS:
        vacios = sum(1 for p in proyectos if campo_vacio(p.get(campo)))
        pct = vacios / total * 100 if total > 0 else 0
        bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
        print(f"  {campo:30s} {bar} {vacios:4d}/{total} ({pct:.0f}% vacio)")

    proyectos_con_faltantes = sum(1 for p in proyectos if len(calcular_faltantes(p)) > 0)
    print(f"\nProyectos con al menos 1 campo faltante: {proyectos_con_faltantes}/{total}")

    if args.dry_run:
        print("\n[DRY RUN] No se realizaran busquedas ni uploads.")
        return

    # Cargar existentes de Supabase para matching
    existentes = []
    if upload_enabled and sb and args.match_existing:
        print("\nCargando desarrollos existentes de Supabase para matching...")
        try:
            existentes = sb.get_existing_developments()
            print(f"  {len(existentes)} desarrollos existentes cargados")
        except Exception as e:
            print(f"  Error cargando existentes: {e}")

    # Solo upload mode
    if args.only_upload:
        log = cargar_log()
        if not log:
            print("No hay log de enriquecimiento. Ejecuta primero sin --only-upload.")
            return
        print(f"\nSubiendo {len(log)} proyectos del log a Supabase...")
        subidos, actualizados, errores = subir_a_supabase(sb, proyectos, log, existentes, args.match_existing)
        print(f"\nResultado: {subidos} nuevos, {actualizados} actualizados, {errores} errores")
        return

    # Enriquecer
    print(f"\n{'='*70}")
    print("EJECUTANDO ENRIQUECIMIENTO")
    print(f"{'='*70}")

    log = cargar_log() if not args.force else {}
    session = requests.Session()
    resultados = []
    total_campos_nuevos = 0

    for i, proyecto in enumerate(proyectos):
        print(f"\n[{i+1}/{len(proyectos)}]", end="")
        datos = enriquecer_proyecto(session, proyecto, log)

        proyecto_enriched = {**proyecto}
        for k, v in datos.items():
            if not campo_vacio(v) if not isinstance(v, dict) else v:
                if campo_vacio(proyecto_enriched.get(k)) or k in CAMPOS_ENRIQUECIMIENTO or k == "amenidades_bool":
                    proyecto_enriched[k] = v
                    if k != "amenidades_bool":
                        total_campos_nuevos += 1

        resultados.append(proyecto_enriched)

        if (i + 1) % 10 == 0:
            guardar_log(log)
            print(f"\n  [LOG] Guardado progreso ({i+1}/{len(proyectos)})")

        delay(2, 4)

    guardar_log(log)

    # CSV enriquecido
    if resultados:
        all_keys = set()
        for r in resultados:
            all_keys.update(k for k in r.keys() if k != "amenidades_bool")
        ordered_keys = [h for h in ENRICHED_HEADERS if h in all_keys]
        ordered_keys.extend([k for k in sorted(all_keys) if k not in ordered_keys])

        with open(ENRICHED_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=ordered_keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows([{k: v for k, v in r.items() if k != "amenidades_bool"} for r in resultados])
        print(f"\nCSV enriquecido: {ENRICHED_CSV} ({len(resultados)} filas)")

    # Subir a Supabase
    if upload_enabled and sb and resultados:
        print(f"\n{'='*70}")
        print("SUBIENDO A SUPABASE")
        print(f"{'='*70}")
        subidos, actualizados, errores = subir_a_supabase(sb, resultados, log, existentes, args.match_existing)
        print(f"\nResultado: {subidos} nuevos, {actualizados} actualizados, {errores} errores")

    # Resumen
    print(f"\n{'='*70}")
    print("RESUMEN")
    print(f"{'='*70}")
    print(f"Proyectos procesados: {len(resultados)}")
    print(f"Campos nuevos encontrados: {total_campos_nuevos}")

    confianza_counts = {"alta": 0, "media": 0, "baja": 0}
    for r in resultados:
        conf = r.get("confianza_datos", "baja")
        if conf in confianza_counts:
            confianza_counts[conf] += 1
    print(f"\nConfianza de datos:")
    for nivel, count in confianza_counts.items():
        print(f"  {nivel}: {count}")

    print(f"\nCampos recuperados:")
    for campo in CAMPOS_CRITICOS + CAMPOS_ENRIQUECIMIENTO[:5]:
        antes = sum(1 for p in proyectos if not campo_vacio(p.get(campo)))
        despues = sum(1 for r in resultados if not campo_vacio(r.get(campo)))
        delta = despues - antes
        if delta > 0:
            print(f"  {campo}: {antes} -> {despues} (+{delta})")


def subir_a_supabase(sb, resultados, log, existentes, match_mode):
    """Sube todos los resultados enriquecidos a Supabase."""
    subidos = 0
    actualizados = 0
    errores = 0
    developer_cache = {}

    for i, proyecto in enumerate(resultados):
        nombre = proyecto.get("nombre_proyecto", "")
        ciudad = proyecto.get("ciudad", "")

        if not nombre or campo_vacio(nombre):
            continue

        # Resolver desarrollador
        dev_name = proyecto.get("desarrolladora")
        dev_id = None
        if dev_name and not campo_vacio(dev_name):
            cache_key = normalizar(dev_name)
            if cache_key in developer_cache:
                dev_id = developer_cache[cache_key]
            else:
                try:
                    dev_id = sb.get_developer_id(dev_name)
                    developer_cache[cache_key] = dev_id
                except Exception:
                    pass

        # Convertir a schema Supabase
        row = proyecto_a_supabase(proyecto, dev_id)

        # Verificar si ya existe
        if match_mode and existentes:
            existing_id = match_desarrollo(proyecto, existentes)
            if existing_id:
                # Actualizar solo campos vacios del existente
                try:
                    status, err = sb.update_development(existing_id, row)
                    if status in (200, 204):
                        actualizados += 1
                        if (actualizados + subidos) % 25 == 0:
                            print(f"  Progreso: {subidos} nuevos, {actualizados} actualizados...")
                    else:
                        print(f"  [ERR] Update {nombre}: {status} {err}")
                        errores += 1
                except Exception as e:
                    print(f"  [ERR] Update {nombre}: {e}")
                    errores += 1
                continue

        # Insertar nuevo
        try:
            status, err = sb.insert_development(row)
            if status == 201:
                subidos += 1
                if (subidos + actualizados) % 25 == 0:
                    print(f"  Progreso: {subidos} nuevos, {actualizados} actualizados...")
            elif status == 409:
                # Conflicto (ya existe por slug) - intentar update
                existing_id = match_desarrollo(proyecto, existentes) if existentes else None
                if existing_id:
                    status2, err2 = sb.update_development(existing_id, row)
                    if status2 in (200, 204):
                        actualizados += 1
                    else:
                        errores += 1
                else:
                    # Slug duplicado, agregar sufijo
                    row["slug"] = row.get("slug", "") + f"-{i}"
                    status2, err2 = sb.insert_development(row)
                    if status2 == 201:
                        subidos += 1
                    else:
                        print(f"  [ERR] Insert retry {nombre}: {status2} {err2}")
                        errores += 1
            else:
                print(f"  [ERR] Insert {nombre}: {status} {err}")
                errores += 1
        except Exception as e:
            print(f"  [ERR] Insert {nombre}: {e}")
            errores += 1

    return subidos, actualizados, errores


if __name__ == "__main__":
    main()
