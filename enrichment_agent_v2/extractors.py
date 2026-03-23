"""
Funciones de extracción de datos desde HTML/texto.
Portado de agente_enriquecimiento.py con mejoras para uso async.
"""

import re
import unicodedata
from urllib.parse import urljoin

from .config import AMENIDAD_MAP


# === Utilidades ===============================================================

def normalizar(texto: str) -> str:
    """Normaliza texto: lowercase, sin acentos, espacios simples."""
    if not texto:
        return ""
    texto = unicodedata.normalize("NFKD", texto.lower().strip())
    texto = re.sub(r"[\u0300-\u036f]", "", texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto


def campo_vacio(valor) -> bool:
    """Retorna True si el valor es vacío, None, nan, etc."""
    if valor is None:
        return True
    s = str(valor).strip()
    return s == "" or s.lower() in ("nan", "n/a", "none", "sin datos", "null")


def generar_slug(texto: str, max_len: int = 200) -> str:
    """Genera slug URL-safe desde texto."""
    slug = normalizar(texto).replace(" ", "-")
    slug = re.sub(r"[^a-z0-9\-]", "", slug)  # Remove non-alphanumeric
    slug = re.sub(r"-{2,}", "-", slug)  # Collapse multiple dashes
    return slug.strip("-")[:max_len]


# === Extractores de Precio ====================================================

def extraer_precio(texto: str) -> tuple[int | None, int | None]:
    """Extrae precio mínimo y máximo de texto. Retorna (min, max) en MXN."""
    texto = texto.lower().replace(",", "").replace(" ", "")

    # Patrón 1: $NNNNNN (6-9 dígitos)
    precios_mxn = re.findall(r"\$(\d{6,9})", texto)
    if precios_mxn:
        nums = sorted([int(p) for p in precios_mxn])
        return nums[0], nums[-1] if len(nums) > 1 else nums[0]

    # Patrón 2: N.N mdp/millones
    mdp = re.findall(r"(\d+\.?\d*)\s*(?:mdp|millones)", texto)
    if mdp:
        nums = sorted([float(m) * 1_000_000 for m in mdp])
        return int(nums[0]), int(nums[-1]) if len(nums) > 1 else int(nums[0])

    # Patrón 3: desde $N
    desde = re.search(r"desde\s*\$?\s*(\d[\d,.]*)", texto)
    if desde:
        val = int(desde.group(1).replace(",", "").replace(".", ""))
        if val < 1000:
            val = val * 1_000_000
        return val, None

    return None, None


def extraer_precio_de_rango(rango_str: str) -> tuple[int | None, int | None]:
    """Parsea campo rango_precios del CSV (ej: 'Desde $2,727,000 MXN')."""
    if campo_vacio(rango_str):
        return None, None
    texto = str(rango_str).replace(",", "").replace(" ", "").lower()
    nums = re.findall(r"\$?(\d{6,10})", texto)
    if nums:
        sorted_nums = sorted([int(n) for n in nums])
        return sorted_nums[0], sorted_nums[-1] if len(sorted_nums) > 1 else sorted_nums[0]
    return None, None


# === Extractores de Texto =====================================================

def extraer_superficie(texto: str) -> tuple[float | None, float | None]:
    """Extrae superficie mínima y máxima en m2."""
    texto = texto.lower()
    rango = re.findall(r"(\d+\.?\d*)\s*(?:m2|m\xb2|metros?\s*cuadrados?)", texto)
    if rango:
        nums = sorted([float(s) for s in rango])
        nums = [n for n in nums if 10 <= n <= 2000]
        if nums:
            return nums[0], nums[-1] if len(nums) > 1 else nums[0]
    return None, None


def extraer_num_unidades(texto: str) -> int | None:
    """Extrae número de unidades del texto."""
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


def extraer_desarrolladora(texto: str) -> str | None:
    """Extrae nombre de desarrolladora del texto."""
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
                nombre_lower = nombre.lower()
                if any(bl in nombre_lower for bl in blacklist):
                    continue
                if re.search(r"[A-Z\u00c1-\u00da]", nombre):
                    return nombre
    return None


def extraer_amenidades(texto: str) -> dict[str, bool]:
    """Extrae amenidades como dict de columnas boolean de Supabase."""
    texto = normalizar(texto)
    amenidades_encontradas = {}
    for keyword, col in AMENIDAD_MAP.items():
        if keyword in texto:
            amenidades_encontradas[col] = True
    return amenidades_encontradas


def extraer_amenidades_lista(texto: str) -> list[str]:
    """Extrae lista de nombres de amenidades encontradas."""
    texto = texto.lower()
    amenidades_conocidas = [
        "alberca", "piscina", "gimnasio", "gym", "roof garden", "rooftop",
        "coworking", "lobby", "seguridad 24", "spa", "restaurante",
        "concierge", "elevador", "bodega", "pet friendly", "cancha",
        "area ninos", "yoga", "fire pit", "asador", "salon de eventos",
    ]
    return sorted(set(a.capitalize() for a in amenidades_conocidas if a in texto))


def extraer_fecha_entrega(texto: str) -> str | None:
    """Extrae fecha de entrega estimada del texto."""
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


def extraer_contacto(texto: str) -> tuple[str | None, str | None]:
    """Extrae teléfono y email del texto. Retorna (telefono, email)."""
    telefono = None
    email = None
    tel_match = re.search(
        r"(?:\+?52\s*)?(?:\(?\d{2,3}\)?\s*)?(\d{4}[\s-]?\d{4})", texto
    )
    if tel_match:
        telefono = tel_match.group(0).strip()
    email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", texto)
    if email_match:
        email = email_match.group(0).strip()
    return telefono, email


def extraer_imagen(html_soup, base_url: str = "") -> str | None:
    """Extrae URL de imagen principal desde BeautifulSoup object."""
    og = html_soup.find("meta", property="og:image")
    if og and og.get("content"):
        return og["content"]
    for img in html_soup.find_all("img"):
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


def extraer_coordenadas(texto: str) -> tuple[float | None, float | None]:
    """Extrae lat/lng de texto (busca en scripts, meta tags, etc.)."""
    coord_match = re.search(
        r"(-?\d{1,3}\.\d{4,8})\s*,\s*(-?\d{1,3}\.\d{4,8})", texto
    )
    if coord_match:
        lat = float(coord_match.group(1))
        lng = float(coord_match.group(2))
        # Validar bounding box de México
        if 14 <= lat <= 33 and -118 <= lng <= -86:
            return lat, lng
    return None, None


def extraer_json_ld(html_soup) -> dict:
    """Extrae datos de JSON-LD schema.org (LaHaus, TheRedSearch, etc.)."""
    resultados = {}
    for script in html_soup.find_all("script", type="application/ld+json"):
        try:
            data = __import__("json").loads(script.string)
            if isinstance(data, list):
                data = data[0]
            tipo = data.get("@type", "")
            if tipo not in ("Product", "RealEstateListing", "Residence", "Place", "Apartment"):
                continue

            # Precios
            offers = data.get("offers", {})
            if isinstance(offers, list):
                offers = offers[0]
            if offers.get("lowPrice"):
                resultados["price_min_mxn"] = int(float(offers["lowPrice"]))
            if offers.get("highPrice"):
                resultados["price_max_mxn"] = int(float(offers["highPrice"]))
            elif offers.get("price"):
                resultados["price_min_mxn"] = int(float(offers["price"]))

            # Coordenadas
            geo = data.get("geo", {})
            if geo.get("latitude") and geo.get("longitude"):
                lat = float(geo["latitude"])
                lng = float(geo["longitude"])
                if 14 <= lat <= 33 and -118 <= lng <= -86:
                    resultados["lat"] = lat
                    resultados["lng"] = lng

            # Descripción
            if data.get("description"):
                resultados["description_es"] = str(data["description"])[:2000]

            # Imagen
            img = data.get("image")
            if isinstance(img, list):
                img = img[0] if img else None
            if img:
                resultados["images"] = [str(img)]

        except (ValueError, TypeError, KeyError, AttributeError):
            continue
    return resultados
