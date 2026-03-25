"""
Dispara el WP-cron de WordPress para ejecutar el sync Supabase → WP
inmediatamente después de publicar desarrollos en Supabase.

No requiere autenticación — wp-cron.php es público por diseño en WordPress.
Si falla, no es crítico: el cron de WP corre cada 15min de todas formas.
"""

import logging
import httpx
from .config import WP_SITE_URL

logger = logging.getLogger(__name__)


async def trigger_wp_sync(client: httpx.AsyncClient) -> bool:
    """
    Hace GET a wp-cron.php para que WordPress ejecute todos los eventos
    pendientes, incluido propyte_supabase_sync.

    Retorna True si el ping fue exitoso (HTTP < 400).
    """
    url = f"{WP_SITE_URL}/wp-cron.php"
    try:
        resp = await client.get(
            url,
            params={"doing_wp_cron": "1"},
            timeout=15,
            follow_redirects=True,
        )
        if resp.status_code < 400:
            logger.info(f"[publisher] WP-cron ping OK → {resp.status_code}")
            return True
        else:
            logger.warning(f"[publisher] WP-cron ping HTTP {resp.status_code}")
            return False
    except httpx.TimeoutException:
        logger.warning("[publisher] WP-cron ping timeout (no crítico)")
        return False
    except Exception as e:
        logger.warning(f"[publisher] WP-cron ping falló (no crítico): {e}")
        return False
