"""
Tabla publish_audit_log + helpers para registrar decisiones de publicación.
"""

import logging

logger = logging.getLogger(__name__)

DDL_PUBLISH_AUDIT = """
CREATE TABLE IF NOT EXISTS public.publish_audit_log (
    id               UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    development_id   UUID NOT NULL,
    development_name TEXT,
    city             TEXT,
    quality_score    INTEGER,
    decision         TEXT NOT NULL CHECK (decision IN ('published', 'rejected', 'ai_review')),
    rejection_reason TEXT,
    ai_used          BOOLEAN DEFAULT FALSE,
    ai_notes         TEXT,
    batch_id         UUID NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pal_dev ON public.publish_audit_log(development_id);
CREATE INDEX IF NOT EXISTS idx_pal_decision ON public.publish_audit_log(decision, created_at DESC);
"""


def _esc(val: str) -> str:
    """Escapa comillas simples para SQL."""
    return str(val or "").replace("'", "''")


async def ensure_audit_table(client) -> bool:
    from .supabase_client import execute_sql
    result = await execute_sql(client, DDL_PUBLISH_AUDIT)
    if result is not None:
        logger.info("publish_audit_log table ensured")
        return True
    logger.error("Failed to create publish_audit_log table")
    return False


async def log_decision(
    client,
    dev: dict,
    score: int,
    decision: str,
    batch_id: str,
    rejection_reason: str = None,
    ai_used: bool = False,
    ai_notes: str = None,
) -> bool:
    from .supabase_client import execute_sql

    sql = f"""
    INSERT INTO public.publish_audit_log
        (development_id, development_name, city, quality_score,
         decision, rejection_reason, ai_used, ai_notes, batch_id)
    VALUES (
        '{_esc(dev.get("id", ""))}',
        '{_esc(dev.get("name", ""))}',
        '{_esc(dev.get("city", ""))}',
        {score},
        '{decision}',
        {f"'{_esc(rejection_reason)}'" if rejection_reason else 'NULL'},
        {str(ai_used).lower()},
        {f"'{_esc(str(ai_notes)[:500])}'" if ai_notes else 'NULL'},
        '{_esc(batch_id)}'
    )
    """
    result = await execute_sql(client, sql)
    return result is not None
