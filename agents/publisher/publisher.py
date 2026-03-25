"""
PublisherAgent — evalúa calidad, publica en Supabase y dispara WP sync.

Flujo por desarrollo:
  score >= 80  → publish directo (0 tokens AI)
  score 60-79  → Haiku review con prompt caching (≈150 tokens output)
  score <  60  → reject, log razón

El UPDATE published=true es idempotente (WHERE published = false).
NUNCA setea published=false.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import httpx

from . import config as cfg
from .audit_log import log_decision
from .quality_scorer import score_development
from .supabase_client import execute_sql
from .wp_trigger import trigger_wp_sync

logger = logging.getLogger(__name__)

# ─── Prompt caching para revisión AI ──────────────────────────────────────────
_PUBLISHER_SYSTEM = """\
Eres un editor de contenido inmobiliario para Propyte, una plataforma de bienes raíces en México.
Tu tarea es decidir si un desarrollo inmobiliario está listo para publicarse en el sitio web.

Criterios de aprobación:
1. El nombre debe ser un nombre real de un desarrollo (no un título de scraper o placeholder)
2. La descripción debe ser coherente y profesional (no inventada, no de baja calidad)
3. El precio debe ser plausible para México (departamentos: desde $800k MXN, casas: desde $1.5M MXN)
4. La ciudad debe corresponder a México
5. No debe haber señales de spam, datos duplicados, o placeholders

Responde ÚNICAMENTE en JSON válido, sin texto adicional:
{"approve": true, "reason": "breve justificación"}
o
{"approve": false, "reason": "razón específica del rechazo"}"""

_CANDIDATES_SQL = """\
SELECT
    id, name, city, state, description_es,
    price_min_mxn, price_max_mxn,
    lat, lng, images, stage
FROM public.developments
WHERE deleted_at IS NULL
  AND published = false
  AND description_es IS NOT NULL
  AND price_min_mxn IS NOT NULL
  AND city IS NOT NULL
  AND images IS NOT NULL
  AND array_length(images, 1) >= 1
  AND id NOT IN (
      SELECT development_id
      FROM public.publish_audit_log
      WHERE decision = 'rejected'
        AND created_at > NOW() - INTERVAL '7 days'
  )
ORDER BY updated_at ASC
LIMIT {limit}"""


def _esc(val: Any) -> str:
    return str(val or "").replace("'", "''")


class PublisherAgent:

    async def run_batch(
        self,
        client: httpx.AsyncClient,
        batch_id: uuid.UUID | None = None,
        dry_run: bool = False,
    ) -> dict:
        """
        Proceso principal: fetch candidatos → score → publicar o rechazar.
        Retorna resumen del batch.
        """
        if batch_id is None:
            batch_id = uuid.uuid4()
        batch_str = str(batch_id)

        candidates = await self._fetch_candidates(client)
        logger.info(f"[publisher] {len(candidates)} candidatos encontrados")

        published_ids: list[str] = []
        rejected_ids: list[str] = []
        ai_used_ids: list[str] = []

        for dev in candidates:
            result = score_development(dev, cfg)
            dev_id = str(dev.get("id", ""))
            dev_name = dev.get("name", dev_id)

            if result.passed:
                # Score >= 80: publicar sin AI
                logger.info(
                    f"[publisher] ✓ PUBLISH {dev_name!r} score={result.score}"
                )
                if not dry_run:
                    ok = await self._set_published(client, dev_id)
                    await log_decision(
                        client, dev, result.score, "published", batch_str
                    )
                    if ok:
                        published_ids.append(dev_id)

            elif result.borderline:
                # Score 60-79: pedir revisión a Haiku
                logger.info(
                    f"[publisher] ? REVIEW {dev_name!r} score={result.score} "
                    f"reasons={result.reasons}"
                )
                approve, ai_notes = await self._ai_review(client, dev)
                if approve:
                    logger.info(f"[publisher] ✓ AI APPROVED {dev_name!r}: {ai_notes}")
                    if not dry_run:
                        ok = await self._set_published(client, dev_id)
                        await log_decision(
                            client, dev, result.score, "published", batch_str,
                            ai_used=True, ai_notes=ai_notes
                        )
                        if ok:
                            published_ids.append(dev_id)
                            ai_used_ids.append(dev_id)
                else:
                    logger.info(f"[publisher] ✗ AI REJECTED {dev_name!r}: {ai_notes}")
                    if not dry_run:
                        await log_decision(
                            client, dev, result.score, "rejected", batch_str,
                            rejection_reason=ai_notes, ai_used=True, ai_notes=ai_notes
                        )
                    rejected_ids.append(dev_id)

            else:
                # Score < 60: rechazar con razón de código
                rejection = ",".join(result.reasons) or "low_score"
                logger.info(
                    f"[publisher] ✗ REJECT {dev_name!r} score={result.score} "
                    f"reasons={rejection}"
                )
                if not dry_run:
                    await log_decision(
                        client, dev, result.score, "rejected", batch_str,
                        rejection_reason=rejection
                    )
                rejected_ids.append(dev_id)

        # Trigger WP sync si se publicó algo
        if published_ids and not dry_run:
            logger.info(f"[publisher] Disparando WP sync ({len(published_ids)} publicados)")
            await trigger_wp_sync(client)

        summary = {
            "batch_id": batch_str,
            "dry_run": dry_run,
            "candidates": len(candidates),
            "published": len(published_ids),
            "rejected": len(rejected_ids),
            "ai_reviewed": len(ai_used_ids),
            "published_ids": published_ids,
        }
        logger.info(f"[publisher] Batch completo: {summary}")
        return summary

    # ─── Privados ─────────────────────────────────────────────────────────────

    async def _fetch_candidates(self, client: httpx.AsyncClient) -> list[dict]:
        sql = _CANDIDATES_SQL.format(limit=cfg.PUBLISH_BATCH_SIZE)
        result = await execute_sql(client, sql)
        if not result or not isinstance(result, list):
            return []
        return result

    async def _set_published(self, client: httpx.AsyncClient, dev_id: str) -> bool:
        """Setea published=true. Idempotente. NUNCA setea false."""
        sql = f"""
        UPDATE public.developments
        SET published = true, updated_at = NOW()
        WHERE id = '{_esc(dev_id)}'
          AND published = false
        """
        result = await execute_sql(client, sql)
        return result is not None

    async def _ai_review(
        self, client: httpx.AsyncClient, dev: dict
    ) -> tuple[bool, str]:
        """
        Pide a Claude Haiku que revise un desarrollo borderline.
        Usa prompt caching en el system prompt (90% descuento en tokens de sistema).
        Retorna (approve, reason).
        """
        user_msg = (
            f"Nombre: {dev.get('name', 'N/A')}\n"
            f"Ciudad: {dev.get('city', 'N/A')}, {dev.get('state', '')}\n"
            f"Precio desde: ${dev.get('price_min_mxn', 0):,} MXN\n"
            f"Etapa: {dev.get('stage', 'N/A')}\n"
            f"Descripción: {(dev.get('description_es') or '')[:300]}\n"
            f"Imágenes: {len(dev.get('images') or [])} imagen(es)"
        )

        payload = {
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 150,
            "system": [
                {
                    "type": "text",
                    "text": _PUBLISHER_SYSTEM,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            "messages": [{"role": "user", "content": user_msg}],
        }

        try:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": cfg.ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "anthropic-beta": "prompt-caching-2024-07-31",
                    "content-type": "application/json",
                },
                json=payload,
                timeout=20,
            )
            if resp.status_code != 200:
                logger.warning(f"AI review error {resp.status_code}: {resp.text[:200]}")
                return True, "ai_unavailable_default_approve"

            content = resp.json().get("content", [{}])[0].get("text", "{}")
            # Limpiar markdown si viene envuelto en ```json
            content = content.strip().removeprefix("```json").removesuffix("```").strip()
            parsed = json.loads(content)
            approve = bool(parsed.get("approve", True))
            reason = str(parsed.get("reason", ""))
            return approve, reason

        except json.JSONDecodeError:
            logger.warning("AI review: JSON inválido, aprobando por defecto")
            return True, "ai_json_parse_error_default_approve"
        except Exception as e:
            logger.warning(f"AI review exception: {e}")
            return True, f"ai_exception_default_approve: {e}"
