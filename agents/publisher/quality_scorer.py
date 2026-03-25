"""
Scorer de calidad para desarrollos — pure Python, cero tokens de AI.

Sistema de puntuación:
  - Hard gates: si fallan → score=0, rechazo inmediato
  - Soft criteria: suman puntos hasta 100

Umbrales (en config.py):
  >= 80 → publish automático
  60-79 → borderline, pasa a revisión AI (Haiku)
  <  60 → rechazado
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import *  # noqa: F401, F403


@dataclass
class QualityResult:
    score: int
    passed: bool          # True si score >= MIN_SCORE_AUTO_PUBLISH
    borderline: bool      # True si score en rango [MIN_SCORE_AI_REVIEW, MIN_SCORE_AUTO_PUBLISH)
    reasons: list[str] = field(default_factory=list)


def score_development(dev: dict, cfg) -> QualityResult:
    """
    Evalúa calidad de un desarrollo para publicación.

    Args:
        dev: dict con campos del desarrollo (de Supabase developments view)
        cfg: módulo config con MIN_SCORE_AUTO_PUBLISH, MIN_SCORE_AI_REVIEW,
             MIN_DESCRIPTION_LEN, MIN_PRICE_MXN

    Returns:
        QualityResult con score 0-100 y razones de fallo/baja puntuación
    """
    reasons: list[str] = []

    # ─── HARD GATES ───────────────────────────────────────────────────────────
    # Si alguno falla, score=0 sin evaluar el resto

    if not dev.get("city"):
        return QualityResult(0, False, False, ["missing_city"])

    if not dev.get("price_min_mxn"):
        return QualityResult(0, False, False, ["missing_price"])

    desc = dev.get("description_es") or ""
    if not desc:
        return QualityResult(0, False, False, ["missing_description"])

    images = dev.get("images") or []
    real_images = [
        img for img in images
        if img and "unsplash.com" not in img and img.startswith("http")
    ]
    if not real_images:
        return QualityResult(0, False, False, ["missing_real_image"])

    # ─── SCORING ──────────────────────────────────────────────────────────────
    score = 0

    # Descripción (30 pts)
    desc_len = len(desc)
    if desc_len >= cfg.MIN_DESCRIPTION_LEN:
        score += 30
    elif desc_len >= 40:
        score += 15
        reasons.append(f"short_description_{desc_len}chars")

    # Precio (25 pts)
    price = dev.get("price_min_mxn") or 0
    if price >= cfg.MIN_PRICE_MXN:
        score += 25
    elif price > 0:
        score += 10
        reasons.append(f"low_price_{price}")
    # price=0 ya fue capturado en hard gate arriba

    # Ciudad (15 pts) — ya confirmada en hard gate
    score += 15

    # Coordenadas (15 pts)
    if dev.get("lat") and dev.get("lng"):
        score += 15
    else:
        reasons.append("missing_coords")

    # Imágenes (15 pts)
    n_images = len(real_images)
    if n_images >= 3:
        score += 15
    elif n_images >= 1:
        score += 8
        reasons.append(f"few_images_{n_images}")

    passed = score >= cfg.MIN_SCORE_AUTO_PUBLISH
    borderline = (not passed) and score >= cfg.MIN_SCORE_AI_REVIEW

    return QualityResult(score, passed, borderline, reasons)
