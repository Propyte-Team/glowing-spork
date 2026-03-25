-- =============================================================================
-- 001_extensions.sql — Habilitar extensiones requeridas para deduplicacion
-- pg_trgm: similitud de texto fuzzy (similarity(), %)
-- fuzzystrmatch: distancia Levenshtein y Soundex
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
