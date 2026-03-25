-- =============================================================================
-- 002_dedup_tables.sql — Tablas de soporte para el agente de deduplicacion
-- =============================================================================

-- Configuracion por tabla: que columnas comparar, umbrales, FKs relacionadas
CREATE TABLE IF NOT EXISTS public.dedup_config (
    id              SERIAL PRIMARY KEY,
    table_name      TEXT NOT NULL UNIQUE,
    schema_name     TEXT NOT NULL DEFAULT 'public',
    exact_columns   TEXT[] DEFAULT '{}',
    fuzzy_columns   TEXT[] NOT NULL DEFAULT '{}',
    similarity_threshold FLOAT DEFAULT 0.6,
    scope_columns   TEXT[] DEFAULT '{}',
    related_tables  JSONB DEFAULT '[]'::jsonb,
    merge_strategy  TEXT DEFAULT 'keep_most_complete'
                    CHECK (merge_strategy IN ('keep_oldest', 'keep_newest', 'keep_most_complete')),
    enabled         BOOLEAN DEFAULT true,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Historial completo de cada fusion ejecutada
CREATE TABLE IF NOT EXISTS public.dedup_log (
    id              SERIAL PRIMARY KEY,
    table_name      TEXT NOT NULL,
    winner_id       TEXT NOT NULL,
    loser_id        TEXT NOT NULL,
    similarity_score FLOAT,
    match_type      TEXT,
    fields_merged   JSONB DEFAULT '{}'::jsonb,
    related_updates JSONB DEFAULT '{}'::jsonb,
    action          TEXT DEFAULT 'merged'
                    CHECK (action IN ('merged', 'skipped', 'flagged', 'failed')),
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Cola asincrona de registros pendientes de procesar
CREATE TABLE IF NOT EXISTS public.dedup_queue (
    id              SERIAL PRIMARY KEY,
    table_name      TEXT NOT NULL,
    record_id       TEXT NOT NULL,
    trigger_event   TEXT,
    processed       BOOLEAN DEFAULT false,
    attempts        INT DEFAULT 0,
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    processed_at    TIMESTAMPTZ
);

-- Indices para rendimiento
CREATE INDEX IF NOT EXISTS idx_dedup_log_table_winner
    ON public.dedup_log(table_name, winner_id);
CREATE INDEX IF NOT EXISTS idx_dedup_log_created
    ON public.dedup_log(created_at);
CREATE INDEX IF NOT EXISTS idx_dedup_queue_pending
    ON public.dedup_queue(created_at)
    WHERE NOT processed;
CREATE INDEX IF NOT EXISTS idx_dedup_queue_table
    ON public.dedup_queue(table_name, record_id);
