-- =============================================================================
-- 005_seed_config.sql — Configuracion inicial para tablas reales del proyecto
-- =============================================================================

-- developments: fuzzy por name, exact por source_url, scope por city
-- related_tables: units y enrichment_log apuntan a development_id
INSERT INTO public.dedup_config (
    table_name, schema_name,
    fuzzy_columns, exact_columns,
    similarity_threshold, scope_columns,
    related_tables, merge_strategy, enabled
) VALUES (
    'developments', 'public',
    ARRAY['name'], ARRAY['source_url'],
    0.85, ARRAY['city'],
    '[
        {"table": "units", "fk_column": "development_id"},
        {"table": "enrichment_log", "fk_column": "development_id"}
    ]'::jsonb,
    'keep_most_complete', true
)
ON CONFLICT (table_name) DO UPDATE SET
    fuzzy_columns = EXCLUDED.fuzzy_columns,
    exact_columns = EXCLUDED.exact_columns,
    similarity_threshold = EXCLUDED.similarity_threshold,
    scope_columns = EXCLUDED.scope_columns,
    related_tables = EXCLUDED.related_tables,
    merge_strategy = EXCLUDED.merge_strategy,
    updated_at = NOW();

-- developers: fuzzy por name, exact por website
-- related_tables: developments apunta a developer_id
INSERT INTO public.dedup_config (
    table_name, schema_name,
    fuzzy_columns, exact_columns,
    similarity_threshold, scope_columns,
    related_tables, merge_strategy, enabled
) VALUES (
    'developers', 'public',
    ARRAY['name'], ARRAY['website'],
    0.70, ARRAY[]::text[],
    '[
        {"table": "developments", "fk_column": "developer_id"}
    ]'::jsonb,
    'keep_oldest', true
)
ON CONFLICT (table_name) DO UPDATE SET
    fuzzy_columns = EXCLUDED.fuzzy_columns,
    exact_columns = EXCLUDED.exact_columns,
    similarity_threshold = EXCLUDED.similarity_threshold,
    scope_columns = EXCLUDED.scope_columns,
    related_tables = EXCLUDED.related_tables,
    merge_strategy = EXCLUDED.merge_strategy,
    updated_at = NOW();
