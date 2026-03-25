-- =============================================================================
-- 003_dedup_functions.sql — Funciones PL/pgSQL del agente de deduplicacion
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. dedup_enqueue_record() — Funcion de trigger que encola registros
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.dedup_enqueue_record()
RETURNS TRIGGER AS $$
DECLARE
    v_config_exists BOOLEAN;
BEGIN
    -- Verificar si la tabla tiene config de dedup activa
    SELECT EXISTS(
        SELECT 1 FROM public.dedup_config
        WHERE table_name = TG_TABLE_NAME
          AND enabled = true
    ) INTO v_config_exists;

    IF v_config_exists THEN
        INSERT INTO public.dedup_queue (table_name, record_id, trigger_event)
        VALUES (TG_TABLE_NAME, NEW.id::text, TG_OP);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ---------------------------------------------------------------------------
-- Helper: obtener valor de una columna dado tabla + id + columna
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public._dedup_get_col_value(
    p_schema TEXT, p_table TEXT, p_id TEXT, p_col TEXT
)
RETURNS TEXT AS $$
DECLARE
    v_val TEXT;
BEGIN
    EXECUTE format(
        'SELECT %I::text FROM %I.%I WHERE id = %L',
        p_col, p_schema, p_table, p_id
    ) INTO v_val;
    RETURN v_val;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ---------------------------------------------------------------------------
-- 2. dedup_find_duplicates() — Busca duplicados con match exacto + fuzzy
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.dedup_find_duplicates(
    p_table_name TEXT,
    p_record_id TEXT
)
RETURNS TABLE(duplicate_id TEXT, similarity_score FLOAT, match_type TEXT) AS $$
DECLARE
    v_config       RECORD;
    v_col          TEXT;
    v_val          TEXT;
    v_scope_clause TEXT := '';
    v_query        TEXT;
    v_exists       BOOLEAN;
BEGIN
    -- Leer configuracion
    SELECT * INTO v_config
    FROM public.dedup_config
    WHERE table_name = p_table_name AND enabled = true;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Verificar que el registro existe
    EXECUTE format(
        'SELECT EXISTS(SELECT 1 FROM %I.%I WHERE id = %L AND deleted_at IS NULL)',
        v_config.schema_name, p_table_name, p_record_id
    ) INTO v_exists;

    IF NOT v_exists THEN
        RETURN;
    END IF;

    -- Construir clausula de scope (ej: city = record.city)
    IF v_config.scope_columns IS NOT NULL AND array_length(v_config.scope_columns, 1) > 0 THEN
        FOREACH v_col IN ARRAY v_config.scope_columns LOOP
            v_val := public._dedup_get_col_value(
                v_config.schema_name, p_table_name, p_record_id, v_col
            );
            IF v_val IS NOT NULL THEN
                v_scope_clause := v_scope_clause || format(
                    ' AND %I = %L', v_col, v_val
                );
            END IF;
        END LOOP;
    END IF;

    -- FASE 1: Match exacto por columnas exactas (ej: source_url)
    IF v_config.exact_columns IS NOT NULL AND array_length(v_config.exact_columns, 1) > 0 THEN
        FOREACH v_col IN ARRAY v_config.exact_columns LOOP
            v_val := public._dedup_get_col_value(
                v_config.schema_name, p_table_name, p_record_id, v_col
            );

            IF v_val IS NOT NULL AND v_val != '' THEN
                v_query := format(
                    'SELECT id::text AS duplicate_id, 1.0::float AS similarity_score, %L AS match_type '
                    'FROM %I.%I '
                    'WHERE %I = %L AND id::text != %L AND deleted_at IS NULL %s '
                    'LIMIT 5',
                    'exact_' || v_col,
                    v_config.schema_name, p_table_name,
                    v_col, v_val, p_record_id,
                    v_scope_clause
                );

                RETURN QUERY EXECUTE v_query;
            END IF;
        END LOOP;
    END IF;

    -- FASE 2: Match fuzzy por columnas fuzzy (ej: name)
    IF v_config.fuzzy_columns IS NOT NULL AND array_length(v_config.fuzzy_columns, 1) > 0 THEN
        FOREACH v_col IN ARRAY v_config.fuzzy_columns LOOP
            v_val := public._dedup_get_col_value(
                v_config.schema_name, p_table_name, p_record_id, v_col
            );

            IF v_val IS NOT NULL AND length(v_val) >= 3 THEN
                -- Combinar pg_trgm similarity (0-1) con levenshtein normalizado
                v_query := format(
                    'SELECT '
                    '  id::text AS duplicate_id, '
                    '  GREATEST( '
                    '    similarity(%I, %L), '
                    '    1.0 - (levenshtein(lower(left(%I, 255)), lower(left(%L, 255)))::float / '
                    '           GREATEST(length(%I), length(%L), 1)) '
                    '  ) AS similarity_score, '
                    '  %L AS match_type '
                    'FROM %I.%I '
                    'WHERE id::text != %L '
                    '  AND deleted_at IS NULL '
                    '  AND %I IS NOT NULL '
                    '  AND length(%I) >= 3 '
                    '  %s '
                    '  AND GREATEST( '
                    '    similarity(%I, %L), '
                    '    1.0 - (levenshtein(lower(left(%I, 255)), lower(left(%L, 255)))::float / '
                    '           GREATEST(length(%I), length(%L), 1)) '
                    '  ) >= %s '
                    'ORDER BY similarity_score DESC '
                    'LIMIT 5',
                    -- SELECT clause
                    v_col, v_val,
                    v_col, v_val,
                    v_col, v_val,
                    -- match_type
                    'fuzzy_' || v_col,
                    -- FROM
                    v_config.schema_name, p_table_name,
                    -- WHERE
                    p_record_id,
                    v_col,
                    v_col,
                    v_scope_clause,
                    -- threshold filter
                    v_col, v_val,
                    v_col, v_val,
                    v_col, v_val,
                    v_config.similarity_threshold::text
                );

                RETURN QUERY EXECUTE v_query;
            END IF;
        END LOOP;
    END IF;

    RETURN;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ---------------------------------------------------------------------------
-- 3. dedup_merge_records() — Fusiona dos registros (gap-fill + FK update)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.dedup_merge_records(
    p_table_name TEXT,
    p_winner_id  TEXT,
    p_loser_id   TEXT,
    p_match_type TEXT DEFAULT 'unknown',
    p_score      FLOAT DEFAULT 0.0
)
RETURNS BOOLEAN AS $$
DECLARE
    v_config         RECORD;
    v_col            RECORD;
    v_winner_val     TEXT;
    v_loser_val      TEXT;
    v_fields_merged  JSONB := '{}'::jsonb;
    v_related_updates JSONB := '{}'::jsonb;
    v_set_parts      TEXT[] := '{}';
    v_update_sql     TEXT;
    v_rel            JSONB;
    v_fk_table       TEXT;
    v_fk_column      TEXT;
    v_fk_count       INT;
    v_col_name       TEXT;
    v_is_array       BOOLEAN;
BEGIN
    -- Leer configuracion
    SELECT * INTO v_config
    FROM public.dedup_config
    WHERE table_name = p_table_name AND enabled = true;

    IF NOT FOUND THEN
        RETURN false;
    END IF;

    -- Verificar que ambos registros existen
    EXECUTE format(
        'SELECT EXISTS(SELECT 1 FROM %I.%I WHERE id = %L)',
        v_config.schema_name, p_table_name, p_winner_id
    ) INTO v_winner_val;
    IF v_winner_val != 'true' THEN RETURN false; END IF;

    EXECUTE format(
        'SELECT EXISTS(SELECT 1 FROM %I.%I WHERE id = %L)',
        v_config.schema_name, p_table_name, p_loser_id
    ) INTO v_loser_val;
    IF v_loser_val != 'true' THEN RETURN false; END IF;

    -- Gap-fill: copiar campos no nulos del loser al winner donde el winner tiene NULL
    FOR v_col IN
        SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS col_type
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = v_config.schema_name
          AND c.relname = p_table_name
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND a.attname NOT IN ('id', 'created_at', 'updated_at', 'deleted_at', 'slug')
    LOOP
        v_col_name := v_col.attname;
        v_is_array := v_col.col_type LIKE '%[]';

        -- Obtener valores del winner y loser directamente de la tabla
        BEGIN
            v_winner_val := public._dedup_get_col_value(
                v_config.schema_name, p_table_name, p_winner_id, v_col_name
            );
            v_loser_val := public._dedup_get_col_value(
                v_config.schema_name, p_table_name, p_loser_id, v_col_name
            );
        EXCEPTION WHEN OTHERS THEN
            CONTINUE;
        END;

        -- Si winner es NULL/vacio y loser tiene valor, copiar
        IF (v_winner_val IS NULL OR v_winner_val = '' OR v_winner_val = '{}')
           AND v_loser_val IS NOT NULL
           AND v_loser_val != ''
           AND v_loser_val != '{}' THEN
            v_set_parts := array_append(
                v_set_parts,
                format('%I = (SELECT %I FROM %I.%I WHERE id = %L)',
                    v_col_name, v_col_name,
                    v_config.schema_name, p_table_name, p_loser_id)
            );
            v_fields_merged := v_fields_merged || jsonb_build_object(
                v_col_name, jsonb_build_object('from', p_loser_id, 'value', left(v_loser_val, 100))
            );

        -- Si ambos tienen arrays, fusionar
        ELSIF v_is_array
              AND v_winner_val IS NOT NULL AND v_winner_val != '{}'
              AND v_loser_val IS NOT NULL AND v_loser_val != '{}' THEN
            v_set_parts := array_append(
                v_set_parts,
                format(
                    '%I = (SELECT ARRAY(SELECT DISTINCT unnest(array_cat('
                    '  (SELECT %I FROM %I.%I WHERE id = %L), '
                    '  (SELECT %I FROM %I.%I WHERE id = %L) '
                    '))))',
                    v_col_name,
                    v_col_name, v_config.schema_name, p_table_name, p_winner_id,
                    v_col_name, v_config.schema_name, p_table_name, p_loser_id
                )
            );
            v_fields_merged := v_fields_merged || jsonb_build_object(
                v_col_name, jsonb_build_object('action', 'array_merge', 'from', p_loser_id)
            );
        END IF;
    END LOOP;

    -- Aplicar gap-fill al winner
    IF array_length(v_set_parts, 1) > 0 THEN
        v_update_sql := format(
            'UPDATE %I.%I SET %s, updated_at = NOW() WHERE id = %L',
            v_config.schema_name, p_table_name,
            array_to_string(v_set_parts, ', '),
            p_winner_id
        );
        EXECUTE v_update_sql;
    END IF;

    -- Actualizar foreign keys en tablas relacionadas
    IF v_config.related_tables IS NOT NULL AND jsonb_array_length(v_config.related_tables) > 0 THEN
        FOR v_rel IN SELECT * FROM jsonb_array_elements(v_config.related_tables)
        LOOP
            v_fk_table := v_rel->>'table';
            v_fk_column := v_rel->>'fk_column';

            IF v_fk_table IS NOT NULL AND v_fk_column IS NOT NULL THEN
                EXECUTE format(
                    'UPDATE public.%I SET %I = %L WHERE %I = %L',
                    v_fk_table, v_fk_column, p_winner_id,
                    v_fk_column, p_loser_id
                );

                GET DIAGNOSTICS v_fk_count = ROW_COUNT;
                v_related_updates := v_related_updates || jsonb_build_object(
                    v_fk_table, v_fk_count
                );
            END IF;
        END LOOP;
    END IF;

    -- Soft-delete del loser
    EXECUTE format(
        'UPDATE %I.%I SET deleted_at = NOW(), updated_at = NOW() WHERE id = %L',
        v_config.schema_name, p_table_name, p_loser_id
    );

    -- Registrar en dedup_log
    INSERT INTO public.dedup_log (
        table_name, winner_id, loser_id, similarity_score,
        match_type, fields_merged, related_updates, action
    ) VALUES (
        p_table_name, p_winner_id, p_loser_id, p_score,
        p_match_type, v_fields_merged, v_related_updates, 'merged'
    );

    RETURN true;

EXCEPTION WHEN OTHERS THEN
    -- Registrar error sin abortar
    INSERT INTO public.dedup_log (
        table_name, winner_id, loser_id, similarity_score,
        match_type, action, error_message
    ) VALUES (
        p_table_name, p_winner_id, p_loser_id, p_score,
        p_match_type, 'failed', SQLERRM
    );
    RETURN false;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ---------------------------------------------------------------------------
-- 4. dedup_choose_winner() — Elige el registro maestro segun la estrategia
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.dedup_choose_winner(
    p_table_name TEXT,
    p_id_a TEXT,
    p_id_b TEXT
)
RETURNS TEXT AS $$
DECLARE
    v_config   RECORD;
    v_count_a  INT := 0;
    v_count_b  INT := 0;
    v_date_a   TIMESTAMPTZ;
    v_date_b   TIMESTAMPTZ;
    v_col      RECORD;
    v_val      TEXT;
BEGIN
    SELECT * INTO v_config
    FROM public.dedup_config
    WHERE table_name = p_table_name;

    IF NOT FOUND THEN
        RETURN p_id_a;
    END IF;

    IF v_config.merge_strategy = 'keep_oldest' THEN
        EXECUTE format(
            'SELECT created_at FROM %I.%I WHERE id = %L',
            v_config.schema_name, p_table_name, p_id_a
        ) INTO v_date_a;
        EXECUTE format(
            'SELECT created_at FROM %I.%I WHERE id = %L',
            v_config.schema_name, p_table_name, p_id_b
        ) INTO v_date_b;
        RETURN CASE WHEN v_date_a <= v_date_b THEN p_id_a ELSE p_id_b END;

    ELSIF v_config.merge_strategy = 'keep_newest' THEN
        EXECUTE format(
            'SELECT created_at FROM %I.%I WHERE id = %L',
            v_config.schema_name, p_table_name, p_id_a
        ) INTO v_date_a;
        EXECUTE format(
            'SELECT created_at FROM %I.%I WHERE id = %L',
            v_config.schema_name, p_table_name, p_id_b
        ) INTO v_date_b;
        RETURN CASE WHEN v_date_a >= v_date_b THEN p_id_a ELSE p_id_b END;

    ELSE  -- keep_most_complete: contar campos no nulos
        FOR v_col IN
            SELECT a.attname
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = v_config.schema_name
              AND c.relname = p_table_name
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND a.attname NOT IN ('id', 'created_at', 'updated_at', 'deleted_at')
        LOOP
            v_val := public._dedup_get_col_value(
                v_config.schema_name, p_table_name, p_id_a, v_col.attname
            );
            IF v_val IS NOT NULL AND v_val != '' AND v_val != '{}' THEN
                v_count_a := v_count_a + 1;
            END IF;

            v_val := public._dedup_get_col_value(
                v_config.schema_name, p_table_name, p_id_b, v_col.attname
            );
            IF v_val IS NOT NULL AND v_val != '' AND v_val != '{}' THEN
                v_count_b := v_count_b + 1;
            END IF;
        END LOOP;

        -- Si empatan, el mas antiguo gana
        IF v_count_a >= v_count_b THEN
            RETURN p_id_a;
        ELSE
            RETURN p_id_b;
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ---------------------------------------------------------------------------
-- 5. dedup_process_record() — Orquestador per-record
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.dedup_process_record(
    p_table_name TEXT,
    p_record_id TEXT
)
RETURNS INT AS $$
DECLARE
    v_dup        RECORD;
    v_winner_id  TEXT;
    v_loser_id   TEXT;
    v_merges     INT := 0;
    v_exists     BOOLEAN;
    v_schema     TEXT;
BEGIN
    -- Obtener schema de la config
    SELECT schema_name INTO v_schema
    FROM public.dedup_config
    WHERE table_name = p_table_name AND enabled = true;

    IF v_schema IS NULL THEN
        v_schema := 'public';
    END IF;

    -- Verificar que el registro aun existe y no fue soft-deleted
    EXECUTE format(
        'SELECT EXISTS(SELECT 1 FROM %I.%I WHERE id = %L AND deleted_at IS NULL)',
        v_schema, p_table_name, p_record_id
    ) INTO v_exists;

    IF NOT v_exists THEN
        RETURN 0;
    END IF;

    -- Buscar duplicados
    FOR v_dup IN
        SELECT * FROM public.dedup_find_duplicates(p_table_name, p_record_id)
    LOOP
        -- Verificar que el duplicado no fue ya procesado
        EXECUTE format(
            'SELECT EXISTS(SELECT 1 FROM %I.%I WHERE id = %L AND deleted_at IS NULL)',
            v_schema, p_table_name, v_dup.duplicate_id
        ) INTO v_exists;

        IF NOT v_exists THEN
            CONTINUE;
        END IF;

        -- Verificar que no se fusione consigo mismo
        IF v_dup.duplicate_id = p_record_id THEN
            CONTINUE;
        END IF;

        -- Elegir winner
        v_winner_id := public.dedup_choose_winner(p_table_name, p_record_id, v_dup.duplicate_id);
        v_loser_id := CASE WHEN v_winner_id = p_record_id
                          THEN v_dup.duplicate_id
                          ELSE p_record_id END;

        -- Fusionar
        IF public.dedup_merge_records(
            p_table_name, v_winner_id, v_loser_id,
            v_dup.match_type, v_dup.similarity_score
        ) THEN
            v_merges := v_merges + 1;
        END IF;

        -- Si el record_id actual fue el loser, no seguir buscando duplicados
        IF v_loser_id = p_record_id THEN
            EXIT;
        END IF;
    END LOOP;

    RETURN v_merges;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ---------------------------------------------------------------------------
-- 6. dedup_process_queue() — Procesa la cola de deduplicacion
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.dedup_process_queue(
    p_limit INT DEFAULT 100
)
RETURNS INT AS $$
DECLARE
    v_item    RECORD;
    v_total   INT := 0;
    v_merges  INT;
BEGIN
    FOR v_item IN
        SELECT id, table_name, record_id
        FROM public.dedup_queue
        WHERE NOT processed
        ORDER BY created_at
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    LOOP
        BEGIN
            v_merges := public.dedup_process_record(v_item.table_name, v_item.record_id);

            UPDATE public.dedup_queue
            SET processed = true,
                processed_at = NOW(),
                attempts = attempts + 1
            WHERE id = v_item.id;

            v_total := v_total + v_merges;

        EXCEPTION WHEN OTHERS THEN
            UPDATE public.dedup_queue
            SET attempts = attempts + 1,
                error_message = SQLERRM
            WHERE id = v_item.id;
        END;
    END LOOP;

    RETURN v_total;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
