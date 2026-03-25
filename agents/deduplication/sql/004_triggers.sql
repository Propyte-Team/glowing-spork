-- =============================================================================
-- 004_triggers.sql — Triggers de deduplicacion con deteccion automatica VIEW/TABLE
-- =============================================================================

-- Trigger para developments (detecta si es VIEW o TABLE)
DO $$
DECLARE
    v_relkind TEXT;
BEGIN
    -- Detectar si public.developments es tabla ('r') o vista ('v')
    SELECT c.relkind INTO v_relkind
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = 'public' AND c.relname = 'developments';

    IF v_relkind = 'r' THEN
        -- Es una tabla real: trigger directo
        EXECUTE 'DROP TRIGGER IF EXISTS trg_dedup_developments ON public.developments';
        EXECUTE '
            CREATE TRIGGER trg_dedup_developments
            AFTER INSERT OR UPDATE ON public.developments
            FOR EACH ROW
            EXECUTE FUNCTION public.dedup_enqueue_record()
        ';
        RAISE NOTICE 'Trigger creado en public.developments (TABLE)';

    ELSIF v_relkind = 'v' THEN
        -- Es una vista: trigger en la tabla subyacente de real_estate_hub
        -- Verificar que la tabla subyacente existe
        IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'real_estate_hub' AND c.relname = 'Propyte_desarrollos'
        ) THEN
            EXECUTE 'DROP TRIGGER IF EXISTS trg_dedup_desarrollos ON real_estate_hub."Propyte_desarrollos"';
            EXECUTE '
                CREATE TRIGGER trg_dedup_desarrollos
                AFTER INSERT OR UPDATE ON real_estate_hub."Propyte_desarrollos"
                FOR EACH ROW
                EXECUTE FUNCTION public.dedup_enqueue_record()
            ';
            RAISE NOTICE 'developments es VIEW — trigger creado en real_estate_hub."Propyte_desarrollos"';
        ELSE
            RAISE WARNING 'developments es VIEW pero no se encontro real_estate_hub."Propyte_desarrollos"';
        END IF;
    ELSE
        RAISE WARNING 'public.developments tiene relkind=%s (esperado r o v)', v_relkind;
    END IF;
END $$;

-- Trigger para developers
DO $$
DECLARE
    v_relkind TEXT;
BEGIN
    SELECT c.relkind INTO v_relkind
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = 'public' AND c.relname = 'developers';

    IF v_relkind = 'r' THEN
        EXECUTE 'DROP TRIGGER IF EXISTS trg_dedup_developers ON public.developers';
        EXECUTE '
            CREATE TRIGGER trg_dedup_developers
            AFTER INSERT OR UPDATE ON public.developers
            FOR EACH ROW
            EXECUTE FUNCTION public.dedup_enqueue_record()
        ';
        RAISE NOTICE 'Trigger creado en public.developers (TABLE)';

    ELSIF v_relkind = 'v' THEN
        IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'real_estate_hub' AND c.relname = 'Propyte_desarrolladores'
        ) THEN
            EXECUTE 'DROP TRIGGER IF EXISTS trg_dedup_desarrolladores ON real_estate_hub."Propyte_desarrolladores"';
            EXECUTE '
                CREATE TRIGGER trg_dedup_desarrolladores
                AFTER INSERT OR UPDATE ON real_estate_hub."Propyte_desarrolladores"
                FOR EACH ROW
                EXECUTE FUNCTION public.dedup_enqueue_record()
            ';
            RAISE NOTICE 'developers es VIEW — trigger creado en real_estate_hub."Propyte_desarrolladores"';
        ELSE
            RAISE WARNING 'developers es VIEW pero no se encontro real_estate_hub."Propyte_desarrolladores"';
        END IF;
    END IF;
END $$;
