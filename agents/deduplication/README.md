# Agente de Deduplicacion — PostgreSQL Nativo

Agente que detecta y fusiona registros duplicados automaticamente dentro de Supabase, usando triggers de PostgreSQL + fuzzy matching con `pg_trgm` y `fuzzystrmatch`.

## Arquitectura

```
INSERT/UPDATE en tabla configurada
        │
        ▼
  [TRIGGER] dedup_enqueue_record()
        │
        ▼
  dedup_queue (cola asincrona)
        │
        ▼  (pg_cron cada 5 min o run.py --scan)
  dedup_process_queue()
        │
        ▼
  dedup_find_duplicates()  →  pg_trgm similarity + levenshtein
        │
        ▼
  dedup_choose_winner()    →  keep_most_complete / keep_oldest / keep_newest
        │
        ▼
  dedup_merge_records()    →  gap-fill + FK update + soft-delete loser
        │
        ▼
  dedup_log (auditoria)
```

**El trigger nunca bloquea INSERT/UPDATE.** Solo encola el record_id. El procesamiento es asincrono.

## Prerequisitos

- Supabase con Management API access
- `.env` en la raiz de glowing-spork con:
  ```
  PROPYTE_SUPABASE_URL=https://yjbrynsykkycozeybykj.supabase.co
  PROPYTE_SUPABASE_SERVICE_KEY=eyJ...
  SUPABASE_MGMT_TOKEN=sbp_...
  SUPABASE_PROJECT_REF=yjbrynsykkycozeybykj
  ```
- Python 3.10+ con `httpx`

## Instalacion (una sola vez)

```bash
cd C:/Users/Luis/glowing-spork
python -m agents.deduplication.setup
```

El script:
1. Habilita extensiones `pg_trgm` y `fuzzystrmatch`
2. Detecta si `developments` es TABLE o VIEW (adapta triggers automaticamente)
3. Crea tablas de soporte: `dedup_config`, `dedup_log`, `dedup_queue`
4. Crea 6 funciones PL/pgSQL
5. Crea triggers AFTER INSERT/UPDATE
6. Inserta configuracion inicial para `developments` y `developers`

## Uso

### Primera pasada (escanear duplicados existentes)

```bash
# Dry-run: solo reportar duplicados sin fusionar
python -m agents.deduplication.run --full-scan --dry-run

# Ejecutar fusiones
python -m agents.deduplication.run --full-scan

# Solo una tabla
python -m agents.deduplication.run --full-scan --table developments --dry-run
```

### Operacion continua

```bash
# Procesar cola pendiente (registros nuevos desde triggers)
python -m agents.deduplication.run --scan

# Ver estado de la cola
python -m agents.deduplication.run --status

# Ver historial de fusiones
python -m agents.deduplication.run --report
```

### Con pg_cron (automatico en Supabase)

Si pg_cron esta disponible:
```sql
SELECT cron.schedule('dedup-process', '*/5 * * * *', 'SELECT public.dedup_process_queue(100)');
```

## Configuracion

La tabla `dedup_config` controla el comportamiento por tabla:

| Campo | Descripcion |
|-------|-------------|
| `table_name` | Tabla a deduplicar (ej: `developments`) |
| `exact_columns` | Match exacto (ej: `{source_url}`) |
| `fuzzy_columns` | Match fuzzy con pg_trgm (ej: `{name}`) |
| `similarity_threshold` | Umbral 0.0-1.0 (default: 0.6) |
| `scope_columns` | Columnas que deben coincidir exactamente para acotar busqueda (ej: `{city}`) |
| `related_tables` | FKs a actualizar en merge: `[{"table":"units","fk_column":"development_id"}]` |
| `merge_strategy` | `keep_most_complete` / `keep_oldest` / `keep_newest` |
| `enabled` | Activar/desactivar sin borrar |

### Agregar una tabla nueva

```sql
INSERT INTO dedup_config (table_name, fuzzy_columns, similarity_threshold)
VALUES ('mi_tabla', '{nombre}', 0.7);
```

Luego crear el trigger manualmente:
```sql
CREATE TRIGGER trg_dedup_mi_tabla
  AFTER INSERT OR UPDATE ON public.mi_tabla
  FOR EACH ROW EXECUTE FUNCTION public.dedup_enqueue_record();
```

## Estrategia de merge

1. **Gap-fill:** Si el winner tiene un campo NULL y el loser lo tiene, se copia al winner
2. **Array merge:** Campos `text[]` (images, amenities) se fusionan con deduplicacion
3. **FK update:** Todas las tablas en `related_tables` se actualizan para apuntar al winner
4. **Soft-delete:** El loser recibe `deleted_at = NOW()` (nunca se borra fisicamente)
5. **Auditoria:** Cada fusion queda en `dedup_log` con detalle de campos y FKs

## Archivos

```
agents/deduplication/
├── config.py            # Variables de entorno
├── supabase_client.py   # execute_sql() via Management API
├── setup.py             # Instalador one-shot
├── run.py               # CLI operacional
├── __main__.py          # Entry point
├── sql/
│   ├── 001_extensions.sql
│   ├── 002_dedup_tables.sql
│   ├── 003_dedup_functions.sql
│   ├── 004_triggers.sql
│   └── 005_seed_config.sql
└── README.md
```

## Troubleshooting

**Triggers no se crearon:**
Si la Management API no tiene permisos para `CREATE TRIGGER`, ejecuta `004_triggers.sql` manualmente en el SQL Editor de Supabase.

**pg_trgm no disponible:**
En algunos planes de Supabase la extension puede no estar. El agente tambien usa `levenshtein()` de `fuzzystrmatch` como fallback.

**Muchos falsos positivos:**
Sube el `similarity_threshold` en `dedup_config` (ej: de 0.6 a 0.75).

**Muchos falsos negativos:**
Baja el threshold o agrega mas `scope_columns` para acotar la busqueda.
