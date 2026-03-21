@echo off
REM ============================================
REM Agente de Enriquecimiento + Supabase Upload
REM ============================================
REM
REM Configura tus credenciales de Supabase aqui:
set PROPYTE_SUPABASE_URL=https://yjbrynsykkcozeybykj.supabase.co
set PROPYTE_SUPABASE_SERVICE_KEY=TU_SERVICE_ROLE_KEY_AQUI

REM === OPCIONES ===
REM Cambiar segun lo que necesites:

REM Opcion 1: Solo diagnostico (sin buscar ni subir)
REM python agente_enriquecimiento.py --dry-run

REM Opcion 2: Enriquecer 10 proyectos + subir a Supabase
REM python agente_enriquecimiento.py --limit 10

REM Opcion 3: Enriquecer TODO + subir (puede tardar horas)
REM python agente_enriquecimiento.py

REM Opcion 4: Enriquecer + subir, actualizando existentes
REM python agente_enriquecimiento.py --match-existing

REM Opcion 5: Solo enriquecer sin subir
REM python agente_enriquecimiento.py --no-upload --limit 50

REM === EJECUTAR (descomenta la opcion que quieras) ===
python agente_enriquecimiento.py --limit 10 --match-existing

pause
