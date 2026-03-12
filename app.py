import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime

PROYECTOS_CSV = "proyectos.csv"
INVENTARIO_CSV = "inventario.csv"
MOVIMIENTOS_CSV = "movimientos.csv"
CONFIG_FILE = "config.json"

COLUMNAS_PROYECTOS = [
    "nombre_proyecto", "desarrolladora", "ciudad", "url_carpeta_drive", "notas", "total_unidades", "inicio_ventas"
]

COLORES_ESTADO = {
    "disponible": "#2ecc71",
    "vendido": "#e74c3c",
    "apartado": "#f39c12",
}


def cargar_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {"frecuencia_dias": 7}


def guardar_config(config):
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


def cargar_proyectos():
    if os.path.exists(PROYECTOS_CSV):
        df = pd.read_csv(PROYECTOS_CSV)
        for col in COLUMNAS_PROYECTOS:
            if col not in df.columns:
                df[col] = ""
        return df
    return pd.DataFrame(columns=COLUMNAS_PROYECTOS)


def guardar_proyectos(df):
    df.to_csv(PROYECTOS_CSV, index=False)


def cargar_inventario():
    if os.path.exists(INVENTARIO_CSV):
        return pd.read_csv(INVENTARIO_CSV)
    return pd.DataFrame()


def cargar_movimientos():
    if os.path.exists(MOVIMIENTOS_CSV):
        return pd.read_csv(MOVIMIENTOS_CSV)
    return pd.DataFrame(columns=["proyecto", "unidad", "estado_anterior", "estado_nuevo", "fecha_cambio"])


# --- UI ---

st.set_page_config(page_title="Analista de Inventario", page_icon="🏗️", layout="wide")
st.title("🏗️ Analista de Inventario Inmobiliario")

tab_dashboard, tab_movimientos, tab_proyectos, tab_agregar, tab_inventario, tab_config = st.tabs([
    "📈 Dashboard", "🔄 Movimientos", "📋 Proyectos", "➕ Agregar Proyecto", "📊 Inventario", "⚙️ Configuración"
])

# --- TAB: Dashboard ---
with tab_dashboard:
    df_inventario = cargar_inventario()
    config = cargar_config()

    if df_inventario.empty:
        st.info("No hay datos de inventario. Ejecuta `/analista-inventario` en Claude Code.")
    else:
        # Determinar ultima revision
        ultima_fecha = df_inventario["fecha_revision"].max()
        df_ultimo = df_inventario[df_inventario["fecha_revision"] == ultima_fecha]

        st.markdown(f"### Ultima revision: `{ultima_fecha}`")
        st.caption(f"Frecuencia configurada: cada {config.get('frecuencia_dias', 7)} dias")

        st.divider()

        # --- Metricas generales ---
        proyectos_unicos = df_ultimo["proyecto"].nunique()
        total_unidades = len(df_ultimo)
        disponibles = len(df_ultimo[df_ultimo["estado"] == "disponible"])
        vendidas = len(df_ultimo[df_ultimo["estado"] == "vendido"])
        apartadas = len(df_ultimo[df_ultimo["estado"] == "apartado"])
        absorcion = (vendidas / total_unidades * 100) if total_unidades > 0 else 0

        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("Proyectos", proyectos_unicos)
        col2.metric("Total unidades", total_unidades)
        col3.metric("Disponibles", disponibles)
        col4.metric("Vendidas", vendidas)
        col5.metric("Apartadas", apartadas)
        col6.metric("Absorcion", f"{absorcion:.0f}%")

        st.divider()

        # --- Resumen por proyecto ---
        st.subheader("Resumen por proyecto")
        df_proyectos_dash = cargar_proyectos()

        for proyecto in df_ultimo["proyecto"].unique():
            df_proy = df_ultimo[df_ultimo["proyecto"] == proyecto]
            proy_en_csv = len(df_proy)
            proy_disp = len(df_proy[df_proy["estado"] == "disponible"])
            proy_vend = len(df_proy[df_proy["estado"] == "vendido"])
            proy_apart = len(df_proy[df_proy["estado"] == "apartado"])

            # Usar total_unidades de proyectos.csv si existe
            proy_total = proy_en_csv
            total_override = None
            if not df_proyectos_dash.empty and "total_unidades" in df_proyectos_dash.columns:
                match = df_proyectos_dash[df_proyectos_dash["nombre_proyecto"] == proyecto]
                if not match.empty:
                    val = match.iloc[0].get("total_unidades")
                    if pd.notna(val) and val > 0:
                        total_override = int(val)
                        proy_total = total_override

            proy_absorcion = (proy_vend / proy_total * 100) if proy_total > 0 else 0

            precios = df_proy["precio_lista_mxn"].dropna()
            precios_disp = df_proy[df_proy["estado"] == "disponible"]["precio_lista_mxn"].dropna()

            label_total = f"{proy_total} unidades"
            if total_override and total_override != proy_en_csv:
                label_total = f"{proy_en_csv} listadas / {proy_total} totales"

            with st.expander(f"**{proyecto}** — {label_total} | Absorcion: {proy_absorcion:.0f}%", expanded=True):
                # Barra de absorcion visual
                pct_vend = proy_vend / proy_total * 100 if proy_total else 0
                pct_apart = proy_apart / proy_total * 100 if proy_total else 0
                pct_disp = proy_disp / proy_total * 100 if proy_total else 0

                st.markdown(
                    f"""<div style="display:flex;height:30px;border-radius:6px;overflow:hidden;margin-bottom:12px;">
                        <div style="width:{pct_vend}%;background:{COLORES_ESTADO['vendido']};display:flex;align-items:center;justify-content:center;color:white;font-size:12px;font-weight:bold;">{proy_vend}</div>
                        <div style="width:{pct_apart}%;background:{COLORES_ESTADO['apartado']};display:flex;align-items:center;justify-content:center;color:white;font-size:12px;font-weight:bold;">{proy_apart if proy_apart else ''}</div>
                        <div style="width:{pct_disp}%;background:{COLORES_ESTADO['disponible']};display:flex;align-items:center;justify-content:center;color:white;font-size:12px;font-weight:bold;">{proy_disp}</div>
                    </div>""",
                    unsafe_allow_html=True,
                )

                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("Disponibles", proy_disp)
                mc2.metric("Vendidas", proy_vend)
                mc3.metric("Apartadas", proy_apart)
                if not precios_disp.empty:
                    mc4.metric("Rango disponible", f"${precios_disp.min():,.0f} - ${precios_disp.max():,.0f}")
                elif not precios.empty:
                    mc4.metric("Precio promedio", f"${precios.mean():,.0f}")
                else:
                    mc4.metric("Precios", "Sin datos")

                # Velocidad de absorcion mensual
                inicio_ventas = None
                if not df_proyectos_dash.empty and "inicio_ventas" in df_proyectos_dash.columns:
                    match = df_proyectos_dash[df_proyectos_dash["nombre_proyecto"] == proyecto]
                    if not match.empty:
                        val = match.iloc[0].get("inicio_ventas")
                        if pd.notna(val) and str(val).strip():
                            inicio_ventas = str(val).strip()

                if inicio_ventas:
                    try:
                        from dateutil.relativedelta import relativedelta
                        fecha_inicio = datetime.strptime(inicio_ventas, "%Y-%m")
                        meses = max(1, (datetime.now().year - fecha_inicio.year) * 12 + datetime.now().month - fecha_inicio.month)
                        vel_mensual = round(proy_vend / meses, 1)
                        meses_restantes = round(proy_disp / vel_mensual, 1) if vel_mensual > 0 else float('inf')

                        mc5, mc6, mc7 = st.columns(3)
                        mc5.metric("Inicio ventas", inicio_ventas)
                        mc6.metric("Vel. mensual", f"{vel_mensual} uds/mes")
                        if meses_restantes != float('inf'):
                            mc7.metric("Meses p/agotar", f"{meses_restantes}")
                        else:
                            mc7.metric("Meses p/agotar", "N/A")
                    except Exception:
                        pass

                # Alertas de datos faltantes
                alertas = []
                if not total_override:
                    alertas.append("Total de unidades no identificado")
                if not inicio_ventas:
                    alertas.append("Fecha de inicio de ventas no identificada")
                if alertas:
                    st.warning(f"⚠️ Datos faltantes: {' | '.join(alertas)}")

                # Editor inline siempre disponible
                with st.expander("✏️ Editar datos del proyecto"):
                    col_a, col_b, col_btn = st.columns([2, 2, 1])
                    new_total = col_a.number_input(
                        "Total unidades", min_value=1, step=1,
                        key=f"fix_total_{proyecto}",
                        value=int(total_override) if total_override else None,
                        placeholder="Ej: 75"
                    )
                    new_inicio = col_b.text_input(
                        "Inicio ventas (YYYY-MM)",
                        key=f"fix_inicio_{proyecto}",
                        value=inicio_ventas if inicio_ventas else "",
                        placeholder="Ej: 2025-06"
                    )
                    if col_btn.button("💾 Guardar", key=f"fix_btn_{proyecto}"):
                        df_proy_edit = cargar_proyectos()
                        idx = df_proy_edit[df_proy_edit["nombre_proyecto"] == proyecto].index
                        if not idx.empty:
                            if new_total:
                                df_proy_edit.loc[idx[0], "total_unidades"] = int(new_total)
                            if new_inicio is not None:
                                df_proy_edit.loc[idx[0], "inicio_ventas"] = new_inicio.strip()
                            df_proy_edit.to_csv(PROYECTOS_CSV, index=False)
                            st.rerun()

                # Tabla por nivel
                resumen_nivel = df_proy.groupby("piso").agg(
                    total=("unidad", "count"),
                    disponibles=("estado", lambda x: (x == "disponible").sum()),
                    vendidas=("estado", lambda x: (x == "vendido").sum()),
                    apartadas=("estado", lambda x: (x == "apartado").sum()),
                ).reset_index()
                resumen_nivel.columns = ["Nivel", "Total", "Disponibles", "Vendidas", "Apartadas"]
                st.dataframe(resumen_nivel, use_container_width=True, hide_index=True)

        st.divider()

        # --- Comparativa de absorcion ---
        st.subheader("Comparativa de absorcion")

        absorcion_data = []
        for proyecto in df_ultimo["proyecto"].unique():
            df_proy = df_ultimo[df_ultimo["proyecto"] == proyecto]
            proy_total = len(df_proy)
            proy_vend = len(df_proy[df_proy["estado"] == "vendido"])

            # Usar total_unidades de proyectos.csv si existe
            if not df_proyectos_dash.empty and "total_unidades" in df_proyectos_dash.columns:
                match = df_proyectos_dash[df_proyectos_dash["nombre_proyecto"] == proyecto]
                if not match.empty:
                    val = match.iloc[0].get("total_unidades")
                    if pd.notna(val) and val > 0:
                        proy_total = int(val)

            absorcion_data.append({
                "Proyecto": proyecto,
                "Absorcion %": round(proy_vend / proy_total * 100, 1) if proy_total else 0,
                "Vendidas": proy_vend,
                "Total": proy_total,
            })

        df_absorcion = pd.DataFrame(absorcion_data).sort_values("Absorcion %", ascending=False)
        # Tabla de absorcion con barras HTML (sin altair)
        for _, row in df_absorcion.iterrows():
            pct = row["Absorcion %"]
            st.markdown(
                f"""<div style="display:flex;align-items:center;margin-bottom:4px;">
                    <span style="width:150px;font-size:14px;">{row['Proyecto']}</span>
                    <div style="flex:1;background:#333;border-radius:4px;height:20px;margin:0 8px;">
                        <div style="width:{pct}%;background:#e74c3c;height:100%;border-radius:4px;"></div>
                    </div>
                    <span style="width:50px;font-size:14px;text-align:right;">{pct:.0f}%</span>
                </div>""",
                unsafe_allow_html=True,
            )

        # --- Historial si hay multiples fechas ---
        fechas_unicas = sorted(df_inventario["fecha_revision"].unique())
        if len(fechas_unicas) > 1:
            st.divider()
            st.subheader("Historial de absorcion")

            historial = []
            for fecha in fechas_unicas:
                df_fecha = df_inventario[df_inventario["fecha_revision"] == fecha]
                for proyecto in df_fecha["proyecto"].unique():
                    df_pf = df_fecha[df_fecha["proyecto"] == proyecto]
                    total = len(df_pf)
                    vendidas = len(df_pf[df_pf["estado"] == "vendido"])
                    historial.append({
                        "fecha": fecha,
                        "proyecto": proyecto,
                        "absorcion": round(vendidas / total * 100, 1) if total else 0,
                    })

            df_hist = pd.DataFrame(historial)
            df_pivot = df_hist.pivot(index="fecha", columns="proyecto", values="absorcion").reset_index()
            st.dataframe(df_pivot, use_container_width=True, hide_index=True)


# --- TAB: Movimientos ---
with tab_movimientos:
    st.header("Historial de movimientos")
    st.caption("Cambios de estado detectados entre revisiones (disponible → apartado → vendido)")

    df_mov = cargar_movimientos()
    df_inv_mov = cargar_inventario()

    if df_mov.empty and len(df_inv_mov["fecha_revision"].unique()) <= 1 if not df_inv_mov.empty else True:
        st.info("Los movimientos se registran automaticamente cuando se detectan cambios de estado entre revisiones semanales. Aun no hay multiples revisiones.")
    else:
        if not df_mov.empty:
            # Filtros
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                proy_mov = ["Todos"] + df_mov["proyecto"].unique().tolist()
                filtro_proy_mov = st.selectbox("Proyecto", proy_mov, key="mov_proy")
            with col_f2:
                tipo_mov = ["Todos"] + sorted(df_mov["estado_nuevo"].unique().tolist())
                filtro_tipo_mov = st.selectbox("Nuevo estado", tipo_mov, key="mov_tipo")

            df_mov_f = df_mov.copy()
            if filtro_proy_mov != "Todos":
                df_mov_f = df_mov_f[df_mov_f["proyecto"] == filtro_proy_mov]
            if filtro_tipo_mov != "Todos":
                df_mov_f = df_mov_f[df_mov_f["estado_nuevo"] == filtro_tipo_mov]

            # Metricas de movimientos
            col_mv1, col_mv2, col_mv3 = st.columns(3)
            col_mv1.metric("Total movimientos", len(df_mov_f))
            col_mv2.metric("Nuevas ventas", len(df_mov_f[df_mov_f["estado_nuevo"] == "vendido"]))
            col_mv3.metric("Nuevos apartados", len(df_mov_f[df_mov_f["estado_nuevo"] == "apartado"]))

            st.divider()

            # Tabla de movimientos (mas recientes primero)
            df_mov_display = df_mov_f.sort_values("fecha_cambio", ascending=False)
            st.dataframe(df_mov_display, use_container_width=True, hide_index=True)

            # Grafico de movimientos por semana
            if len(df_mov_f) > 0:
                st.divider()
                st.subheader("Velocidad de ventas por semana")

                df_ventas = df_mov_f[df_mov_f["estado_nuevo"] == "vendido"].copy()
                if not df_ventas.empty:
                    df_ventas["semana"] = pd.to_datetime(df_ventas["fecha_cambio"]).dt.isocalendar().week.astype(str) + "-" + pd.to_datetime(df_ventas["fecha_cambio"]).dt.isocalendar().year.astype(str)
                    ventas_semana = df_ventas.groupby(["fecha_cambio", "proyecto"]).size().reset_index(name="ventas")
                    ventas_pivot = ventas_semana.pivot(index="fecha_cambio", columns="proyecto", values="ventas").fillna(0).reset_index()
                    st.dataframe(ventas_pivot, use_container_width=True, hide_index=True)
                else:
                    st.info("No hay ventas registradas en los movimientos.")

    # Seccion: detectar movimientos desde inventario historico
    if not df_inv_mov.empty:
        fechas = sorted(df_inv_mov["fecha_revision"].unique())
        if len(fechas) > 1:
            st.divider()
            st.subheader("Absorcion historica por proyecto")

            historial = []
            for fecha in fechas:
                df_f = df_inv_mov[df_inv_mov["fecha_revision"] == fecha]
                for proy in df_f["proyecto"].unique():
                    df_pf = df_f[df_f["proyecto"] == proy]
                    total = len(df_pf)
                    vendidas = len(df_pf[df_pf["estado"] == "vendido"])
                    disponibles = len(df_pf[df_pf["estado"] == "disponible"])
                    apartadas = len(df_pf[df_pf["estado"] == "apartado"])
                    historial.append({
                        "fecha": fecha,
                        "proyecto": proy,
                        "vendidas": vendidas,
                        "disponibles": disponibles,
                        "apartadas": apartadas,
                    })

            df_hist = pd.DataFrame(historial)

            # Grafico de disponibles por fecha (baja = se estan vendiendo)
            st.markdown("**Unidades disponibles por revision** (si baja, se estan vendiendo)")
            df_disp_pivot = df_hist.pivot(index="fecha", columns="proyecto", values="disponibles").reset_index()
            st.dataframe(df_disp_pivot, use_container_width=True, hide_index=True)

            st.markdown("**Unidades vendidas por revision** (si sube, hay absorcion)")
            df_vend_pivot = df_hist.pivot(index="fecha", columns="proyecto", values="vendidas").reset_index()
            st.dataframe(df_vend_pivot, use_container_width=True, hide_index=True)


# --- TAB: Proyectos ---
with tab_proyectos:
    st.header("Proyectos registrados")
    df_proyectos = cargar_proyectos()

    # Convertir columnas problematicas a string para el editor
    for col in ["inicio_ventas", "notas"]:
        if col in df_proyectos.columns:
            df_proyectos[col] = df_proyectos[col].fillna("").astype(str)

    if df_proyectos.empty:
        st.info("No hay proyectos registrados. Ve a la pestaña 'Agregar Proyecto'.")
    else:
        editado = st.data_editor(
            df_proyectos,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "nombre_proyecto": st.column_config.TextColumn("Proyecto", width="medium"),
                "desarrolladora": st.column_config.TextColumn("Desarrolladora", width="medium"),
                "ciudad": st.column_config.TextColumn("Ciudad", width="small"),
                "url_carpeta_drive": st.column_config.TextColumn("URL Google Drive", width="large"),
                "notas": st.column_config.TextColumn("Notas", width="large"),
                "total_unidades": st.column_config.NumberColumn("Total unidades", width="small", help="Total real de unidades del proyecto. Solo llenar si el skill no logra detectarlo."),
                "inicio_ventas": st.column_config.TextColumn("Inicio ventas", width="small", help="Mes y año de inicio de ventas (YYYY-MM). Ej: 2025-06"),
            },
            key="editor_proyectos",
        )

        if st.button("💾 Guardar cambios", key="guardar_proyectos"):
            guardar_proyectos(editado)
            st.success("Proyectos guardados correctamente.")

        st.divider()

        st.subheader("Eliminar proyecto")
        proyecto_eliminar = st.selectbox(
            "Selecciona el proyecto a eliminar",
            df_proyectos["nombre_proyecto"].tolist(),
            key="select_eliminar",
        )
        if st.button("🗑️ Eliminar proyecto", type="secondary"):
            df_filtrado = df_proyectos[df_proyectos["nombre_proyecto"] != proyecto_eliminar]
            guardar_proyectos(df_filtrado)
            st.success(f"'{proyecto_eliminar}' eliminado.")
            st.rerun()

# --- TAB: Agregar ---
with tab_agregar:
    st.header("Agregar nuevo proyecto")

    with st.form("form_agregar", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            nombre = st.text_input("Nombre del proyecto *")
            desarrolladora = st.text_input("Desarrolladora")
            ciudad = st.text_input("Ciudad")
        with col2:
            url_drive = st.text_input("URL carpeta Google Drive *")
            notas = st.text_area("Notas", placeholder="Ej: PDF se llama 'Lista de Precios Marzo'")

        submitted = st.form_submit_button("➕ Agregar proyecto", type="primary")

        if submitted:
            if not nombre or not url_drive:
                st.error("El nombre y la URL de Drive son obligatorios.")
            else:
                df_proyectos = cargar_proyectos()
                nuevo = pd.DataFrame([{
                    "nombre_proyecto": nombre,
                    "desarrolladora": desarrolladora,
                    "ciudad": ciudad,
                    "url_carpeta_drive": url_drive,
                    "notas": notas,
                }])
                df_proyectos = pd.concat([df_proyectos, nuevo], ignore_index=True)
                guardar_proyectos(df_proyectos)
                st.success(f"'{nombre}' agregado correctamente.")

# --- TAB: Inventario ---
with tab_inventario:
    st.header("Resultados del inventario")
    df_inventario_tab = cargar_inventario()

    if df_inventario_tab.empty:
        st.info("No hay datos de inventario. Ejecuta `/analista-inventario` en Claude Code.")
    else:
        # Filtros
        col_filtro1, col_filtro2, col_filtro3 = st.columns(3)
        with col_filtro1:
            proyectos_disponibles = ["Todos"] + df_inventario_tab["proyecto"].unique().tolist()
            filtro_proyecto = st.selectbox("Proyecto", proyectos_disponibles)
        with col_filtro2:
            estados_disponibles = ["Todos"] + df_inventario_tab["estado"].unique().tolist()
            filtro_estado = st.selectbox("Estado", estados_disponibles)
        with col_filtro3:
            fechas_disponibles = ["Todas"] + sorted(df_inventario_tab["fecha_revision"].unique().tolist(), reverse=True)
            filtro_fecha = st.selectbox("Fecha revision", fechas_disponibles)

        df_filtrado = df_inventario_tab.copy()
        if filtro_proyecto != "Todos":
            df_filtrado = df_filtrado[df_filtrado["proyecto"] == filtro_proyecto]
        if filtro_estado != "Todos":
            df_filtrado = df_filtrado[df_filtrado["estado"] == filtro_estado]
        if filtro_fecha != "Todas":
            df_filtrado = df_filtrado[df_filtrado["fecha_revision"] == filtro_fecha]

        # Metricas
        ultima_fecha = df_filtrado["fecha_revision"].max() if not df_filtrado.empty else "N/A"
        df_ultima = df_filtrado[df_filtrado["fecha_revision"] == ultima_fecha] if not df_filtrado.empty else df_filtrado

        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        with col_m1:
            st.metric("Total unidades", len(df_ultima))
        with col_m2:
            disponibles = len(df_ultima[df_ultima["estado"] == "disponible"])
            st.metric("Disponibles", disponibles)
        with col_m3:
            apartadas = len(df_ultima[df_ultima["estado"] == "apartado"])
            st.metric("Apartadas", apartadas)
        with col_m4:
            if not df_ultima.empty and "precio_lista_mxn" in df_ultima.columns:
                precios = df_ultima["precio_lista_mxn"].dropna()
                if not precios.empty:
                    st.metric("Precio promedio", f"${precios.mean():,.0f} MXN")
                else:
                    st.metric("Precio promedio", "N/A")

        st.divider()
        st.dataframe(df_filtrado, use_container_width=True, hide_index=True)

        # Descargar CSV filtrado
        csv_export = df_filtrado.to_csv(index=False)
        st.download_button(
            "📥 Descargar CSV filtrado",
            csv_export,
            file_name=f"inventario_filtrado_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

# --- TAB: Configuracion ---
with tab_config:
    st.header("Configuracion de revision")
    config = cargar_config()

    frecuencia = st.number_input(
        "Frecuencia de revision (dias)",
        min_value=1,
        max_value=365,
        value=config.get("frecuencia_dias", 7),
        help="Cada cuantos dias se ejecuta la revision automatica.",
    )

    if st.button("💾 Guardar configuracion"):
        config["frecuencia_dias"] = frecuencia
        guardar_config(config)
        st.success(f"Configuracion guardada. Frecuencia: cada {frecuencia} dias.")
        st.code(f"/loop {frecuencia}d /analista-inventario", language="bash")
        st.caption("Ejecuta este comando en Claude Code para activar la revision automatica.")

    st.divider()
    st.subheader("Comando para revision automatica")
    st.code(f"/loop {config.get('frecuencia_dias', 7)}d /analista-inventario", language="bash")
    st.caption("Copia y pega este comando en Claude Code para activar la revision periodica.")
