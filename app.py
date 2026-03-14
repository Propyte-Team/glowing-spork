import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime

PROYECTOS_CSV = "proyectos.csv"
INVENTARIO_CSV = "inventario.csv"
MOVIMIENTOS_CSV = "movimientos.csv"
VENTAS_CSV = "ventas.csv"
INCREMENTOS_CSV = "incrementos_precios.csv"
CONFIG_FILE = "config.json"

COLUMNAS_PROYECTOS = [
    "nombre_proyecto", "desarrolladora", "ciudad", "url_carpeta_drive", "notas", "total_unidades", "inicio_ventas"
]

# CRM color palette
NAVY = "#1E3A5F"
TEAL = "#00B4C8"
AMBER = "#F5A623"
LIGHT_BG = "#F4F6F8"
WHITE = "#FFFFFF"
GRAPHITE = "#2C2C2C"
RED = "#E74C3C"
GREEN = "#2ECC71"
ORANGE = "#F39C12"

COLORES_ESTADO = {
    "disponible": GREEN,
    "vendido": RED,
    "apartado": ORANGE,
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


def cargar_ventas():
    if os.path.exists(VENTAS_CSV):
        return pd.read_csv(VENTAS_CSV)
    return pd.DataFrame(columns=["proyecto", "unidad", "tipologia", "precio_ultimo_lista", "fecha_venta"])


def cargar_incrementos():
    if os.path.exists(INCREMENTOS_CSV):
        return pd.read_csv(INCREMENTOS_CSV)
    return pd.DataFrame(columns=["proyecto", "unidad", "tipologia", "precio_anterior", "precio_nuevo", "incremento_pct", "fecha_registro"])


# --- KPI card helper ---
def kpi_card(label, value, icon="", color=NAVY, subtitle=""):
    subtitle_html = f'<div style="font-size:11px;color:#94a3b8;margin-top:2px;">{subtitle}</div>' if subtitle else ""
    return f"""
    <div style="background:{WHITE};border:1px solid #e2e8f0;border-radius:12px;padding:20px 24px;
                box-shadow:0 1px 3px rgba(0,0,0,0.06);transition:box-shadow 0.2s;">
        <div style="display:flex;align-items:center;gap:12px;">
            <div style="background:{color}15;color:{color};width:40px;height:40px;border-radius:50%;
                        display:flex;align-items:center;justify-content:center;font-size:18px;flex-shrink:0;">
                {icon}
            </div>
            <div>
                <div style="font-size:12px;font-weight:500;color:#64748b;text-transform:uppercase;
                            letter-spacing:0.5px;">{label}</div>
                <div style="font-size:24px;font-weight:700;color:{GRAPHITE};line-height:1.2;">{value}</div>
                {subtitle_html}
            </div>
        </div>
    </div>"""


def section_header(title, subtitle=""):
    sub = f'<p style="font-size:13px;color:#64748b;margin:4px 0 0 0;">{subtitle}</p>' if subtitle else ""
    return f"""
    <div style="margin:32px 0 20px 0;">
        <h2 style="font-family:Inter,sans-serif;font-size:20px;font-weight:600;color:{NAVY};margin:0;">
            {title}
        </h2>
        {sub}
    </div>"""


def card_container(content):
    return f"""
    <div style="background:{WHITE};border:1px solid #e2e8f0;border-radius:12px;padding:24px;
                box-shadow:0 1px 3px rgba(0,0,0,0.06);margin-bottom:16px;">
        {content}
    </div>"""


def badge(text, color):
    return f'<span style="background:{color}20;color:{color};padding:3px 10px;border-radius:20px;font-size:12px;font-weight:600;">{text}</span>'


def empty_state(icon, title, description):
    return f"""
    <div style="text-align:center;padding:60px 20px;">
        <div style="font-size:48px;margin-bottom:16px;opacity:0.4;">{icon}</div>
        <div style="font-size:18px;font-weight:600;color:{NAVY};margin-bottom:8px;">{title}</div>
        <div style="font-size:14px;color:#64748b;max-width:400px;margin:0 auto;">{description}</div>
    </div>"""


# --- UI Setup ---

st.set_page_config(page_title="Analista de Inventario", page_icon="🏗️", layout="wide")

# Custom CSS matching CRM design
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* Global font */
    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }}

    /* Hide default Streamlit header/footer */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}

    /* Main container background */
    .stApp {{
        background-color: {LIGHT_BG};
    }}

    /* Custom header bar */
    .main-header {{
        background: linear-gradient(135deg, {NAVY} 0%, #2a4f7a 100%);
        padding: 24px 32px;
        border-radius: 0 0 16px 16px;
        margin: -1rem -1rem 24px -1rem;
        box-shadow: 0 4px 12px rgba(30,58,95,0.15);
    }}
    .main-header h1 {{
        color: white;
        font-size: 26px;
        font-weight: 700;
        margin: 0;
        letter-spacing: -0.5px;
    }}
    .main-header p {{
        color: rgba(255,255,255,0.7);
        font-size: 13px;
        margin: 4px 0 0 0;
    }}

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 0px;
        background: {WHITE};
        border-radius: 12px;
        padding: 4px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    }}
    .stTabs [data-baseweb="tab"] {{
        border-radius: 8px;
        padding: 10px 20px;
        font-size: 13px;
        font-weight: 500;
        color: #64748b;
        background: transparent;
        border: none;
    }}
    .stTabs [aria-selected="true"] {{
        background: {NAVY} !important;
        color: white !important;
        font-weight: 600;
        box-shadow: 0 2px 4px rgba(30,58,95,0.2);
    }}
    .stTabs [data-baseweb="tab-highlight"] {{
        display: none;
    }}
    .stTabs [data-baseweb="tab-border"] {{
        display: none;
    }}

    /* Metric cards override */
    [data-testid="stMetric"] {{
        background: {WHITE};
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }}
    [data-testid="stMetricLabel"] {{
        font-size: 12px !important;
        font-weight: 500 !important;
        color: #64748b !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    [data-testid="stMetricValue"] {{
        font-size: 22px !important;
        font-weight: 700 !important;
        color: {GRAPHITE} !important;
    }}

    /* Dataframe styling */
    [data-testid="stDataFrame"] {{
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }}

    /* Button styling */
    .stButton > button {{
        background: {NAVY};
        color: white;
        border: none;
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: 600;
        font-size: 13px;
        transition: all 0.2s;
    }}
    .stButton > button:hover {{
        background: #2a4f7a;
        box-shadow: 0 2px 8px rgba(30,58,95,0.25);
    }}
    .stButton > button[kind="secondary"] {{
        background: transparent;
        color: {RED};
        border: 1px solid {RED};
    }}

    /* Download button */
    .stDownloadButton > button {{
        background: {WHITE};
        color: {NAVY};
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        font-weight: 500;
    }}
    .stDownloadButton > button:hover {{
        background: {LIGHT_BG};
        border-color: {TEAL};
        color: {TEAL};
    }}

    /* Form styling */
    .stForm {{
        background: {WHITE};
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 24px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }}

    /* Input styling */
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stTextArea > div > div > textarea {{
        border-radius: 8px;
        border: 1px solid #e2e8f0;
        font-family: 'Inter', sans-serif;
    }}
    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {{
        border-color: {TEAL};
        box-shadow: 0 0 0 3px {TEAL}20;
    }}

    /* Select box styling */
    .stSelectbox > div > div {{
        border-radius: 8px;
    }}

    /* Expander styling */
    .streamlit-expanderHeader {{
        background: {WHITE};
        border-radius: 8px;
        font-weight: 600;
        color: {NAVY};
    }}

    /* Divider */
    hr {{
        border-color: #e2e8f0 !important;
        margin: 24px 0 !important;
    }}

    /* Success/Info/Warning messages */
    .stAlert {{
        border-radius: 10px;
        border: none;
    }}

    /* Data editor */
    [data-testid="stDataEditor"] {{
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        overflow: hidden;
    }}

    /* Code blocks */
    .stCodeBlock {{
        border-radius: 8px;
    }}
</style>
""", unsafe_allow_html=True)

# Header
st.markdown(f"""
<div class="main-header">
    <h1>Analista de Inventario</h1>
    <p>Monitoreo de proyectos inmobiliarios &bull; Precios, ventas y absorcion en tiempo real</p>
</div>
""", unsafe_allow_html=True)

tab_dashboard, tab_ventas, tab_precios, tab_movimientos, tab_proyectos, tab_agregar, tab_inventario, tab_config = st.tabs([
    "Dashboard", "Ventas", "Precios", "Movimientos", "Proyectos", "Agregar", "Inventario", "Config"
])

# --- TAB: Dashboard ---
with tab_dashboard:
    df_inventario = cargar_inventario()
    config = cargar_config()

    if df_inventario.empty:
        st.markdown(empty_state(
            "📊", "Sin datos de inventario",
            "Ejecuta /analista-inventario en Claude Code para analizar tus proyectos."
        ), unsafe_allow_html=True)
    else:
        ultima_fecha = df_inventario["fecha_revision"].max()
        df_ultimo = df_inventario[df_inventario["fecha_revision"] == ultima_fecha]

        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:24px;">
            <div style="background:{TEAL}15;color:{TEAL};padding:6px 14px;border-radius:20px;font-size:12px;font-weight:600;">
                Ultima revision: {ultima_fecha}
            </div>
            <div style="color:#94a3b8;font-size:12px;">
                Frecuencia: cada {config.get('frecuencia_dias', 7)} dias
            </div>
        </div>
        """, unsafe_allow_html=True)

        # KPI metrics
        proyectos_unicos = df_ultimo["proyecto"].nunique()
        total_unidades = len(df_ultimo)
        disponibles = len(df_ultimo[df_ultimo["estado"] == "disponible"])
        vendidas = len(df_ultimo[df_ultimo["estado"] == "vendido"])
        apartadas = len(df_ultimo[df_ultimo["estado"] == "apartado"])
        absorcion = (vendidas / total_unidades * 100) if total_unidades > 0 else 0

        cols = st.columns(6)
        kpis = [
            ("Proyectos", proyectos_unicos, "🏢", NAVY),
            ("Total unidades", total_unidades, "🏠", NAVY),
            ("Disponibles", disponibles, "🟢", GREEN),
            ("Vendidas", vendidas, "🔴", RED),
            ("Apartadas", apartadas, "🟡", ORANGE),
            ("Absorcion", f"{absorcion:.0f}%", "📈", TEAL),
        ]
        for col, (label, value, icon, color) in zip(cols, kpis):
            col.markdown(kpi_card(label, value, icon, color), unsafe_allow_html=True)

        # Recent activity indicators
        df_ventas_dash = cargar_ventas()
        df_inc_dash = cargar_incrementos()
        if not df_ventas_dash.empty or not df_inc_dash.empty:
            st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
            col_r1, col_r2 = st.columns(2)
            if not df_ventas_dash.empty:
                ventas_recientes = len(df_ventas_dash[df_ventas_dash["fecha_venta"] == ultima_fecha])
                col_r1.markdown(kpi_card("Ventas esta revision", ventas_recientes, "💰", RED), unsafe_allow_html=True)
            if not df_inc_dash.empty:
                inc_recientes = len(df_inc_dash[df_inc_dash["fecha_registro"] == ultima_fecha])
                col_r2.markdown(kpi_card("Cambios de precio", inc_recientes, "💲", AMBER), unsafe_allow_html=True)

        # Project summary
        st.markdown(section_header("Resumen por proyecto"), unsafe_allow_html=True)
        df_proyectos_dash = cargar_proyectos()

        for proyecto in df_ultimo["proyecto"].unique():
            df_proy = df_ultimo[df_ultimo["proyecto"] == proyecto]
            proy_en_csv = len(df_proy)
            proy_disp = len(df_proy[df_proy["estado"] == "disponible"])
            proy_vend = len(df_proy[df_proy["estado"] == "vendido"])
            proy_apart = len(df_proy[df_proy["estado"] == "apartado"])

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

            pct_vend = proy_vend / proy_total * 100 if proy_total else 0
            pct_apart = proy_apart / proy_total * 100 if proy_total else 0
            pct_disp = proy_disp / proy_total * 100 if proy_total else 0

            # Project card header with absorption bar
            precio_rango = ""
            if not precios_disp.empty:
                precio_rango = f"${precios_disp.min():,.0f} — ${precios_disp.max():,.0f}"
            elif not precios.empty:
                precio_rango = f"Prom: ${precios.mean():,.0f}"

            with st.expander(f"**{proyecto}** — {label_total} | Absorcion: {proy_absorcion:.0f}%", expanded=True):
                # Absorption bar
                st.markdown(f"""
                <div style="margin-bottom:20px;">
                    <div style="display:flex;justify-content:space-between;margin-bottom:6px;">
                        <div style="display:flex;gap:16px;font-size:12px;color:#64748b;">
                            <span>{badge(f'{proy_vend} vendidas', RED)} </span>
                            <span>{badge(f'{proy_apart} apartadas', ORANGE)} </span>
                            <span>{badge(f'{proy_disp} disponibles', GREEN)} </span>
                        </div>
                        <span style="font-size:13px;font-weight:600;color:{NAVY};">{proy_absorcion:.0f}% absorcion</span>
                    </div>
                    <div style="display:flex;height:12px;border-radius:6px;overflow:hidden;background:#e2e8f0;">
                        <div style="width:{pct_vend}%;background:{RED};transition:width 0.3s;"></div>
                        <div style="width:{pct_apart}%;background:{ORANGE};transition:width 0.3s;"></div>
                        <div style="width:{pct_disp}%;background:{GREEN};transition:width 0.3s;"></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("Disponibles", proy_disp)
                mc2.metric("Vendidas", proy_vend)
                mc3.metric("Apartadas", proy_apart)
                if precio_rango:
                    mc4.metric("Rango disponible", precio_rango)
                else:
                    mc4.metric("Precios", "Sin datos")

                # Monthly velocity
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

                # Missing data alerts
                alertas = []
                if not total_override:
                    alertas.append("Total de unidades no identificado")
                if not inicio_ventas:
                    alertas.append("Fecha de inicio de ventas no identificada")
                if alertas:
                    st.warning(f"Datos faltantes: {' | '.join(alertas)}")

                # Inline editor
                with st.expander("Editar datos del proyecto"):
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
                    if col_btn.button("Guardar", key=f"fix_btn_{proyecto}"):
                        df_proy_edit = cargar_proyectos()
                        idx = df_proy_edit[df_proy_edit["nombre_proyecto"] == proyecto].index
                        if not idx.empty:
                            if new_total:
                                df_proy_edit.loc[idx[0], "total_unidades"] = int(new_total)
                            if new_inicio is not None:
                                df_proy_edit.loc[idx[0], "inicio_ventas"] = new_inicio.strip()
                            df_proy_edit.to_csv(PROYECTOS_CSV, index=False)
                            st.rerun()

                # Level breakdown table
                resumen_nivel = df_proy.groupby("piso").agg(
                    total=("unidad", "count"),
                    disponibles=("estado", lambda x: (x == "disponible").sum()),
                    vendidas=("estado", lambda x: (x == "vendido").sum()),
                    apartadas=("estado", lambda x: (x == "apartado").sum()),
                ).reset_index()
                resumen_nivel.columns = ["Nivel", "Total", "Disponibles", "Vendidas", "Apartadas"]
                st.dataframe(resumen_nivel, use_container_width=True, hide_index=True)

        # Absorption comparison
        st.markdown(section_header("Comparativa de absorcion"), unsafe_allow_html=True)

        absorcion_data = []
        for proyecto in df_ultimo["proyecto"].unique():
            df_proy = df_ultimo[df_ultimo["proyecto"] == proyecto]
            proy_total = len(df_proy)
            proy_vend = len(df_proy[df_proy["estado"] == "vendido"])

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

        bars_html = ""
        for _, row in df_absorcion.iterrows():
            pct = row["Absorcion %"]
            bars_html += f"""
            <div style="display:flex;align-items:center;margin-bottom:12px;">
                <span style="width:160px;font-size:14px;font-weight:500;color:{GRAPHITE};">{row['Proyecto']}</span>
                <div style="flex:1;background:#e2e8f0;border-radius:6px;height:24px;margin:0 12px;overflow:hidden;">
                    <div style="width:{pct}%;background:linear-gradient(90deg, {NAVY}, {TEAL});height:100%;border-radius:6px;
                                display:flex;align-items:center;justify-content:flex-end;padding-right:8px;
                                min-width:40px;transition:width 0.3s;">
                        <span style="color:white;font-size:11px;font-weight:600;">{pct:.0f}%</span>
                    </div>
                </div>
                <span style="width:80px;font-size:13px;color:#64748b;text-align:right;">{row['Vendidas']}/{row['Total']}</span>
            </div>"""

        st.markdown(card_container(bars_html), unsafe_allow_html=True)

        # History
        fechas_unicas = sorted(df_inventario["fecha_revision"].unique())
        if len(fechas_unicas) > 1:
            st.markdown(section_header("Historial de absorcion"), unsafe_allow_html=True)

            historial = []
            for fecha in fechas_unicas:
                df_fecha = df_inventario[df_inventario["fecha_revision"] == fecha]
                for proyecto in df_fecha["proyecto"].unique():
                    df_pf = df_fecha[df_fecha["proyecto"] == proyecto]
                    total = len(df_pf)
                    vendidas_h = len(df_pf[df_pf["estado"] == "vendido"])
                    historial.append({
                        "fecha": fecha,
                        "proyecto": proyecto,
                        "absorcion": round(vendidas_h / total * 100, 1) if total else 0,
                    })

            df_hist = pd.DataFrame(historial)
            df_pivot = df_hist.pivot(index="fecha", columns="proyecto", values="absorcion").reset_index()
            st.dataframe(df_pivot, use_container_width=True, hide_index=True)


# --- TAB: Ventas ---
with tab_ventas:
    st.markdown(section_header("Registro de ventas", "Historial de unidades vendidas con su ultimo precio de lista"), unsafe_allow_html=True)

    df_ventas_tab = cargar_ventas()

    if df_ventas_tab.empty:
        st.markdown(empty_state(
            "💰", "Sin ventas registradas",
            "Se detectan automaticamente cuando /analista-inventario encuentra cambios de estado a 'vendido' entre revisiones."
        ), unsafe_allow_html=True)
    else:
        proyectos_venta = ["Todos"] + sorted(df_ventas_tab["proyecto"].unique().tolist())
        filtro_proy_venta = st.selectbox("Filtrar por proyecto", proyectos_venta, key="ventas_proy")

        df_v = df_ventas_tab.copy()
        if filtro_proy_venta != "Todos":
            df_v = df_v[df_v["proyecto"] == filtro_proy_venta]

        # KPI cards
        precios_venta = df_v["precio_ultimo_lista"].dropna()
        cols_v = st.columns(3)
        cols_v[0].markdown(kpi_card("Total ventas", len(df_v), "🏷️", RED), unsafe_allow_html=True)
        if not precios_venta.empty:
            cols_v[1].markdown(kpi_card("Precio promedio", f"${precios_venta.mean():,.0f}", "💵", NAVY), unsafe_allow_html=True)
        else:
            cols_v[1].markdown(kpi_card("Precio promedio", "Sin datos", "💵", NAVY), unsafe_allow_html=True)
        if not df_v.empty:
            cols_v[2].markdown(kpi_card("Ultima venta", df_v["fecha_venta"].max(), "📅", TEAL), unsafe_allow_html=True)
        else:
            cols_v[2].markdown(kpi_card("Ultima venta", "N/A", "📅", TEAL), unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        df_v_display = df_v.sort_values("fecha_venta", ascending=False)
        st.dataframe(
            df_v_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "proyecto": st.column_config.TextColumn("Proyecto"),
                "unidad": st.column_config.TextColumn("Unidad"),
                "tipologia": st.column_config.TextColumn("Tipologia"),
                "precio_ultimo_lista": st.column_config.NumberColumn("Ultimo precio lista", format="$%,.0f"),
                "fecha_venta": st.column_config.TextColumn("Fecha venta"),
            },
        )

        if len(df_v["proyecto"].unique()) > 1:
            st.markdown(section_header("Resumen por proyecto"), unsafe_allow_html=True)
            resumen_ventas = df_v.groupby("proyecto").agg(
                total_ventas=("unidad", "count"),
                precio_promedio=("precio_ultimo_lista", "mean"),
                ultima_venta=("fecha_venta", "max"),
            ).reset_index()
            resumen_ventas.columns = ["Proyecto", "Total ventas", "Precio promedio", "Ultima venta"]
            st.dataframe(resumen_ventas, use_container_width=True, hide_index=True)

        csv_ventas = df_v.to_csv(index=False)
        st.download_button(
            "Descargar CSV de ventas",
            csv_ventas,
            file_name=f"ventas_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            key="dl_ventas",
        )


# --- TAB: Precios ---
with tab_precios:
    st.markdown(section_header("Incrementos de precios", "Cambios de precio detectados entre revisiones"), unsafe_allow_html=True)

    df_inc_tab = cargar_incrementos()

    if df_inc_tab.empty:
        st.markdown(empty_state(
            "💲", "Sin incrementos registrados",
            "Se detectan automaticamente cuando /analista-inventario encuentra cambios de precio entre revisiones."
        ), unsafe_allow_html=True)
    else:
        proyectos_inc = ["Todos"] + sorted(df_inc_tab["proyecto"].unique().tolist())
        filtro_proy_inc = st.selectbox("Filtrar por proyecto", proyectos_inc, key="inc_proy")

        df_i = df_inc_tab.copy()
        if filtro_proy_inc != "Todos":
            df_i = df_i[df_i["proyecto"] == filtro_proy_inc]

        cols_i = st.columns(3)
        cols_i[0].markdown(kpi_card("Total cambios", len(df_i), "📊", NAVY), unsafe_allow_html=True)
        if not df_i.empty:
            inc_promedio = df_i["incremento_pct"].mean()
            cols_i[1].markdown(kpi_card("Incremento promedio", f"{inc_promedio:+.2f}%", "📈", GREEN if inc_promedio > 0 else RED), unsafe_allow_html=True)
            cols_i[2].markdown(kpi_card("Rango", f"{df_i['incremento_pct'].min():+.1f}% a {df_i['incremento_pct'].max():+.1f}%", "↕️", TEAL), unsafe_allow_html=True)
        else:
            cols_i[1].markdown(kpi_card("Incremento promedio", "N/A", "📈", NAVY), unsafe_allow_html=True)
            cols_i[2].markdown(kpi_card("Rango", "N/A", "↕️", NAVY), unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        df_i_display = df_i.sort_values("fecha_registro", ascending=False)
        st.dataframe(
            df_i_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "proyecto": st.column_config.TextColumn("Proyecto"),
                "unidad": st.column_config.TextColumn("Unidad"),
                "tipologia": st.column_config.TextColumn("Tipologia"),
                "precio_anterior": st.column_config.NumberColumn("Precio anterior", format="$%,.0f"),
                "precio_nuevo": st.column_config.NumberColumn("Precio nuevo", format="$%,.0f"),
                "incremento_pct": st.column_config.NumberColumn("Incremento %", format="%.2f%%"),
                "fecha_registro": st.column_config.TextColumn("Fecha registro"),
            },
        )

        if len(df_i["proyecto"].unique()) > 1:
            st.markdown(section_header("Incremento promedio por proyecto"), unsafe_allow_html=True)
            resumen_inc = df_i.groupby("proyecto").agg(
                total_cambios=("unidad", "count"),
                incremento_promedio=("incremento_pct", "mean"),
            ).reset_index()
            resumen_inc.columns = ["Proyecto", "Total cambios", "Incremento promedio %"]
            st.dataframe(resumen_inc, use_container_width=True, hide_index=True)

        csv_inc = df_i.to_csv(index=False)
        st.download_button(
            "Descargar CSV de incrementos",
            csv_inc,
            file_name=f"incrementos_precios_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            key="dl_inc",
        )


# --- TAB: Movimientos ---
with tab_movimientos:
    st.markdown(section_header("Historial de movimientos", "Cambios de estado detectados entre revisiones"), unsafe_allow_html=True)

    df_mov = cargar_movimientos()
    df_inv_mov = cargar_inventario()

    if df_mov.empty and len(df_inv_mov["fecha_revision"].unique()) <= 1 if not df_inv_mov.empty else True:
        st.markdown(empty_state(
            "🔄", "Sin movimientos registrados",
            "Los movimientos se detectan automaticamente cuando hay cambios de estado entre revisiones semanales."
        ), unsafe_allow_html=True)
    else:
        if not df_mov.empty:
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

            cols_m = st.columns(3)
            cols_m[0].markdown(kpi_card("Total movimientos", len(df_mov_f), "🔄", NAVY), unsafe_allow_html=True)
            cols_m[1].markdown(kpi_card("Nuevas ventas", len(df_mov_f[df_mov_f["estado_nuevo"] == "vendido"]), "🔴", RED), unsafe_allow_html=True)
            cols_m[2].markdown(kpi_card("Nuevos apartados", len(df_mov_f[df_mov_f["estado_nuevo"] == "apartado"]), "🟡", ORANGE), unsafe_allow_html=True)

            st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

            df_mov_display = df_mov_f.sort_values("fecha_cambio", ascending=False)
            st.dataframe(df_mov_display, use_container_width=True, hide_index=True)

            if len(df_mov_f) > 0:
                st.markdown(section_header("Velocidad de ventas por semana"), unsafe_allow_html=True)

                df_ventas_mov = df_mov_f[df_mov_f["estado_nuevo"] == "vendido"].copy()
                if not df_ventas_mov.empty:
                    df_ventas_mov["semana"] = pd.to_datetime(df_ventas_mov["fecha_cambio"]).dt.isocalendar().week.astype(str) + "-" + pd.to_datetime(df_ventas_mov["fecha_cambio"]).dt.isocalendar().year.astype(str)
                    ventas_semana = df_ventas_mov.groupby(["fecha_cambio", "proyecto"]).size().reset_index(name="ventas")
                    ventas_pivot = ventas_semana.pivot(index="fecha_cambio", columns="proyecto", values="ventas").fillna(0).reset_index()
                    st.dataframe(ventas_pivot, use_container_width=True, hide_index=True)
                else:
                    st.markdown(empty_state("📉", "Sin ventas", "No hay ventas registradas en los movimientos filtrados."), unsafe_allow_html=True)

    if not df_inv_mov.empty:
        fechas = sorted(df_inv_mov["fecha_revision"].unique())
        if len(fechas) > 1:
            st.markdown(section_header("Absorcion historica por proyecto"), unsafe_allow_html=True)

            historial = []
            for fecha in fechas:
                df_f = df_inv_mov[df_inv_mov["fecha_revision"] == fecha]
                for proy in df_f["proyecto"].unique():
                    df_pf = df_f[df_f["proyecto"] == proy]
                    total = len(df_pf)
                    vendidas_h = len(df_pf[df_pf["estado"] == "vendido"])
                    disponibles_h = len(df_pf[df_pf["estado"] == "disponible"])
                    apartadas_h = len(df_pf[df_pf["estado"] == "apartado"])
                    historial.append({
                        "fecha": fecha,
                        "proyecto": proy,
                        "vendidas": vendidas_h,
                        "disponibles": disponibles_h,
                        "apartadas": apartadas_h,
                    })

            df_hist = pd.DataFrame(historial)

            st.markdown(f"<p style='font-size:13px;font-weight:500;color:{NAVY};margin-bottom:8px;'>Unidades disponibles por revision</p>", unsafe_allow_html=True)
            df_disp_pivot = df_hist.pivot(index="fecha", columns="proyecto", values="disponibles").reset_index()
            st.dataframe(df_disp_pivot, use_container_width=True, hide_index=True)

            st.markdown(f"<p style='font-size:13px;font-weight:500;color:{NAVY};margin:16px 0 8px 0;'>Unidades vendidas por revision</p>", unsafe_allow_html=True)
            df_vend_pivot = df_hist.pivot(index="fecha", columns="proyecto", values="vendidas").reset_index()
            st.dataframe(df_vend_pivot, use_container_width=True, hide_index=True)


# --- TAB: Proyectos ---
with tab_proyectos:
    st.markdown(section_header("Proyectos registrados", "Administra los proyectos inmobiliarios que monitoreas"), unsafe_allow_html=True)
    df_proyectos = cargar_proyectos()

    for col in ["inicio_ventas", "notas"]:
        if col in df_proyectos.columns:
            df_proyectos[col] = df_proyectos[col].fillna("").astype(str)

    if df_proyectos.empty:
        st.markdown(empty_state(
            "📋", "Sin proyectos registrados",
            "Ve a la pestana 'Agregar' para registrar tu primer proyecto."
        ), unsafe_allow_html=True)
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
                "total_unidades": st.column_config.NumberColumn("Total unidades", width="small", help="Total real de unidades del proyecto."),
                "inicio_ventas": st.column_config.TextColumn("Inicio ventas", width="small", help="Formato YYYY-MM. Ej: 2025-06"),
            },
            key="editor_proyectos",
        )

        if st.button("Guardar cambios", key="guardar_proyectos"):
            guardar_proyectos(editado)
            st.success("Proyectos guardados correctamente.")

        st.markdown(section_header("Eliminar proyecto"), unsafe_allow_html=True)
        proyecto_eliminar = st.selectbox(
            "Selecciona el proyecto a eliminar",
            df_proyectos["nombre_proyecto"].tolist(),
            key="select_eliminar",
        )
        if st.button("Eliminar proyecto", type="secondary"):
            df_filtrado = df_proyectos[df_proyectos["nombre_proyecto"] != proyecto_eliminar]
            guardar_proyectos(df_filtrado)
            st.success(f"'{proyecto_eliminar}' eliminado.")
            st.rerun()

# --- TAB: Agregar ---
with tab_agregar:
    st.markdown(section_header("Agregar nuevo proyecto", "Registra un proyecto inmobiliario para monitorear su inventario"), unsafe_allow_html=True)

    with st.form("form_agregar", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            nombre = st.text_input("Nombre del proyecto *")
            desarrolladora = st.text_input("Desarrolladora")
            ciudad = st.text_input("Ciudad")
        with col2:
            url_drive = st.text_input("URL carpeta Google Drive *")
            notas = st.text_area("Notas", placeholder="Ej: PDF se llama 'Lista de Precios Marzo'")

        submitted = st.form_submit_button("Agregar proyecto", type="primary")

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
    st.markdown(section_header("Inventario completo", "Vista detallada de todas las unidades registradas"), unsafe_allow_html=True)
    df_inventario_tab = cargar_inventario()

    if df_inventario_tab.empty:
        st.markdown(empty_state(
            "📊", "Sin datos de inventario",
            "Ejecuta /analista-inventario en Claude Code para analizar tus proyectos."
        ), unsafe_allow_html=True)
    else:
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

        ultima_fecha_inv = df_filtrado["fecha_revision"].max() if not df_filtrado.empty else "N/A"
        df_ultima = df_filtrado[df_filtrado["fecha_revision"] == ultima_fecha_inv] if not df_filtrado.empty else df_filtrado

        cols_inv = st.columns(4)
        cols_inv[0].markdown(kpi_card("Total unidades", len(df_ultima), "🏠", NAVY), unsafe_allow_html=True)
        disp_inv = len(df_ultima[df_ultima["estado"] == "disponible"])
        cols_inv[1].markdown(kpi_card("Disponibles", disp_inv, "🟢", GREEN), unsafe_allow_html=True)
        apart_inv = len(df_ultima[df_ultima["estado"] == "apartado"])
        cols_inv[2].markdown(kpi_card("Apartadas", apart_inv, "🟡", ORANGE), unsafe_allow_html=True)
        if not df_ultima.empty and "precio_lista_mxn" in df_ultima.columns:
            precios_inv = df_ultima["precio_lista_mxn"].dropna()
            if not precios_inv.empty:
                cols_inv[3].markdown(kpi_card("Precio promedio", f"${precios_inv.mean():,.0f}", "💵", TEAL), unsafe_allow_html=True)
            else:
                cols_inv[3].markdown(kpi_card("Precio promedio", "N/A", "💵", TEAL), unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        st.dataframe(df_filtrado, use_container_width=True, hide_index=True)

        csv_export = df_filtrado.to_csv(index=False)
        st.download_button(
            "Descargar CSV filtrado",
            csv_export,
            file_name=f"inventario_filtrado_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

# --- TAB: Config ---
with tab_config:
    st.markdown(section_header("Configuracion", "Ajustes de revision automatica"), unsafe_allow_html=True)
    config = cargar_config()

    frecuencia = st.number_input(
        "Frecuencia de revision (dias)",
        min_value=1,
        max_value=365,
        value=config.get("frecuencia_dias", 7),
        help="Cada cuantos dias se ejecuta la revision automatica.",
    )

    if st.button("Guardar configuracion"):
        config["frecuencia_dias"] = frecuencia
        guardar_config(config)
        st.success(f"Configuracion guardada. Frecuencia: cada {frecuencia} dias.")

    st.markdown(section_header("Comando para revision automatica"), unsafe_allow_html=True)
    st.code(f"/loop {config.get('frecuencia_dias', 7)}d /analista-inventario", language="bash")
    st.markdown(f"<p style='font-size:12px;color:#64748b;'>Copia y pega este comando en Claude Code para activar la revision periodica.</p>", unsafe_allow_html=True)
