"""
Big Data Tools - Workshop 2, 3 y 4 - Grupo 2
M.Sc. in Applied Analytics (coterminal course)
Fac. de Ingeniería -  Universidad de la Sabana
Prof.: Hugo Franco, Ph.D.
Students: Nicolás Almonacid (0000293190), Leonardo Montoya (0000286207) y Juan Portocarrero (0000294592)
Parte III: MongoDB, Visualización Georreferenciada y Correlación
"""

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from prefect import flow, task, get_run_logger
from scipy import stats

# ─────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────

PROCESSED_PATH = Path("data/processed/youtube_top100_processed.csv")

# Coordenadas y nombres de países
COUNTRY_META = {
    "US": {"lat": 37.09, "lon": -95.71, "name": "United States"},
    "GB": {"lat": 55.38, "lon": -3.44,  "name": "United Kingdom"},
    "CA": {"lat": 56.13, "lon": -106.35,"name": "Canada"},
    "DE": {"lat": 51.17, "lon": 10.45,  "name": "Germany"},
    "FR": {"lat": 46.23, "lon": 2.21,   "name": "France"},
    "IN": {"lat": 20.59, "lon": 78.96,  "name": "India"},
    "JP": {"lat": 36.20, "lon": 138.25, "name": "Japan"},
    "KR": {"lat": 35.91, "lon": 127.77, "name": "South Korea"},
    "MX": {"lat": 23.63, "lon": -102.55,"name": "Mexico"},
    "RU": {"lat": 61.52, "lon": 105.32, "name": "Russia"},
}


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def _generate_demo_data() -> pd.DataFrame:
    """Dataset de demostración si no existe el procesado."""
    rng = np.random.default_rng(42)
    countries = list(COUNTRY_META.keys())
    categories = ["Entertainment", "Music", "News & Politics", "Sports",
                  "Science & Technology", "Comedy", "Film & Animation",
                  "Gaming", "Howto & Style", "Education"]
    n = 500
    pub_dates = pd.date_range("2017-11-01", "2018-06-30", periods=n)
    trend_dates = pub_dates + pd.to_timedelta(rng.integers(0, 30, n), unit="D")
    return pd.DataFrame({
        "video_id":              [f"vid_{i:04d}" for i in range(n)],
        "title":                 [f"Trending Video #{i}" for i in range(n)],
        "country":               rng.choice(countries, n),
        "category_name":         rng.choice(categories, n),
        "views":                 rng.integers(1_000_000, 50_000_000, n),
        "likes":                 rng.integers(10_000, 2_000_000, n),
        "dislikes":              rng.integers(1_000, 500_000, n),
        "publish_date_apply":    pub_dates.date,
        "trending_date_parsed":  trend_dates.date,
        "days_to_trending":      rng.integers(0, 30, n).astype(float),
    })


def load_processed(path: Path = PROCESSED_PATH) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    print("Usando datos de demostración.")
    return _generate_demo_data()


# ─────────────────────────────────────────────
#  TAREA A — CARGA EN MONGODB
# ─────────────────────────────────────────────

@task(name="Cargar datos en MongoDB", retries=2, retry_delay_seconds=10)
def load_to_mongodb(df: pd.DataFrame) -> str:
    """
    Inserta el dataset procesado en una colección de MongoDB Atlas.
    Requiere la variable de entorno MONGO_URI o el archivo .env con la cadena de conexión.
    Estrategia upsert por video_id + country para evitar duplicados.
    """
    logger = get_run_logger()

    # Intentar cargar URI desde .env
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    mongo_uri = os.environ.get("MONGO_URI", "")
    if not mongo_uri:
        logger.warning(
            "     MONGO_URI no definida.\n"
            "     Crea un archivo .env con:\n"
            "       MONGO_URI=mongodb+srv://<user>:<password>@cluster0.xxxxx.mongodb.net/\n"
            "     O exporta la variable de entorno antes de ejecutar.\n"
            "     Se omite la carga a MongoDB."
        )
        return "skipped"

    try:
        from pymongo import MongoClient, UpdateOne
        from pymongo.errors import BulkWriteError

        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
        # Verificar conexión
        client.admin.command("ping")
        logger.info("  (Check)  Conexión a MongoDB exitosa.")

        db = client["youtube_trending"]
        collection = db["top100_videos"]

        # Convertir fechas a string para compatibilidad JSON/BSON
        df_mongo = df.copy()
        for col in df_mongo.select_dtypes(include=["datetime64", "object"]).columns:
            df_mongo[col] = df_mongo[col].astype(str)
        df_mongo = df_mongo.where(pd.notnull(df_mongo), None)

        records = df_mongo.to_dict("records")

        # Upsert para idempotencia (clave compuesta: video_id + country)
        operations = [
            UpdateOne(
                {"video_id": rec["video_id"], "country": rec["country"]},
                {"$set": rec},
                upsert=True,
            )
            for rec in records
        ]

        result = collection.bulk_write(operations, ordered=False)
        logger.info(
            f"  (Check)  MongoDB: {result.upserted_count} insertados, "
            f"{result.modified_count} actualizados."
        )
        client.close()
        return f"mongodb: {len(records)} documentos procesados"

    except ImportError:
        logger.warning("pymongo no instalado. Ejecuta: pip install 'pymongo[srv]'")
        return "pymongo_not_installed"
    except Exception as exc:
        logger.error(f"Error MongoDB: {exc}")
        return f"error: {exc}"


# ─────────────────────────────────────────────
#  TAREA B — VISUALIZACIÓN GEORREFERENCIADA
# ─────────────────────────────────────────────

@task(name="Generar mapas georreferenciados")
def generate_geo_maps(df: pd.DataFrame) -> dict:
    """
    Genera dos mapas interactivos con Plotly:
      b1) Mapa de burbujas: vídeo más visto por país
      b2) Mapa de coropletas: categoría más vista por país
    Devuelve dict con las dos figuras.
    """
    logger = get_run_logger()

    # ── Preparar datos por país ──────────────────────
    country_stats = []
    for country, meta in COUNTRY_META.items():
        subset = df[df["country"] == country]
        if subset.empty:
            continue

        # Vídeo más visto en este país
        top_video = subset.nlargest(1, "views").iloc[0]

        # Categoría con más vistas en este país
        top_cat = (
            subset.groupby("category_name")["views"]
                  .sum()
                  .idxmax()
        )
        top_cat_views = subset.groupby("category_name")["views"].sum().max()

        country_stats.append({
            "country":         country,
            "country_name":    meta["name"],
            "lat":             meta["lat"],
            "lon":             meta["lon"],
            "top_title":       str(top_video.get("title", "N/A"))[:60],
            "top_views":       int(top_video.get("views", 0)),
            "top_views_M":     round(int(top_video.get("views", 0)) / 1e6, 2),
            "top_category":    top_cat,
            "top_cat_views_M": round(top_cat_views / 1e6, 2),
        })

    geo_df = pd.DataFrame(country_stats)

    if geo_df.empty:
        logger.warning("No hay datos georreferenciados suficientes.")
        return {}

    # ── Mapa 1: Vídeo más visto por país (bubble map) ──
    fig1 = px.scatter_geo(
        geo_df,
        lat="lat",
        lon="lon",
        size="top_views_M",
        color="top_views_M",
        hover_name="country_name",
        hover_data={
            "top_title": True,
            "top_views_M": ":.1f",
            "lat": False,
            "lon": False,
        },
        color_continuous_scale="Reds",
        size_max=50,
        projection="natural earth",
        title="Vídeo Más Visto por País (Millones de Vistas)",
        labels={"top_views_M": "Vistas (M)"},
        height=520,
    )
    fig1.update_geos(
        showcoastlines=True, coastlinecolor="lightgray",
        showland=True, landcolor="#1e293b",
        showocean=True, oceancolor="#0f172a",
        showframe=False,
    )
    fig1.update_layout(
        title_font_size=17,
        paper_bgcolor="#0f1117",
        plot_bgcolor="#0f1117",
        font_color="#e0e0e0",
        coloraxis_colorbar=dict(title="Vistas (M)", tickfont=dict(color="#e0e0e0")),
        margin=dict(l=0, r=0, t=60, b=0),
    )

    # ── Mapa 2: Categoría más vista por país (choropleth) ──
    # Asignar un índice numérico a cada categoría para la escala de color
    all_cats = sorted(geo_df["top_category"].unique())
    cat_idx = {cat: i for i, cat in enumerate(all_cats)}
    geo_df["cat_idx"] = geo_df["top_category"].map(cat_idx)

    fig2 = px.scatter_geo(
        geo_df,
        lat="lat",
        lon="lon",
        size="top_cat_views_M",
        color="top_category",
        hover_name="country_name",
        hover_data={
            "top_category": True,
            "top_cat_views_M": ":.1f",
            "lat": False,
            "lon": False,
        },
        color_discrete_sequence=px.colors.qualitative.Bold,
        size_max=45,
        projection="natural earth",
        title="Categoría Más Vista por País",
        labels={"top_category": "Categoría", "top_cat_views_M": "Vistas (M)"},
        height=520,
    )
    fig2.update_geos(
        showcoastlines=True, coastlinecolor="lightgray",
        showland=True, landcolor="#1e293b",
        showocean=True, oceancolor="#0f172a",
        showframe=False,
    )
    fig2.update_layout(
        title_font_size=17,
        paper_bgcolor="#0f1117",
        plot_bgcolor="#0f1117",
        font_color="#e0e0e0",
        legend=dict(bgcolor="#1a1d2e", bordercolor="#2d3748"),
        margin=dict(l=0, r=0, t=60, b=0),
    )

    logger.info("  (Check)  Mapas georreferenciados generados.")
    return {"map_top_video": fig1, "map_top_category": fig2, "geo_df": geo_df}


# ─────────────────────────────────────────────
#  TAREA C — CORRELACIÓN (días - vistas)
# ─────────────────────────────────────────────

@task(name="Calcular correlación días_a_trending vs. vistas")
def compute_correlation(df: pd.DataFrame) -> dict:
    """
    Calcula la correlación entre 'days_to_trending' y 'views'.

    Se elige el método de Spearman porque:
      • Las vistas y los días no siguen distribución normal (sesgo fuerte).
      • Spearman mide asociación monotónica sin suponer linealidad.
      • Es robusto a outliers, comunes en datos de YouTube.

    También se incluye Pearson para referencia y contraste.
    """
    logger = get_run_logger()

    col_days  = "days_to_trending"
    col_views = "views"

    if col_days not in df.columns or col_views not in df.columns:
        logger.warning(f"Columnas requeridas no encontradas: {col_days}, {col_views}")
        return {}

    # Filtrar filas válidas
    valid = df[[col_days, col_views]].dropna()
    x = valid[col_days].values
    y = valid[col_views].values

    # ── Spearman (método principal) ──
    sp_r, sp_p = stats.spearmanr(x, y)

    # ── Pearson (referencia) ──
    pe_r, pe_p = stats.pearsonr(x, y)

    # ── Kendall Tau (complementario) ──
    kt_tau, kt_p = stats.kendalltau(x, y)

    results = {
        "n_samples": len(valid),
        "spearman_r":    round(sp_r, 4),
        "spearman_p":    round(sp_p, 6),
        "pearson_r":     round(pe_r, 4),
        "pearson_p":     round(pe_p, 6),
        "kendall_tau":   round(kt_tau, 4),
        "kendall_p":     round(kt_p, 6),
    }

    # ── Interpretación de Spearman ──
    def interpret(r):
        r = abs(r)
        if r >= 0.7:  return "fuerte"
        if r >= 0.4:  return "moderada"
        if r >= 0.2:  return "débil"
        return "muy débil o nula"

    direction = "negativa" if sp_r < 0 else "positiva"
    strength  = interpret(sp_r)
    sig       = "estadísticamente significativa" if sp_p < 0.05 else "NO significativa (p > 0.05)"

    results["interpretation"] = (
        f"La correlación de Spearman es {direction} y {strength} "
        f"(ρ = {sp_r:.4f}, p = {sp_p:.6f}). "
        f"La asociación es {sig} (α = 0.05)."
    )

    logger.info("\n" + "="*60)
    logger.info("  RESULTADOS DE CORRELACIÓN")
    logger.info("  días_hasta_trending  - -  número de vistas")
    logger.info("="*60)
    logger.info(f"  Muestras válidas   : {results['n_samples']}")
    logger.info(f"  Spearman ρ         : {results['spearman_r']:+.4f}  (p = {results['spearman_p']:.6f})")
    logger.info(f"  Pearson  r         : {results['pearson_r']:+.4f}  (p = {results['pearson_p']:.6f})")
    logger.info(f"  Kendall  τ         : {results['kendall_tau']:+.4f}  (p = {results['kendall_p']:.6f})")
    logger.info(f"\n {results['interpretation']}")
    logger.info("="*60)

    return results


# ─────────────────────────────────────────────
#  FIGURA — Scatter con recta de regresión
# ─────────────────────────────────────────────

def fig_correlation_scatter(df: pd.DataFrame, corr_results: dict) -> go.Figure:
    """Scatter plot: días a trending vs. vistas, coloreado por categoría."""
    valid = df[["days_to_trending", "views", "category_name", "country"]].dropna(
        subset=["days_to_trending", "views"]
    )

    fig = px.scatter(
        valid,
        x="days_to_trending",
        y="views",
        color="category_name",
        opacity=0.6,
        trendline="ols",
        trendline_scope="overall",
        hover_data=["country"],
        title=(
            "Correlación: Días hasta Trending vs. Vistas<br>"
            f"<sup>Spearman ρ = {corr_results.get('spearman_r', 'N/A')}</sup>"
        ),
        labels={
            "days_to_trending": "Días desde publicación hasta tendencia",
            "views": "Vistas",
            "category_name": "Categoría",
        },
        color_discrete_sequence=px.colors.qualitative.Vivid,
        height=520,
    )
    fig.update_traces(marker=dict(size=6))
    fig.update_layout(
        title_font_size=16,
        paper_bgcolor="#0f1117",
        plot_bgcolor="#1a1d2e",
        font_color="#e0e0e0",
        legend=dict(bgcolor="#1a1d2e", bordercolor="#2d3748", font=dict(size=10)),
        xaxis=dict(gridcolor="#2d3748"),
        yaxis=dict(gridcolor="#2d3748", tickformat=".2s"),
        margin=dict(l=60, r=20, t=80, b=60),
    )
    return fig


# ─────────────────────────────────────────────
#  ENSAMBLADO FINAL HTML — PARTES II + III
# ─────────────────────────────────────────────

def build_full_dashboard(
    df: pd.DataFrame,
    geo_figs: dict,
    corr_results: dict,
    output_html: str = "dashboard_completo.html",
):
    """Genera el dashboard completo combinando visualizaciones II y III."""
    from parte2_dashboard import (
        fig_top_videos_by_country,
        fig_categories_by_country,
        fig_interaction_polarity,
        fig_interaction_heatmap,
        fig_trending_timeline,
    )

    print("Ensamblando dashboard completo…")

    # ── Figuras Parte II ──
    p2_figs = [
        fig_top_videos_by_country(df),
        fig_categories_by_country(df),
        fig_interaction_polarity(df),
        fig_interaction_heatmap(df),
        fig_trending_timeline(df),
    ]

    # ── Figuras Parte III ──
    p3_figs = []
    if "map_top_video" in geo_figs:
        p3_figs.append(geo_figs["map_top_video"])
    if "map_top_category" in geo_figs:
        p3_figs.append(geo_figs["map_top_category"])
    if corr_results:
        p3_figs.append(fig_correlation_scatter(df, corr_results))

    all_figs = p2_figs + p3_figs

    # ── Construir HTML ──
    divs = [
        fig.to_html(full_html=False, include_plotlyjs=("cdn" if i == 0 else False))
        for i, fig in enumerate(all_figs)
    ]

    # Resumen de correlación para la página
    corr_html = ""
    if corr_results:
        sig_badge = (
            '<span style="background:#38a169;color:#fff;padding:2px 8px;'
            'border-radius:8px;font-size:0.8rem;">Significativa</span>'
            if corr_results.get("spearman_p", 1) < 0.05 else
            '<span style="background:#e53e3e;color:#fff;padding:2px 8px;'
            'border-radius:8px;font-size:0.8rem;">No significativa</span>'
        )
        sp_r = corr_results.get("spearman_r", "N/A")
        pe_r = corr_results.get("pearson_r", "N/A")
        interp = corr_results.get("interpretation", "")
        corr_html = f"""
        <div class="corr-box">
          <h2>Análisis de Correlación: Días hasta Trending -- Vistas</h2>
          <div class="corr-grid">
            <div class="corr-stat"><div class="val">{sp_r}</div><div class="lbl">Spearman ρ<br>(método principal)</div></div>
            <div class="corr-stat"><div class="val">{pe_r}</div><div class="lbl">Pearson r<br>(referencia)</div></div>
            <div class="corr-stat"><div class="val">{corr_results.get("kendall_tau","N/A")}</div><div class="lbl">Kendall τ</div></div>
            <div class="corr-stat"><div class="val">{corr_results.get("n_samples","N/A")}</div><div class="lbl">Muestras válidas</div></div>
          </div>
          <p class="interp">{interp} {sig_badge}</p>
          <p class="method-note">
            <strong>¿Por qué se utiliza Spearman?</strong> Las vistas de YouTube presentan
            distribución fuertemente sesgada a la derecha y numerosos valores
            atípicos (outliers). El coeficiente de Spearman mide asociación
            monotónica sin asumir normalidad ni linealidad, siendo más adecuado
            que Pearson para este tipo de datos. (Retomando los contenidos de Sesión 4 — Descriptive Statistics)
          </p>
        </div>"""

    html_out = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>YouTube Trending — Dashboard Completo</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Segoe UI',sans-serif;background:#0f1117;color:#e0e0e0}}
    header{{background:linear-gradient(135deg,#1a1a2e,#16213e,#0f3460);padding:32px 48px;border-bottom:2px solid #e94560}}
    header h1{{font-size:2rem;color:#fff;margin-bottom:6px}}
    header p{{color:#a0aec0;font-size:0.9rem}}
    .badge{{background:#e94560;color:#fff;padding:3px 10px;border-radius:12px;font-size:.75rem;margin-left:10px;vertical-align:middle}}
    .section-title{{font-size:1.3rem;color:#63b3ed;padding:8px 0 16px;margin-top:8px;border-bottom:1px solid #2d3748}}
    main{{padding:32px 48px}}
    .chart-card{{background:#1a1d2e;border:1px solid #2d3748;border-radius:12px;padding:24px;margin-bottom:28px;box-shadow:0 4px 20px rgba(0,0,0,.4)}}
    .corr-box{{background:#1a1d2e;border:2px solid #4299e1;border-radius:12px;padding:28px;margin-bottom:28px}}
    .corr-box h2{{color:#63b3ed;font-size:1.2rem;margin-bottom:18px}}
    .corr-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:18px}}
    .corr-stat{{background:#0f1117;border-radius:8px;padding:14px;text-align:center;border:1px solid #2d3748}}
    .corr-stat .val{{font-size:1.8rem;font-weight:700;color:#68d391}}
    .corr-stat .lbl{{font-size:.75rem;color:#a0aec0;margin-top:4px}}
    .interp{{color:#e0e0e0;margin-bottom:12px;line-height:1.6}}
    .method-note{{background:#0f1117;padding:14px;border-radius:8px;border-left:3px solid #4299e1;font-size:.85rem;color:#a0aec0;line-height:1.6}}
    footer{{text-align:center;padding:20px;color:#4a5568;font-size:.8rem;border-top:1px solid #2d3748}}
  </style>
</head>
<body>
<header>
  <h1>YouTube Trending — Dashboard Completo
    <span class="badge">Big Data Tools</span>
  </h1>
  <p>Fac. de Ingeniería -  Universidad de la Sabana · Workshop 2, 3 y 4 - Grupo 2 · Partes II y III · datasnaek/youtube-new</p>
</header>
<main>
  <h2 class="section-title">Parte II — Análisis Exploratorio Interactivo</h2>
  {''.join(f'<div class="chart-card">{divs[i]}</div>' for i in range(len(p2_figs)))}

  <h2 class="section-title">Parte III — Mapas Georreferenciados y Correlación</h2>
  {corr_html}
  {''.join(f'<div class="chart-card">{divs[len(p2_figs)+i]}</div>' for i in range(len(p3_figs)))}
</main>
<footer>
  Generado por el pipeline Prefect · Big Data Tools - Workshop 2, 3 y 4 - Grupo 2 · Fac. de Ingeniería -  Universidad de la Sabana
</footer>
</body>
</html>"""

    out_path = Path("data/processed") / output_html
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_out, encoding="utf-8")
    print(f"  (Check)  Dashboard completo guardado en: {out_path}")
    return out_path


# ─────────────────────────────────────────────
#  FLOW PRINCIPAL — PARTE III
# ─────────────────────────────────────────────

@flow(name="YouTube Trending Pipeline - Parte III",
      description="MongoDB, mapas georreferenciados y correlación estadística.")
def youtube_pipeline_parte3():
    logger = get_run_logger()
    df = load_processed()

    mongo_status = load_to_mongodb(df)
    logger.info(f"  MongoDB: {mongo_status}")

    geo_figs     = generate_geo_maps(df)
    corr_results = compute_correlation(df)

    return {
        "mongo_status": mongo_status,
        "geo_figs":     geo_figs,
        "corr_results": corr_results,
        "df":           df,
    }


# ─────────────────────────────────────────────
#  PUNTO DE ENTRADA
# ─────────────────────────────────────────────

if __name__ == "__main__":
    results = youtube_pipeline_parte3()
    df = results["df"]

    out = build_full_dashboard(
        df,
        results["geo_figs"],
        results["corr_results"],
    )
    print(f"\n(Check)  Parte III completada. Dashboard: {out}")
    print(f"   MongoDB: {results['mongo_status']}")
    print(f"   Spearman ρ: {results['corr_results'].get('spearman_r', 'N/A')}")
