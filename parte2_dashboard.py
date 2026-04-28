"""
Big Data Tools - Workshop 2, 3 y 4 - Grupo 2
M.Sc. in Applied Analytics (coterminal course)
Fac. de Ingeniería -  Universidad de la Sabana
Prof.: Hugo Franco, Ph.D.
Students: Nicolás Almonacid (0000293190), Leonardo Montoya (0000286207) y Juan Portocarrero (0000294592)
Parte II: Dashboard Interactivo con Plotly
Visualizaciones de los datos de YouTube Trending
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ─────────────────────────────────────────────
#  CARGA DEL DATASET PROCESADO
# ─────────────────────────────────────────────

PROCESSED_PATH = Path("data/processed/youtube_top100_processed.csv")

# Coordenadas aproximadas de los países del dataset (para mapas)
COUNTRY_COORDS = {
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


def load_data(path: Path = PROCESSED_PATH) -> pd.DataFrame:
    """Carga el dataset procesado; crea datos simulados si no existe."""
    if path.exists():
        df = pd.read_csv(path)
        print(f"  (Check)  Dataset cargado: {df.shape}")
    else:
        print("Dataset no encontrado. Generando datos de demostración…")
        df = _generate_demo_data()
    return df


def _generate_demo_data() -> pd.DataFrame:
    """Genera un dataset de demostración con la misma estructura del dataset real."""
    rng = np.random.default_rng(42)
    countries = list(COUNTRY_COORDS.keys())
    categories = ["Entertainment", "Music", "News & Politics", "Sports",
                  "Science & Technology", "Comedy", "Film & Animation",
                  "Gaming", "Howto & Style", "Education"]

    n = 400
    pub_dates = pd.date_range("2017-11-01", "2018-06-30", periods=n)
    trend_dates = pub_dates + pd.to_timedelta(rng.integers(0, 30, n), unit="D")

    df = pd.DataFrame({
        "video_id":        [f"vid_{i:04d}" for i in range(n)],
        "title":           [f"Video Trending #{i}" for i in range(n)],
        "country":         rng.choice(countries, n),
        "category_name":   rng.choice(categories, n),
        "views":           rng.integers(1_000_000, 50_000_000, n),
        "likes":           rng.integers(10_000, 2_000_000, n),
        "dislikes":        rng.integers(1_000, 500_000, n),
        "comment_count":   rng.integers(500, 200_000, n),
        "publish_date_apply": pub_dates.date,
        "trending_date_parsed": trend_dates.date,
        "days_to_trending": rng.integers(0, 30, n).astype(float),
    })
    return df


def _get_interaction_polarity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula la polaridad de interacción:
      polarity = (likes - dislikes) / (likes + dislikes)
    Rango [-1, +1]. Positivo = más likes, Negativo = más dislikes.
    """
    df = df.copy()
    total = df["likes"] + df["dislikes"]
    df["polarity"] = (df["likes"] - df["dislikes"]) / total.replace(0, np.nan)
    df["interaction_score"] = df["views"] + df["likes"] * 2 - df["dislikes"]
    return df


# ─────────────────────────────────────────────
#  FIGURA 1 — Top vídeos por país (bar chart)
# ─────────────────────────────────────────────

def fig_top_videos_by_country(df: pd.DataFrame) -> go.Figure:
    """Bar chart: Top 5 vídeos más vistos por país."""
    top_per_country = (
        df.sort_values("views", ascending=False)
          .groupby("country")
          .head(5)
          .copy()
    )
    top_per_country["title_short"] = top_per_country["title"].str[:40] + "…"

    fig = px.bar(
        top_per_country,
        x="views",
        y="title_short",
        color="country",
        facet_col="country",
        facet_col_wrap=5,
        orientation="h",
        title="Top 5 Vídeos Más Vistos por País",
        labels={"views": "Vistas", "title_short": "Vídeo", "country": "País"},
        color_discrete_sequence=px.colors.qualitative.Vivid,
        height=700,
    )
    fig.update_layout(
        showlegend=False,
        title_font_size=18,
        margin=dict(l=20, r=20, t=80, b=20),
    )
    fig.update_xaxes(tickformat=".2s")
    return fig


# ─────────────────────────────────────────────
#  FIGURA 2 — Categorías por país (stacked bar)
# ─────────────────────────────────────────────

def fig_categories_by_country(df: pd.DataFrame) -> go.Figure:
    """
    Stacked bar: Proporción de categorías por país.
    Usa datos en formato largo (tidy) para Plotly Express.
    """
    cat_country = (
        df.groupby(["country", "category_name"])["views"]
          .sum()
          .reset_index()
    )
    cat_country["views_M"] = cat_country["views"] / 1e6

    fig = px.bar(
        cat_country,
        x="country",
        y="views_M",
        color="category_name",
        barmode="stack",
        title="Vistas Totales por País y Categoría (Millones)",
        labels={
            "country": "País",
            "views_M": "Vistas (Millones)",
            "category_name": "Categoría",
        },
        color_discrete_sequence=px.colors.qualitative.Set3,
        height=520,
    )
    fig.update_layout(
        title_font_size=18,
        legend_title_text="Categoría",
        xaxis_title="País",
        yaxis_title="Vistas (Millones)",
    )
    return fig


# ─────────────────────────────────────────────
#  FIGURA 3 — Polaridad de interacción
# ─────────────────────────────────────────────

def fig_interaction_polarity(df: pd.DataFrame) -> go.Figure:
    """
    Bubble chart: Vistas vs. Polaridad de interacción.
    Tamaño de burbuja = número de likes.
    Color = zona / país.
    Polaridad negativa - más dislikes; positiva - más likes.
    """
    df = _get_interaction_polarity(df)

    # Agregar por país y categoría
    agg = (
        df.groupby(["country", "category_name"])
          .agg(
              views=("views", "sum"),
              likes=("likes", "sum"),
              dislikes=("dislikes", "sum"),
              polarity=("polarity", "mean"),
              n_videos=("video_id", "count"),
          )
          .reset_index()
    )
    agg["polarity_label"] = agg["polarity"].apply(
        lambda p: "Positiva (más likes)" if p >= 0 else "Negativa (más dislikes)"
    )

    fig = px.scatter(
        agg,
        x="polarity",
        y="views",
        size="likes",
        color="country",
        symbol="polarity_label",
        hover_data=["category_name", "n_videos", "dislikes"],
        title="Polaridad de Interacción vs. Vistas por País y Categoría",
        labels={
            "polarity": "Polaridad  [-1 = dislikes | +1 = likes]",
            "views": "Vistas Totales",
            "country": "País",
            "polarity_label": "Balance",
        },
        color_discrete_sequence=px.colors.qualitative.Alphabet,
        height=550,
    )
    # Línea vertical en polaridad = 0
    fig.add_vline(x=0, line_dash="dash", line_color="gray",
                  annotation_text="Neutro", annotation_position="top right")
    fig.update_traces(marker=dict(opacity=0.75, line=dict(width=0.5, color="white")))
    fig.update_layout(title_font_size=18)
    return fig


# ─────────────────────────────────────────────
#  FIGURA 4 — Heatmap de interacción
# ─────────────────────────────────────────────

def fig_interaction_heatmap(df: pd.DataFrame) -> go.Figure:
    """
    Heatmap: Puntuación de interacción promedio (likes-dislikes+vistas/1M)
    por Categoría x País.
    """
    df = _get_interaction_polarity(df)
    pivot = (
        df.groupby(["category_name", "country"])["polarity"]
          .mean()
          .unstack(fill_value=0)
    )

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale="RdYlGn",
            zmid=0,
            colorbar=dict(title="Polaridad promedio"),
            hovertemplate="País: %{x}<br>Categoría: %{y}<br>Polaridad: %{z:.3f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="🗺  Heatmap: Polaridad de Interacción por Categoría y País",
        title_font_size=18,
        xaxis_title="País",
        yaxis_title="Categoría",
        height=480,
        margin=dict(l=160, r=40, t=80, b=60),
    )
    return fig


# ─────────────────────────────────────────────
#  FIGURA 5 — Línea temporal de tendencias
# ─────────────────────────────────────────────

def fig_trending_timeline(df: pd.DataFrame) -> go.Figure:
    """Área apilada: Evolución mensual del número de vídeos en tendencia por país."""
    df = df.copy()
    df["trending_date_parsed"] = pd.to_datetime(df.get("trending_date_parsed", pd.NaT), errors="coerce")
    df = df.dropna(subset=["trending_date_parsed"])
    df["month"] = df["trending_date_parsed"].dt.to_period("M").astype(str)

    monthly = (
        df.groupby(["month", "country"])["video_id"]
          .count()
          .reset_index(name="n_trending")
    )

    fig = px.area(
        monthly,
        x="month",
        y="n_trending",
        color="country",
        title="Evolución Mensual de Vídeos en Tendencia por País",
        labels={"month": "Mes", "n_trending": "N° Vídeos en Tendencia", "country": "País"},
        color_discrete_sequence=px.colors.qualitative.Pastel,
        height=460,
    )
    fig.update_layout(title_font_size=18, xaxis_tickangle=-45)
    return fig


# ─────────────────────────────────────────────
#  ENSAMBLADO DEL DASHBOARD HTML
# ─────────────────────────────────────────────

def build_dashboard(df: pd.DataFrame, output_html: str = "dashboard_youtube.html"):
    """Genera el dashboard completo como archivo HTML standalone."""
    print("Construyendo dashboard…")

    figs = {
        "top_videos":   fig_top_videos_by_country(df),
        "categories":   fig_categories_by_country(df),
        "polarity":     fig_interaction_polarity(df),
        "heatmap":      fig_interaction_heatmap(df),
        "timeline":     fig_trending_timeline(df),
    }

    # Convertir cada figura a HTML div
    divs = [
        fig.to_html(full_html=False, include_plotlyjs="cdn" if i == 0 else False)
        for i, fig in enumerate(figs.values())
    ]

    html_content = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>YouTube Trending Dashboard — Big Data Tools</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: 'Segoe UI', sans-serif;
      background: #0f1117;
      color: #e0e0e0;
    }}
    header {{
      background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
      padding: 32px 48px;
      border-bottom: 2px solid #e94560;
    }}
    header h1 {{ font-size: 2rem; color: #fff; margin-bottom: 6px; }}
    header p  {{ font-size: 0.95rem; color: #a0aec0; }}
    .badge {{
      display: inline-block;
      background: #e94560;
      color: white;
      padding: 3px 10px;
      border-radius: 12px;
      font-size: 0.75rem;
      margin-left: 10px;
      vertical-align: middle;
    }}
    main {{ padding: 32px 48px; }}
    .chart-card {{
      background: #1a1d2e;
      border: 1px solid #2d3748;
      border-radius: 12px;
      padding: 24px;
      margin-bottom: 28px;
      box-shadow: 0 4px 20px rgba(0,0,0,0.4);
    }}
    footer {{
      text-align: center;
      padding: 20px;
      color: #4a5568;
      font-size: 0.8rem;
      border-top: 1px solid #2d3748;
    }}
  </style>
</head>
<body>
<header>
  <h1>YouTube Trending Dashboard
    <span class="badge">Big Data Tools</span>
  </h1>
  <p>Fac. de Ingeniería -  Universidad de la Sabana · Workshop 2, 3 y 4 - Grupo 2 · Parte II · datasnaek/youtube-new</p>
</header>
  <main>
    {''.join(f'<div class="chart-card">{div}</div>' for div in divs)}
  </main>
  <footer>
    Generado por el pipeline Prefect · Big Data Tools - Workshop 2, 3 y 4 - Grupo 2 · Fac. de Ingeniería -  Universidad de la Sabana
  </footer>
</body>
</html>"""

    output_path = Path("data/processed") / output_html
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_content, encoding="utf-8")
    print(f"  (Check)  Dashboard guardado en: {output_path}")
    return output_path


# ─────────────────────────────────────────────
#  PUNTO DE ENTRADA
# ─────────────────────────────────────────────

if __name__ == "__main__":
    df = load_data()
    out = build_dashboard(df)
    print(f"\n(Check)  Parte II completada. Abre en navegador: {out}")
