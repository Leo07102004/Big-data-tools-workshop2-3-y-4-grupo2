"""
Big Data Tools - Workshop 2, 3 y 4 - Grupo 2
M.Sc. in Applied Analytics (coterminal course)
Fac. de Ingeniería -  Universidad de la Sabana
Prof.: Hugo Franco, Ph.D.
Students: Nicolás Almonacid (0000293190), Leonardo Montoya (0000286207) y Juan Portocarrero (0000294592)
Parte I: Pipeline de Datos con Prefect
Dataset: datasnaek/youtube-new (Kaggle)
"""

import os
import json
import glob
import logging
import functools
import time
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from prefect import flow, task, get_run_logger

# ─────────────────────────────────────────────
#  CUSTOM DECORATORS
# ─────────────────────────────────────────────

def timing_decorator(func):
    """Decorator que mide y registra el tiempo de ejecución de cada tarea."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"Tarea '{func.__name__}' completada en {elapsed:.2f} seg.")
        return result
    return wrapper


def validate_dataframe(func):
    """Decorator que valida que el resultado sea un DataFrame no vacío."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, pd.DataFrame):
            assert not result.empty, f"'{func.__name__}' devolvió un DataFrame vacío."
            print(f"  (Check)  DataFrame válido: {result.shape[0]} filas x {result.shape[1]} cols.")
        return result
    return wrapper


# ─────────────────────────────────────────────
#  CONFIGURACIÓN GLOBAL
# ─────────────────────────────────────────────

# Mapeo de archivos CSV - código de país ISO-2
COUNTRY_MAP = {
    "CAvideos.csv":  "CA",
    "DEvideos.csv":  "DE",
    "FRvideos.csv":  "FR",
    "GBvideos.csv":  "GB",
    "INvideos.csv":  "IN",
    "JPvideos.csv":  "JP",
    "KRvideos.csv":  "KR",
    "MXvideos.csv":  "MX",
    "RUvideos.csv":  "RU",
    "USvideos.csv":  "US",
}

# Mapeo de archivos JSON de categorías - código de país
CATEGORY_JSON_MAP = {
    "CA_category_id.json": "CA",
    "DE_category_id.json": "DE",
    "FR_category_id.json": "FR",
    "GB_category_id.json": "GB",
    "IN_category_id.json": "IN",
    "JP_category_id.json": "JP",
    "KR_category_id.json": "KR",
    "MX_category_id.json": "MX",
    "RU_category_id.json": "RU",
    "US_category_id.json": "US",
}

STAGING_DIR = Path("data/staging")
PROCESSED_DIR = Path("data/processed")


# ─────────────────────────────────────────────
#  TAREA 1 — DESCARGA / STAGING
# ─────────────────────────────────────────────

@task(name="Descargar datos de Kaggle", retries=2, retry_delay_seconds=15)
@timing_decorator
def download_data() -> Path:
    """
    Descarga el dataset youtube-new desde Kaggle a la zona de staging local.
    Requiere que ~/.kaggle/kaggle.json (o KAGGLE_USERNAME + KAGGLE_KEY) esté configurado.
    Si los archivos ya existen en staging, omite la descarga.
    """
    logger = get_run_logger()
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    existing_csvs = list(STAGING_DIR.glob("*videos.csv"))
    if len(existing_csvs) >= 5:
        logger.info(f"Staging ya contiene {len(existing_csvs)} CSVs. Se omite la descarga.")
        return STAGING_DIR

    logger.info("Iniciando descarga desde Kaggle API…")
    try:
        import kaggle  # noqa: F401  (pip install kaggle)
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files(
            "datasnaek/youtube-new",
            path=str(STAGING_DIR),
            unzip=True,
            quiet=False,
        )
        logger.info(f"  (Check)  Datos descargados en: {STAGING_DIR}")
    except Exception as exc:
        logger.warning(
            f"    No se pudo usar la API de Kaggle ({exc}).\n"
            "     Asegúrate de tener ~/.kaggle/kaggle.json configurado.\n"
            "     Alternativamente, descarga manualmente y coloca los archivos en data/staging/."
        )
    return STAGING_DIR


# ─────────────────────────────────────────────
#  TAREA 2 — CARGAR CATEGORÍAS (JSON)
# ─────────────────────────────────────────────

@task(name="Cargar categorías desde JSON")
@timing_decorator
def load_categories(staging_dir: Path) -> dict:
    """
    Lee todos los archivos JSON de categorías del staging y devuelve
    un diccionario anidado: {country_code: {category_id: category_name}}.
    """
    logger = get_run_logger()
    categories: dict[str, dict] = {}

    for json_file, country in CATEGORY_JSON_MAP.items():
        fpath = staging_dir / json_file
        if not fpath.exists():
            logger.warning(f"No encontrado: {fpath}")
            continue
        with open(fpath, encoding="utf-8") as f:
            data = json.load(f)
        categories[country] = {
            str(item["id"]): item["snippet"]["title"]
            for item in data.get("items", [])
        }
        logger.info(f"  (Check)  {country}: {len(categories[country])} categorías cargadas.")

    return categories


# ─────────────────────────────────────────────
#  TAREA 3 — UNIFICACIÓN Y TOP-100 GLOBAL
# ─────────────────────────────────────────────

@task(name="Unificar CSVs y seleccionar Top-100 global")
@timing_decorator
@validate_dataframe
def unify_and_top100(staging_dir: Path, categories: dict) -> pd.DataFrame:
    """
    Lee cada CSV del staging, añade columna 'country', resuelve el nombre
    de categoría, concatena todo y devuelve los 100 vídeos con más vistas
    a escala global (agrupando por video_id y sumando views).
    """
    logger = get_run_logger()
    frames = []

    # Encodings comunes para manejar caracteres especiales
    ENCODINGS = ["utf-8", "latin-1", "cp1252"]

    for csv_file, country in COUNTRY_MAP.items():
        fpath = staging_dir / csv_file
        if not fpath.exists():
            logger.warning(f"CSV no encontrado: {fpath}")
            continue

        # Intenta múltiples encodings
        df = None
        for enc in ENCODINGS:
            try:
                df = pd.read_csv(fpath, encoding=enc)
                break
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue

        if df is None:
            logger.warning(f"No se pudo leer {csv_file}")
            continue

        df["country"] = country

        # Resolver nombre de categoría usando el JSON del país
        cat_map = categories.get(country, {})
        df["category_name"] = df["category_id"].astype(str).map(cat_map).fillna("Unknown")

        frames.append(df)
        logger.info(f"  (Check)  {country}: {len(df)} filas cargadas.")

    if not frames:
        raise RuntimeError(
            "No se encontraron CSVs en el staging. "
            "Descarga el dataset manualmente en data/staging/."
        )

    full_df = pd.concat(frames, ignore_index=True)
    logger.info(f"Dataset completo: {full_df.shape}")

    # Asegurar que views_count sea numérico
    full_df["views"] = pd.to_numeric(full_df.get("views", full_df.get("view_count", 0)), errors="coerce").fillna(0)

    # Top 100 global: sumar vistas por video_id (puede aparecer en varios países)
    top100_ids = (
        full_df.groupby("video_id")["views"]
        .sum()
        .nlargest(100)
        .index
    )
    top100_df = full_df[full_df["video_id"].isin(top100_ids)].copy()
    logger.info(f"Top-100 global: {top100_df.shape}")

    return top100_df


# ─────────────────────────────────────────────
#  TAREA 4 — ESTANDARIZACIÓN DE FECHAS
# ─────────────────────────────────────────────

@task(name="Estandarizar fechas de publicación")
@timing_decorator
@validate_dataframe
def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza el formato de fechas del campo 'publish_time' usando dos enfoques:
      a) apply + lambda  - publish_date_apply
      b) iterrows        - publish_date_iterrows
    Ambas columnas deben producir el mismo resultado (fecha sin hora, ISO-8601).
    """
    logger = get_run_logger()

    # ── Detectar la columna de fecha de publicación ──
    pub_col = None
    for candidate in ["publish_time", "publishedAt", "published_at", "publish_date"]:
        if candidate in df.columns:
            pub_col = candidate
            break

    if pub_col is None:
        logger.warning("No se encontró columna de fecha de publicación.")
        df["publish_date_apply"] = pd.NaT
        df["publish_date_iterrows"] = pd.NaT
        return df

    # ─── a) Con apply + lambda ───────────────────────
    def parse_date(val):
        """Parsea una cadena de fecha a objeto date (solo año-mes-día)."""
        if pd.isna(val):
            return pd.NaT
        val = str(val).strip()
        # Intenta múltiples formatos comunes en el dataset
        for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
            try:
                return datetime.strptime(val, fmt).date()
            except ValueError:
                continue
        # Fallback con pandas
        try:
            return pd.to_datetime(val).date()
        except Exception:
            return pd.NaT

    df["publish_date_apply"] = df[pub_col].apply(lambda x: parse_date(x))
    logger.info("  (Check)  Fechas estandarizadas con apply+lambda.")

    # ─── b) Con iterrows ────────────────────────────
    parsed_dates = []
    for idx, row in df.iterrows():
        parsed_dates.append(parse_date(row[pub_col]))
    df["publish_date_iterrows"] = parsed_dates
    logger.info("  (Check)  Fechas estandarizadas con iterrows.")

    # Verificar consistencia entre los dos métodos
    match = (df["publish_date_apply"] == df["publish_date_iterrows"]).sum()
    logger.info(f"Fechas coincidentes entre métodos: {match}/{len(df)}")

    return df


# ─────────────────────────────────────────────
#  TAREA 5 — DURACIÓN HASTA TENDENCIA
# ─────────────────────────────────────────────

@task(name="Calcular días hasta trending")
@timing_decorator
@validate_dataframe
def compute_days_to_trending(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula los días entre la fecha de publicación ('publish_date_apply')
    y la fecha en que el vídeo fue tendencia ('trending_date').
    Resultado en la columna 'days_to_trending'.
    """
    logger = get_run_logger()

    # ── Detectar columna de trending_date ──
    trend_col = None
    for candidate in ["trending_date", "trending_at", "trend_date"]:
        if candidate in df.columns:
            trend_col = candidate
            break

    if trend_col is None:
        logger.warning("No se encontró columna 'trending_date'. Se asigna NaN.")
        df["days_to_trending"] = np.nan
        return df

    def parse_trending(val):
        """El dataset de Kaggle usa formato '17.14.11' = YY.DD.MM - 2017-11-14."""
        if pd.isna(val):
            return pd.NaT
        val = str(val).strip()
        try:
            # Formato nativo del dataset: YY.DD.MM
            parts = val.split(".")
            if len(parts) == 3:
                year = 2000 + int(parts[0])
                day = int(parts[1])
                month = int(parts[2])
                return datetime(year, month, day).date()
        except (ValueError, IndexError):
            pass
        try:
            return pd.to_datetime(val).date()
        except Exception:
            return pd.NaT

    df["trending_date_parsed"] = df[trend_col].apply(parse_trending)

    # Calcular diferencia en días
    def days_diff(row):
        pub = row.get("publish_date_apply")
        trnd = row.get("trending_date_parsed")
        if pd.isna(pub) or pd.isna(trnd):
            return np.nan
        try:
            delta = (datetime.combine(trnd, datetime.min.time()) -
                     datetime.combine(pub, datetime.min.time()))
            return max(delta.days, 0)  # no negativo
        except Exception:
            return np.nan

    df["days_to_trending"] = df.apply(days_diff, axis=1)

    valid = df["days_to_trending"].notna().sum()
    logger.info(f"  (Check)  days_to_trending calculado para {valid}/{len(df)} filas.")
    logger.info(f"     Mediana: {df['days_to_trending'].median():.1f} días")
    logger.info(f"     Media  : {df['days_to_trending'].mean():.1f} días")

    return df


# ─────────────────────────────────────────────
#  TAREA 6 — GUARDAR DATASET PROCESADO
# ─────────────────────────────────────────────

@task(name="Guardar dataset procesado")
@timing_decorator
def save_processed(df: pd.DataFrame) -> Path:
    """Persiste el DataFrame final en CSV y Parquet para las siguientes etapas."""
    logger = get_run_logger()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = PROCESSED_DIR / "youtube_top100_processed.csv"
    parquet_path = PROCESSED_DIR / "youtube_top100_processed.parquet"

    df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info(f"CSV guardado en: {csv_path}")

    try:
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Parquet guardado en: {parquet_path}")
    except Exception as e:
        logger.warning(f"No se pudo guardar Parquet: {e}")

    # Resumen del dataset final
    logger.info("\n" + "="*55)
    logger.info("  RESUMEN DEL DATASET FINAL")
    logger.info("="*55)
    logger.info(f"  Filas     : {df.shape[0]}")
    logger.info(f"  Columnas  : {df.shape[1]}")
    logger.info(f"  Países    : {df['country'].nunique()}")
    logger.info(f"  Vídeos únicos: {df['video_id'].nunique()}")
    if "days_to_trending" in df.columns:
        logger.info(f"  Días hasta trending (mediana): {df['days_to_trending'].median():.1f}")
    logger.info("="*55)

    return csv_path


# ─────────────────────────────────────────────
#  FLOW PRINCIPAL — PARTE I
# ─────────────────────────────────────────────

@flow(name="YouTube Trending Pipeline - Parte I",
      description="Pipeline ETL completo con Prefect para el dataset YouTube Trending.")
def youtube_pipeline_parte1() -> Path:
    """
    Orquesta todas las tareas del pipeline de datos:
      1. Descarga datos desde Kaggle (staging)
      2. Carga categorías desde JSON
      3. Unifica CSVs y selecciona Top-100 global
      4. Estandariza fechas con apply+lambda e iterrows
      5. Calcula días hasta trending
      6. Guarda el dataset procesado
    """
    staging_dir   = download_data()
    categories    = load_categories(staging_dir)
    top100_df     = unify_and_top100(staging_dir, categories)
    dated_df      = standardize_dates(top100_df)
    final_df      = compute_days_to_trending(dated_df)
    output_path   = save_processed(final_df)
    return output_path


# ─────────────────────────────────────────────
#  PUNTO DE ENTRADA
# ─────────────────────────────────────────────

if __name__ == "__main__":
    result = youtube_pipeline_parte1()
    print(f"\n(Check)  Pipeline Parte I completado. Dataset guardado en: {result}")
