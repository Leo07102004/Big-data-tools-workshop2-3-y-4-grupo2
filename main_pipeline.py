"""
Big Data Tools - Workshop 2, 3 y 4 - Grupo 2
M.Sc. in Applied Analytics (coterminal course)
Fac. de Ingeniería -  Universidad de la Sabana
Prof.: Hugo Franco, Ph.D.
Students: Nicolás Almonacid (0000293190), Leonardo Montoya (0000286207) y Juan Portocarrero (0000294592)
Pipeline Maestro
Orquesta las Partes I, II y III como un único flow de Prefect.
Ejecutar: python main_pipeline.py
"""

import sys
from pathlib import Path

# Añadir el directorio actual al path para importar los módulos
sys.path.insert(0, str(Path(__file__).parent))

from prefect import flow, get_run_logger

from parte1_pipeline import (
    download_data,
    load_categories,
    unify_and_top100,
    standardize_dates,
    compute_days_to_trending,
    save_processed,
)
from parte2_dashboard import load_data as load_data_p2, build_dashboard
from parte3_mongodb_geo_corr import (
    load_to_mongodb,
    generate_geo_maps,
    compute_correlation,
    build_full_dashboard,
)


# ─────────────────────────────────────────────
#  FLOW MAESTRO
# ─────────────────────────────────────────────

@flow(
    name="YouTube Trending - Pipeline Completo (Workshop 3)",
    description=(
        "Pipeline end-to-end con Prefect:\n"
        "  Parte I  - Ingesta, unificación, fechas, días_a_trending\n"
        "  Parte II - Dashboard interactivo Plotly\n"
        "  Parte III - MongoDB, mapas georreferenciados, correlación"
    ),
)
def full_pipeline():
    logger = get_run_logger()
    logger.info("\n" + "="*60)
    logger.info("  INICIANDO PIPELINE COMPLETO - WORKSHOP 3")
    logger.info("  Universidad de La Sabana · Big Data Tools")
    logger.info("="*60)

    # ── PARTE I: ETL ─────────────────────────────
    logger.info("\n[PARTE I]  Descarga, unificación y preparación de datos…")
    staging_dir  = download_data()
    categories   = load_categories(staging_dir)
    top100_df    = unify_and_top100(staging_dir, categories)
    dated_df     = standardize_dates(top100_df)
    final_df     = compute_days_to_trending(dated_df)
    output_path  = save_processed(final_df)
    logger.info(f"  (Check)  Parte I completada - {output_path}")

    # ── PARTE II: Dashboard Plotly ────────────────
    logger.info("\n[PARTE II]  Generando dashboard interactivo…")
    df_p2 = load_data_p2(output_path)
    dashboard_p2 = build_dashboard(df_p2, output_html="dashboard_youtube.html")
    logger.info(f"  (Check)  Parte II completada - {dashboard_p2}")

    # ── PARTE III: MongoDB + Geo + Correlación ────
    logger.info("\n[PARTE III]  MongoDB, mapas y correlación…")
    mongo_status = load_to_mongodb(df_p2)
    geo_figs     = generate_geo_maps(df_p2)
    corr_results = compute_correlation(df_p2)

    dashboard_full = build_full_dashboard(
        df_p2, geo_figs, corr_results,
        output_html="dashboard_completo.html",
    )
    logger.info(f"  (Check)  Parte III completada - {dashboard_full}")

    # ── RESUMEN FINAL ─────────────────────────────
    logger.info("\n" + "="*60)
    logger.info("  PIPELINE COMPLETADO EXITOSAMENTE")
    logger.info("="*60)
    logger.info(f"  Dataset procesado : {output_path}")
    logger.info(f"  Dashboard Parte II: {dashboard_p2}")
    logger.info(f"  Dashboard completo: {dashboard_full}")
    logger.info(f"  MongoDB status    : {mongo_status}")
    if corr_results:
        logger.info(f"  Spearman ρ        : {corr_results.get('spearman_r', 'N/A')}")
        logger.info(f"  Interpretación    : {corr_results.get('interpretation', '')}")
    logger.info("="*60)

    return {
        "dataset":          str(output_path),
        "dashboard_p2":     str(dashboard_p2),
        "dashboard_full":   str(dashboard_full),
        "mongo_status":     mongo_status,
        "corr_results":     corr_results,
    }


# ─────────────────────────────────────────────
#  PUNTO DE ENTRADA
# ─────────────────────────────────────────────

if __name__ == "__main__":
    result = full_pipeline()
    print("\n" + "="*60)
    print("(Check)  WORKSHOP 3 COMPLETADO")
    print("="*60)
    print(f"Dataset       : {result['dataset']}")
    print(f"Dashboard II  : {result['dashboard_p2']}")
    print(f"Dashboard III : {result['dashboard_full']}")
    print(f"MongoDB       : {result['mongo_status']}")
    if result.get("corr_results"):
        r = result["corr_results"]
        print(f"\n Correlación Spearman ρ = {r.get('spearman_r')}")
        print(f"      {r.get('interpretation','')}")
    print("="*60)
