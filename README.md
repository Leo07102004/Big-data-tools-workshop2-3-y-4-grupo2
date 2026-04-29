# Workshop 3 — YouTube Trending Pipeline
## Big Data Tools · Universidad de La Sabana

Pipeline de datos completo sobre el dataset `datasnaek/youtube-new` de Kaggle,
organizado en tres partes y orquestado con **Prefect**.

---

## Estructura del proyecto

```
youtube_pipeline/
├── main_pipeline.py           - Entry point: ejecuta TODO el pipeline
├── parte1_pipeline.py         - Parte I:   ETL con Prefect
├── parte2_dashboard.py        - Parte II:  Dashboard Plotly
├── parte3_mongodb_geo_corr.py - Parte III: MongoDB + Geo + Correlación
├── requirements.txt           - Dependencias
├── .env                       - (crear manualmente) con MONGO_URI
└── data/
    ├── staging/               - CSVs y JSONs descargados de Kaggle
    └── processed/             - Dataset procesado + dashboards HTML
```

---

## 1. Configuración del entorno

```bash
# Crear entorno conda (Python 3.10 recomendado)
conda create -n BigDataPipelines python=3.10
conda activate BigDataPipelines

# Instalar dependencias conda primero 
# Si se presentan inconvenientes con patsy usar: conda install patsy 
conda install pandas numpy scipy matplotlib seaborn scikit-learn kaggle -c conda-forge


# Instalar prefect, plotly y pymongo con pip
pip install -U prefect plotly "pymongo[srv]" dnspython python-dotenv geopandas folium
```

---

## 2. Configurar Kaggle API

1. Ve a https://www.kaggle.com/ → Settings → API → **Create New Token**
2. Descarga `kaggle.json` y colócalo en `~/.kaggle/kaggle.json`
3. En Windows: `C:\Users\<usuario>\.kaggle\kaggle.json`

```bash
# Verificar configuración
kaggle datasets list --search "youtube-new"
```

Si no tienes acceso a la API, descarga manualmente desde:
https://www.kaggle.com/datasets/datasnaek/youtube-new
y coloca todos los archivos en `data/staging/`.

---

## 3. Configurar MongoDB Atlas (Parte III)

1. Crea una cuenta en https://www.mongodb.com/cloud/atlas/register
2. Crea un cluster M0 (gratuito)
3. Crea un usuario de base de datos y habilita acceso desde `0.0.0.0/0`
4. Copia la cadena de conexión y crea el archivo `.env`:

```ini
# .env
MONGO_URI=mongodb+srv://<usuario>:<clave>@cluster0.xxxxx.mongodb.net/
```

---

## 4. Ejecutar el pipeline

### Opción A — Pipeline completo (recomendado)
```bash
python main_pipeline.py
```

### Opción B — Por partes
```bash
python parte1_pipeline.py   # ETL + staging
python parte2_dashboard.py  # Dashboard Plotly
python parte3_mongodb_geo_corr.py  # MongoDB + Geo + Correlación
```

### Opción C — Con Prefect UI (monitoreo)
```bash
# Terminal 1: iniciar servidor Prefect
prefect server start

# Terminal 2: ejecutar pipeline
python main_pipeline.py
# Abrir http://localhost:4200 para ver el DAG y estados
```

---

## 5. Descripción de cada parte

### Parte I — Pipeline ETL con Prefect

| Tarea | Descripción |
|-------|-------------|
| `download_data` | Descarga archivos CSV/JSON desde Kaggle a `data/staging/` |
| `load_categories` | Lee JSONs de categorías → dict `{country: {id: nombre}}` |
| `unify_and_top100` | Unifica CSVs, añade país y categoría, selecciona Top-100 global por vistas |
| `standardize_dates` | Estandariza `publish_time` con **a) apply+lambda** y **b) iterrows** |
| `compute_days_to_trending` | Calcula días entre publicación y fecha de tendencia |
| `save_processed` | Guarda el dataset final en CSV y Parquet |

**Decoradores personalizados implementados** (tema Session 3):
- `@timing_decorator`: mide tiempo de ejecución de cada tarea
- `@validate_dataframe`: verifica que el resultado sea un DataFrame no vacío

### Parte II — Dashboard Interactivo (Plotly)

- **Top 5 vídeos por país**: bar chart horizontal facetado
- **Categorías por país**: stacked bar con vistas en millones (datos en formato largo/tidy)
- **Polaridad de interacción**: bubble chart con `polarity = (likes − dislikes) / (likes + dislikes)`
- **Heatmap**: polaridad promedio por Categoría × País
- **Línea temporal**: evolución mensual de vídeos en tendencia

### Parte III — MongoDB + Geo + Correlación

#### MongoDB
- Upsert por `(video_id, country)` para idempotencia
- Colección: `youtube_trending.top100_videos`

#### Mapas georreferenciados
- **Bubble map**: vídeo más visto por país (tamaño = vistas)
- **Scatter geo coloreado**: categoría más vista por país

#### Correlación
Se usa **Spearman** (no Pearson) porque:
- Las vistas de YouTube tienen distribución fuertemente sesgada (pocos vídeos con millones de vistas)
- Hay muchos valores atípicos (outliers)
- No se puede asumir normalidad ni linealidad
- Spearman mide asociación monotónica y es robusto a outliers

Resultados reportados:
- Spearman ρ (método principal)
- Pearson r (referencia)
- Kendall τ (complementario)

---

## 6. Salidas generadas

| Archivo | Contenido |
|---------|-----------|
| `data/processed/youtube_top100_processed.csv` | Dataset final procesado |
| `data/processed/youtube_top100_processed.parquet` | Mismo dataset en Parquet |
| `data/processed/dashboard_youtube.html` | Dashboard Parte II (Plotly) |
| `data/processed/dashboard_completo.html` | Dashboard Partes II + III |

---

## 7. Conceptos aplicados de clase

| Concepto | Sesión | Aplicación |
|----------|--------|-----------|
| CRISP-DM / TDSP | Sesión 3, Parte I | Estructura del pipeline (Business Understanding → Deployment) |
| ETL / ELT | Sesión 3, Parte II | Pipeline de datos (Staging → Transform → Load) |
| Data Lakehouse | Sesión 3, Parte II | Bronze (staging) → Silver (procesado) → Gold (top100) |
| Prefect Flows & Tasks | Sesión 3, Parte III | `@flow`, `@task`, retries, estados |
| Decoradores Python | Sesión 3, Parte III | `@timing_decorator`, `@validate_dataframe` |
| Descriptive Statistics | Sesión 4 | Correlación Spearman, mediana, media |
| Scatter / Bubble plots | Sesión 4 | Dashboard Plotly |
| Box plots / Histogramas | Sesión 4 | Distribución de días a trending |
| Tidy Data (long format) | Sesión 4 | DataFrames para Plotly Express |
