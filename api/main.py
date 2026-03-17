# api/main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security.api_key import APIKeyHeader
from google.cloud import bigquery
import os

app = FastAPI(
    title="Data Engineering Platform — Analytics API",
    description="KPIs business exposés depuis BigQuery Marts",
    version="1.0.0"
)

# ── Auth ─────────────────────────────────────────────────────────────────────
API_KEY = os.getenv("API_KEY", "changeme")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def check_api_key(key: str = Depends(api_key_header)):
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="API key invalide")
    return key

# ── BQ Client ────────────────────────────────────────────────────────────────
PROJECT = os.getenv("GCP_PROJECT_ID", "ton-projet-gcp")
MARTS   = os.getenv("GCP_DATASET_MARTS", "marts")

def get_bq_client():
    return bigquery.Client(project=PROJECT, location="EU")

# ── Health ───────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}

# ── KPIs ─────────────────────────────────────────────────────────────────────
@app.get("/kpis/daily-revenue", dependencies=[Depends(check_api_key)])
def daily_revenue(days: int = 7):
    client = get_bq_client()
    query = f"""
        SELECT
            event_date,
            revenue,
            nb_purchases,
            nb_buyers,
            avg_order_value
        FROM `{PROJECT}.{MARTS}.mart_daily_revenue`
        WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        ORDER BY event_date DESC
    """
    rows = client.query(query).result()
    return [dict(row) for row in rows]

@app.get("/kpis/top-products", dependencies=[Depends(check_api_key)])
def top_products(limit: int = 10):
    client = get_bq_client()
    query = f"""
        SELECT product_id, total_revenue, nb_purchases
        FROM `{PROJECT}.{MARTS}.mart_top_products`
        ORDER BY total_revenue DESC
        LIMIT {limit}
    """
    rows = client.query(query).result()
    return [dict(row) for row in rows]

@app.get("/kpis/conversion-rate", dependencies=[Depends(check_api_key)])
def conversion_rate():
    client = get_bq_client()
    query = f"""
        SELECT
            event_date,
            nb_views,
            nb_purchases,
            ROUND(nb_purchases / NULLIF(nb_views, 0) * 100, 2) AS conversion_pct
        FROM `{PROJECT}.{MARTS}.mart_funnel_conversion`
        ORDER BY event_date DESC
        LIMIT 30
    """
    rows = client.query(query).result()
    return [dict(row) for row in rows]