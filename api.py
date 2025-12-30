from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pymysql
from datetime import datetime, timedelta
import csv
import io
import os

app = FastAPI(
    title="Blog Lead Crawler API",
    version="1.0.0"
)

# =========================
# CORS
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# DATABASE CONFIG
# (Use ENV vars on Render)
# =========================
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "blog_lead_crawler"),
    "cursorclass": pymysql.cursors.DictCursor
}

def get_db():
    return pymysql.connect(**DB_CONFIG)

# =========================
# HEALTH CHECK
# =========================
@app.get("/")
def health():
    return {"status": "ok"}

# =========================
# LAST 30 DAYS BLOG HISTORY
# =========================
@app.get("/history")
def last_30_days():
    db = get_db()
    cur = db.cursor()

    cur.execute("""
        SELECT blog_url, first_crawled
        FROM blogs
        WHERE first_crawled >= %s
        ORDER BY first_crawled DESC
    """, (datetime.now() - timedelta(day
