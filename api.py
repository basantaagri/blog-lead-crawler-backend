from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
from datetime import datetime, timedelta

app = FastAPI(title="Blog Lead Crawler API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# DATABASE CONFIG
# =========================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "basant@12345",
    "database": "blog_lead_crawler"
}

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

# =========================
# HEALTH CHECK
# =========================
@app.get("/")
def home():
    return {
        "status": "ok",
        "message": "Blog Lead Crawler backend is running"
    }

# =========================
# LAST 30 DAYS BLOG HISTORY
# =========================
@app.get("/history")
def last_30_days():
    db = get_db()
    cur = db.cursor(dictionary=True)

    cur.execute("""
        SELECT
            blog_url,
            first_crawled
        FROM blogs
        WHERE first_crawled >= %s
        ORDER BY first_crawled DESC
    """, (datetime.now() - timedelta(days=30),))

    rows = cur.fetchall()
    db.close()
    return rows

# =========================
# COMMERCIAL SITES (FIXED)
# =========================
@app.get("/commercial-sites")
def commercial_sites():
    db = get_db()
    cur = db.cursor(dictionary=True)

    cur.execute("""
        SELECT
            commercial_domain,
            total_links,
            dofollow_percent,
            IFNULL(is_casino, 0) AS is_casino
        FROM consolidated_commercial_sites
        ORDER BY total_links DESC
        LIMIT 100
    """)

    rows = cur.fetchall()
    db.close()
    return rows
from fastapi.responses import StreamingResponse
import csv
import io

@app.get("/commercial-sites/export")
def export_commercial_sites():
    db = get_db()
    cur = db.cursor(dictionary=True)

    cur.execute("""
        SELECT commercial_domain, total_links, dofollow_percent, is_casino
        FROM consolidated_commercial_sites
        ORDER BY total_links DESC
    """)

    rows = cur.fetchall()
    db.close()

    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow(["Commercial Domain", "Total Links", "Dofollow %", "Casino"])

    for r in rows:
        writer.writerow([
            r["commercial_domain"],
            r["total_links"],
            r["dofollow_percent"],
            "Yes" if r["is_casino"] else "No"
        ])

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=commercial_sites.csv"}
    )
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
from fastapi import FastAPI

app = FastAPI()

