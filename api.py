from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os, csv, io
from datetime import datetime

print("### BLOG LEAD CRAWLER — STABLE EXPORT + ANALYTICS VERSION RUNNING ###")

# =========================================================
# APP INIT
# =========================================================
app = FastAPI(title="Blog Lead Crawler API", version="1.3.6")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# DATABASE
# =========================================================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found")

def get_db():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        sslmode="require"
    )

# =========================================================
# MODELS
# =========================================================
class CrawlRequest(BaseModel):
    blog_url: str

# =========================================================
# HEALTH
# =========================================================
@app.get("/")
@app.get("/health")
def health():
    return {"status": "ok"}

# =========================================================
# CRAWL BLOG (ROOT ONLY — SAFE)
# =========================================================
@app.post("/crawl")
def crawl_blog(req: CrawlRequest):
    conn = get_db()
    cur = conn.cursor()

    blog_url = req.blog_url.rstrip("/")

    cur.execute("""
        INSERT INTO blog_pages (blog_url, is_root)
        VALUES (%s, TRUE)
        ON CONFLICT (blog_url) DO NOTHING
    """, (blog_url,))

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "blog stored"}

# =========================================================
# CRAWL LINKS (WORKER TRIGGER ONLY — SAFE)
# =========================================================
@app.post("/crawl-links")
def crawl_links(req: CrawlRequest):
    return {"status": "link crawling started"}

# =========================================================
# EXPORT — BLOG → PAGE → COMMERCIAL LINKS
# =========================================================
@app.get("/export/blog-page-links")
def export_blog_page_links():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            bp.blog_url,
            ol.url AS commercial_url,
            CASE WHEN ol.is_dofollow THEN 'Dofollow' ELSE 'Nofollow' END AS link_type,
            CASE WHEN ol.is_casino THEN 'Casino' ELSE 'Non-Casino' END AS casino_classification
        FROM outbound_links ol
        JOIN blog_pages bp ON bp.id = ol.blog_page_id
        ORDER BY bp.blog_url
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    return StreamingResponse(buf, media_type="text/csv")

# =========================================================
# EXPORT — COMMERCIAL SITES (STABLE)
# =========================================================
@app.get("/export/commercial-sites")
def export_commercial_sites():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            cs.commercial_domain,
            COUNT(ol.id) AS total_links,
            ROUND(
                100.0 * SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END)
                / NULLIF(COUNT(ol.id), 0), 2
            ) AS dofollow_percent,
            BOOL_OR(ol.is_casino) AS is_casino,
            cs.meta_title,
            cs.meta_description,
            cs.homepage_checked,
            COUNT(DISTINCT root.blog_url) AS blogs_linking_count
        FROM commercial_sites cs
        JOIN outbound_links ol
          ON ol.url ILIKE '%' || cs.commercial_domain || '%'
        JOIN blog_pages bp ON bp.id = ol.blog_page_id
        JOIN blog_pages root
          ON root.is_root = TRUE
         AND bp.blog_url ILIKE '%' ||
             replace(replace(root.blog_url,'https://',''),'http://','') || '%'
        GROUP BY
            cs.commercial_domain,
            cs.meta_title,
            cs.meta_description,
            cs.homepage_checked
        ORDER BY total_links DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    return StreamingResponse(buf, media_type="text/csv")

# =========================================================
# EXPORT — BLOG SUMMARY
# =========================================================
@app.get("/export/blog-summary")
def export_blog_summary():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            root.blog_url,
            COUNT(DISTINCT cs.commercial_domain) AS unique_commercial_sites,
            ROUND(
                100.0 * SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END)
                / NULLIF(COUNT(ol.id), 0), 2
            ) AS dofollow_percent,
            BOOL_OR(ol.is_casino) AS casino_present
        FROM blog_pages root
        JOIN blog_pages bp
          ON bp.blog_url ILIKE '%' ||
             replace(replace(root.blog_url,'https://',''),'http://','') || '%'
        JOIN outbound_links ol ON ol.blog_page_id = bp.id
        JOIN commercial_sites cs ON ol.url ILIKE '%' || cs.commercial_domain || '%'
        WHERE root.is_root = TRUE
        GROUP BY root.blog_url
        ORDER BY root.blog_url
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    return StreamingResponse(buf, media_type="text/csv")

# =========================================================
# ✅ PER-BLOG ANALYTICS (READ-ONLY, SAFE)
# =========================================================
@app.get("/analytics/blog")
def blog_analytics(blog_url: str):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(DISTINCT bp.id) AS pages_crawled,
            COUNT(ol.id) AS total_outbound_links,
            COUNT(DISTINCT cs.commercial_domain) AS unique_commercial_domains,
            SUM(CASE WHEN ol.is_casino THEN 1 ELSE 0 END) AS casino_links,
            ROUND(
                100.0 * SUM(CASE WHEN ol.is_casino THEN 1 ELSE 0 END)
                / NULLIF(COUNT(ol.id), 0), 2
            ) AS casino_percentage,
            ROUND(
                100.0 * SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END)
                / NULLIF(COUNT(ol.id), 0), 2
            ) AS dofollow_percentage
        FROM blog_pages root
        JOIN blog_pages bp
          ON bp.blog_url ILIKE '%' ||
             replace(replace(root.blog_url,'https://',''),'http://','') || '%'
        JOIN outbound_links ol ON ol.blog_page_id = bp.id
        JOIN commercial_sites cs ON ol.url ILIKE '%' || cs.commercial_domain || '%'
        WHERE root.is_root = TRUE
          AND root.blog_url = %s
    """, (blog_url,))

    summary = cur.fetchone()

    cur.execute("""
        SELECT
            cs.commercial_domain,
            COUNT(ol.id) AS total_links,
            ROUND(
                100.0 * SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END)
                / NULLIF(COUNT(ol.id), 0), 2
            ) AS dofollow_percent,
            SUM(CASE WHEN ol.is_casino THEN 1 ELSE 0 END) AS casino_links,
            ROUND(
                100.0 * SUM(CASE WHEN ol.is_casino THEN 1 ELSE 0 END)
                / NULLIF(COUNT(ol.id), 0), 2
            ) AS casino_percent
        FROM blog_pages root
        JOIN blog_pages bp
          ON bp.blog_url ILIKE '%' ||
             replace(replace(root.blog_url,'https://',''),'http://','') || '%'
        JOIN outbound_links ol ON ol.blog_page_id = bp.id
        JOIN commercial_sites cs ON ol.url ILIKE '%' || cs.commercial_domain || '%'
        WHERE root.is_root = TRUE
          AND root.blog_url = %s
        GROUP BY cs.commercial_domain
        ORDER BY total_links DESC
    """, (blog_url,))

    breakdown = cur.fetchall()

    cur.close()
    conn.close()

    return {
        "blog_url": blog_url,
        "summary": summary,
        "commercial_breakdown": breakdown
    }

# =========================================================
# HISTORY
# =========================================================
@app.get("/history")
def history():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT blog_url, first_crawled, is_root
        FROM blog_pages
        ORDER BY first_crawled DESC
        LIMIT 50
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows

# =========================================================
# PROGRESS
# =========================================================
@app.get("/progress")
def progress():
    return {
        "status": "idle",
        "last_updated": datetime.utcnow().isoformat()
    }
