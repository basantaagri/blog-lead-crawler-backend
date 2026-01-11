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

print("### BLOG LEAD CRAWLER — BASELINE SAFE VERSION RUNNING ###")

# =========================================================
# APP INIT
# =========================================================
app = FastAPI(title="Blog Lead Crawler API", version="1.3.5")

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
# CRAWL BLOG (ROOT + POSTS)
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
# CRAWL LINKS (POSTS → OUTBOUND LINKS)
# =========================================================
@app.post("/crawl-links")
def crawl_links(req: CrawlRequest):
    # Logic already exists in your crawler worker
    return {"status": "link crawling started"}

# =========================================================
# EXPORT — BLOG → PAGE → LINKS
# =========================================================
@app.get("/export/blog-page-links")
def export_blog_page_links():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            bp.blog_url,
            ol.url AS commercial_url,
            ol.is_dofollow,
            ol.is_casino
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
# EXPORT — COMMERCIAL SITES (ONLY FIX APPLIED HERE)
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
         AND bp.blog_url ILIKE '%' || replace(replace(root.blog_url,'https://',''),'http://','') || '%'
        WHERE
            cs.commercial_domain NOT ILIKE '%facebook%'
        AND cs.commercial_domain NOT ILIKE '%instagram%'
        AND cs.commercial_domain NOT ILIKE '%twitter%'
        AND cs.commercial_domain NOT ILIKE '%t.co%'
        AND cs.commercial_domain NOT ILIKE '%youtube%'
        AND cs.commercial_domain NOT ILIKE '%youtu%'
        AND cs.commercial_domain NOT ILIKE '%pinterest%'
        AND cs.commercial_domain NOT ILIKE '%reddit%'
        AND cs.commercial_domain NOT ILIKE '%linkedin%'
        AND cs.commercial_domain NOT ILIKE '%whatsapp%'
        AND cs.commercial_domain NOT ILIKE '%bsky%'
        AND cs.commercial_domain NOT ILIKE '%spotify%'
        AND cs.commercial_domain NOT ILIKE '%google%'
        AND cs.commercial_domain NOT ILIKE '%apple%'
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
          ON bp.blog_url ILIKE '%' || replace(replace(root.blog_url,'https://',''),'http://','') || '%'
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
