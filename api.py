from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import csv
import io

print("### BLOG LEAD CRAWLER API v1.3.6 — STABLE + SAFE DELETE (LOCKED) + SCORING ###")

# =========================================================
# 🔐 ADMIN DELETE LOCK
# =========================================================
ENABLE_ADMIN_DELETE = os.getenv("ENABLE_ADMIN_DELETE", "false").lower() == "true"

# =========================================================
# APP INIT
# =========================================================
app = FastAPI(
    title="Blog Lead Crawler API",
    version="1.3.6",
)

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
    raise RuntimeError("DATABASE_URL not set")

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

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
# DISCOVER BLOG ROOT
# =========================================================
@app.post("/crawl")
def crawl_blog(req: CrawlRequest):
    blog_url = req.blog_url.strip()
    if not blog_url:
        raise HTTPException(400, "blog_url required")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO blog_pages (blog_url, is_root)
                VALUES (%s, true)
                ON CONFLICT (blog_url) DO NOTHING
                """,
                (blog_url,),
            )
            conn.commit()

    return {"status": "ok"}

# =========================================================
# WORKER TRIGGER (NO LOGIC HERE)
# =========================================================
@app.post("/crawl-links")
def crawl_links(req: CrawlRequest):
    if not req.blog_url.strip():
        raise HTTPException(400, "blog_url required")
    return {"status": "ok"}

# =========================================================
# HISTORY
# =========================================================
@app.get("/history")
def history():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT blog_url, first_crawled, is_root
                FROM blog_pages
                ORDER BY first_crawled DESC
                """
            )
            return cur.fetchall()

# =========================================================
# ANALYTICS — PER BLOG (FIXED)
# =========================================================
@app.get("/analytics/blog")
def analytics_blog(blog_url: str):
    if not blog_url:
        raise HTTPException(400, "blog_url required")

    with get_conn() as conn:
        with conn.cursor() as cur:

            # Pages count
            cur.execute(
                "SELECT COUNT(*) FROM blog_pages WHERE blog_url = %s",
                (blog_url,),
            )
            pages = cur.fetchone()["count"]

            # Stats (JOIN commercial_sites)
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_links,
                    COUNT(DISTINCT cs.commercial_domain) AS unique_domains,
                    AVG(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END) * 100 AS dofollow_pct,
                    AVG(CASE WHEN cs.is_casino THEN 1 ELSE 0 END) * 100 AS casino_pct
                FROM outbound_links ol
                JOIN commercial_sites cs
                    ON cs.commercial_domain = ol.commercial_domain
                WHERE ol.blog_page_id IN (
                    SELECT id FROM blog_pages WHERE blog_url = %s
                )
                """,
                (blog_url,),
            )
            stats = cur.fetchone()

    return {
        "pages_crawled": pages or 0,
        "total_outbound_links": stats["total_links"] or 0,
        "unique_commercial_domains": stats["unique_domains"] or 0,
        "dofollow_percentage": round(stats["dofollow_pct"] or 0, 2),
        "casino_percentage": round(stats["casino_pct"] or 0, 2),
    }

# =========================================================
# 🧮 LEAD SCORING (READ ONLY)
# =========================================================
@app.get("/score/commercial-sites")
def score_commercial_sites():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    cs.commercial_domain,
                    cs.total_links,
                    cs.dofollow_percent,
                    cs.is_casino,
                    COUNT(DISTINCT bp.blog_url) AS blog_count
                FROM commercial_sites cs
                LEFT JOIN outbound_links ol
                    ON ol.commercial_domain = cs.commercial_domain
                LEFT JOIN blog_pages bp
                    ON bp.id = ol.blog_page_id
                GROUP BY
                    cs.commercial_domain,
                    cs.total_links,
                    cs.dofollow_percent,
                    cs.is_casino
                """
            )
            rows = cur.fetchall()

    scored = []
    for r in rows:
        score = (
            min(r["total_links"], 20) * 2
            + (r["dofollow_percent"] or 0) * 0.4
            + (r["blog_count"] or 0) * 5
            - (30 if r["is_casino"] else 0)
        )
        scored.append({
            "commercial_domain": r["commercial_domain"],
            "score": max(0, min(100, round(score))),
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored

# =========================================================
# CSV HELPERS
# =========================================================
def csv_stream(headers, rows):
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)
    buffer.seek(0)
    return buffer

# =========================================================
# EXPORTS
# =========================================================
@app.get("/export/blog-page-links")
def export_blog_page_links():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bp.blog_url, ol.url, ol.is_dofollow, cs.is_casino
                FROM outbound_links ol
                JOIN blog_pages bp ON bp.id = ol.blog_page_id
                JOIN commercial_sites cs ON cs.commercial_domain = ol.commercial_domain
                """
            )
            rows = cur.fetchall()

    return StreamingResponse(
        csv_stream(
            ["blog_url", "outbound_url", "dofollow", "casino"],
            [(r["blog_url"], r["url"], r["is_dofollow"], r["is_casino"]) for r in rows],
        ),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=blog_page_links.csv"},
    )

@app.get("/export/commercial-sites")
def export_commercial_sites():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT commercial_domain, total_links, dofollow_percent, is_casino
                FROM commercial_sites
                ORDER BY total_links DESC
                """
            )
            rows = cur.fetchall()

    return StreamingResponse(
        csv_stream(
            ["domain", "total_links", "dofollow_percent", "casino"],
            [(r["commercial_domain"], r["total_links"], r["dofollow_percent"], r["is_casino"]) for r in rows],
        ),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=commercial_sites.csv"},
    )

@app.get("/export/blog-summary")
def export_blog_summary():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    bp.blog_url,
                    COUNT(DISTINCT ol.commercial_domain) AS commercial_domains,
                    AVG(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END) * 100 AS dofollow_pct,
                    BOOL_OR(cs.is_casino) AS has_casino
                FROM blog_pages bp
                LEFT JOIN outbound_links ol ON ol.blog_page_id = bp.id
                LEFT JOIN commercial_sites cs ON cs.commercial_domain = ol.commercial_domain
                GROUP BY bp.blog_url
                """
            )
            rows = cur.fetchall()

    return StreamingResponse(
        csv_stream(
            ["blog_url", "commercial_domains", "dofollow_percent", "has_casino"],
            [(r["blog_url"], r["commercial_domains"], round(r["dofollow_pct"] or 0, 2), r["has_casino"]) for r in rows],
        ),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=blog_summary.csv"},
    )

# =========================================================
# 🔐 SAFE DELETE (LOCKED)
# =========================================================
@app.delete("/admin/delete-blog")
def delete_blog(blog_url: str = Query(...)):
    if not ENABLE_ADMIN_DELETE:
        raise HTTPException(403, "Admin delete is locked")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM outbound_links WHERE blog_page_id IN (SELECT id FROM blog_pages WHERE blog_url=%s)",
                (blog_url,),
            )
            cur.execute("DELETE FROM blog_pages WHERE blog_url=%s", (blog_url,))
            conn.commit()

    return {"status": "deleted", "blog_url": blog_url}
