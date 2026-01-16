from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os, csv, io

print("### BLOG LEAD CRAWLER API v1.3.6 — STABLE + AUTO EXTRACT RESTORED ###")

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
# 🚀 ONE-FLOW CRAWL (OLD BEHAVIOR RESTORED)
# =========================================================
@app.post("/crawl")
def crawl_blog(req: CrawlRequest):
    blog_url = req.blog_url.strip()
    if not blog_url:
        raise HTTPException(400, "blog_url required")

    # save root blog
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

    # 🔥 AUTO extraction (old behavior)
    try:
        from services.orchestrator import CrawlOrchestrator
        CrawlOrchestrator().run_for_blog(blog_url)
    except Exception as e:
        print("AUTO EXTRACT ERROR:", e)

    return {"status": "ok", "message": "crawl + extraction complete"}

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
# EXPORTS
# =========================================================
def csv_stream(headers, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for r in rows:
        w.writerow(r)
    buf.seek(0)
    return buf

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
    )
