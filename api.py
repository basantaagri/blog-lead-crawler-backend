from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os, csv, io

print("### BLOG LEAD CRAWLER API v1.3.6 — STABLE (DB-DRIVEN) ###")

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
# 🧱 CRAWL (RESTORED STABLE BEHAVIOR)
# - ONLY stores blog root
# - NO crawling
# - NO extraction
# =========================================================
@app.post("/crawl")
def crawl_blog(req: CrawlRequest):
    blog_url = req.blog_url.strip().rstrip("/")
    if not blog_url:
        raise HTTPException(400, "blog_url required")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO blog_pages (blog_url, is_root)
                VALUES (%s, TRUE)
                ON CONFLICT (blog_url) DO NOTHING
                """,
                (blog_url,),
            )
            conn.commit()

    return {"status": "ok", "message": "blog stored"}

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
# CSV HELPER (ORDER LOCKED)
# =========================================================
BLOG_PAGE_LINK_FIELDS = [
    "blog_url",
    "blog_page_url",
    "commercial_url",
    "commercial_domain",
    "anchor_text",
    "is_dofollow",
    "is_casino",
    "first_seen",
]

def csv_dict_stream(fieldnames, rows):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k) for k in fieldnames})
    buf.seek(0)
    return buf

# =========================================================
# EXPORT — BLOG PAGE LINKS (ALL)
# =========================================================
@app.get("/export/blog-page-links")
def export_blog_page_links():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    root.blog_url,
                    bp.blog_url AS blog_page_url,
                    ol.url AS commercial_url,
                    ol.commercial_domain,
                    ol.anchor_text,
                    ol.is_dofollow,
                    cs.is_casino,
                    ol.first_seen
                FROM outbound_links ol
                JOIN blog_pages bp ON bp.id = ol.blog_page_id
                JOIN blog_pages root
                  ON root.is_root = TRUE
                 AND bp.blog_url ILIKE '%' || replace(replace(root.blog_url,'https://',''),'http://','') || '%'
                JOIN commercial_sites cs ON cs.commercial_domain = ol.commercial_domain
                ORDER BY root.blog_url
                """
            )
            rows = cur.fetchall()

    return StreamingResponse(
        csv_dict_stream(BLOG_PAGE_LINK_FIELDS, rows),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=blog_page_links.csv"},
    )

# =========================================================
# EXPORT — CASINO ONLY
# =========================================================
@app.get("/export/blog-page-links/casino-only")
def export_casino_links():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    root.blog_url,
                    bp.blog_url AS blog_page_url,
                    ol.url AS commercial_url,
                    ol.commercial_domain,
                    ol.anchor_text,
                    ol.is_dofollow,
                    cs.is_casino,
                    ol.first_seen
                FROM outbound_links ol
                JOIN blog_pages bp ON bp.id = ol.blog_page_id
                JOIN blog_pages root
                  ON root.is_root = TRUE
                 AND bp.blog_url ILIKE '%' || replace(replace(root.blog_url,'https://',''),'http://','') || '%'
                JOIN commercial_sites cs ON cs.commercial_domain = ol.commercial_domain
                WHERE cs.is_casino = TRUE
                ORDER BY root.blog_url
                """
            )
            rows = cur.fetchall()

    return StreamingResponse(
        csv_dict_stream(BLOG_PAGE_LINK_FIELDS, rows),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=casino_links.csv"},
    )

# =========================================================
# EXPORT — DOFOLLOW ONLY
# =========================================================
@app.get("/export/blog-page-links/dofollow-only")
def export_dofollow_links():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    root.blog_url,
                    bp.blog_url AS blog_page_url,
                    ol.url AS commercial_url,
                    ol.commercial_domain,
                    ol.anchor_text,
                    ol.is_dofollow,
                    cs.is_casino,
                    ol.first_seen
                FROM outbound_links ol
                JOIN blog_pages bp ON bp.id = ol.blog_page_id
                JOIN blog_pages root
                  ON root.is_root = TRUE
                 AND bp.blog_url ILIKE '%' || replace(replace(root.blog_url,'https://',''),'http://','') || '%'
                JOIN commercial_sites cs ON cs.commercial_domain = ol.commercial_domain
                WHERE ol.is_dofollow = TRUE
                ORDER BY root.blog_url
                """
            )
            rows = cur.fetchall()

    return StreamingResponse(
        csv_dict_stream(BLOG_PAGE_LINK_FIELDS, rows),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=dofollow_links.csv"},
    )
