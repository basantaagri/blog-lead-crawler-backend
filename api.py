from dotenv import load_dotenv
load_dotenv()

import os
import csv
import io
import time
import threading
import psycopg2
import requests

from urllib.parse import urljoin
from bs4 import BeautifulSoup
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

print("### BLOG LEAD CRAWLER API v1.3.6 — STABLE + WORKER + SKIP ON FAILURE ###")

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
        sslmode="require",
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
# 🧱 CRAWL — ROOT ONLY (LOCKED)
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

    return {"status": "ok", "message": "blog queued"}

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
                LIMIT 100
                """
            )
            return cur.fetchall()

# =========================================================
# DOMAIN NORMALIZATION (LOCKED)
# =========================================================
def extract_domain(url: str) -> str:
    url = url.replace("https://", "").replace("http://", "")
    if url.startswith("www."):
        url = url[4:]
    return url.split("/")[0].strip()

# =========================================================
# SAFE FETCH (SSL HARDENED)
# =========================================================
def safe_fetch(url: str):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        )
    }

    try:
        return requests.get(url, headers=headers, timeout=15)
    except requests.exceptions.SSLError:
        try:
            http_url = url.replace("https://", "http://", 1)
            return requests.get(http_url, headers=headers, timeout=15)
        except Exception:
            return None

# =========================================================
# 🔁 CRAWLER WORKER (SKIP-ON-FAILURE)
# =========================================================
def crawler_worker():
    print("🟢 Crawler worker started")

    while True:
        try:
            # -------------------------------------------------
            # Pick ONE uncrawled root blog
            # -------------------------------------------------
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, blog_url
                        FROM blog_pages
                        WHERE is_root = TRUE
                          AND id NOT IN (
                              SELECT DISTINCT blog_page_id FROM outbound_links
                          )
                        ORDER BY first_crawled ASC
                        LIMIT 1
                        """
                    )
                    blog = cur.fetchone()

            if not blog:
                time.sleep(5)
                continue

            blog_id = blog["id"]
            blog_url = blog["blog_url"]
            print(f"🔍 Crawling blog: {blog_url}")

            # -------------------------------------------------
            # Fetch blog safely
            # -------------------------------------------------
            resp = safe_fetch(blog_url)

            if not resp or resp.status_code != 200:
                raise Exception("Unreachable or blocked")

            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.find_all("a", href=True)

            with get_conn() as conn:
                with conn.cursor() as cur:
                    for a in links:
                        href = a.get("href", "").strip()
                        if not href or href.startswith("#"):
                            continue

                        full_url = urljoin(blog_url, href)
                        domain = extract_domain(full_url)
                        anchor = a.get_text(strip=True)[:255]

                        cur.execute(
                            """
                            INSERT INTO outbound_links
                            (blog_page_id, url, commercial_domain, anchor_text, is_dofollow)
                            VALUES (%s, %s, %s, %s, TRUE)
                            ON CONFLICT DO NOTHING
                            """,
                            (blog_id, full_url, domain, anchor),
                        )

                        cur.execute(
                            """
                            INSERT INTO commercial_sites (commercial_domain)
                            VALUES (%s)
                            ON CONFLICT (commercial_domain) DO NOTHING
                            """,
                            (domain,),
                        )

                conn.commit()

            print(f"✅ Completed blog: {blog_url}")

        except Exception as e:
            print(f"❌ Failed blog, skipping permanently: {blog_url} | {e}")

            # 🔒 SKIP-ON-FAILURE PATCH (THE FIX)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE blog_pages
                        SET is_root = FALSE
                        WHERE id = %s
                        """,
                        (blog_id,),
                    )
                    conn.commit()

            time.sleep(3)

# =========================================================
# START WORKER THREAD
# =========================================================
threading.Thread(target=crawler_worker, daemon=True).start()

# =========================================================
# CSV CONFIG (LOCKED)
# =========================================================
BLOG_PAGE_LINK_FIELDS = [
    "blog_page_url",
    "commercial_url",
    "commercial_domain",
    "anchor_text",
    "is_dofollow",
    "is_casino",
    "first_seen",
]

def csv_dict_stream(fieldnames, rows):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k) for k in fieldnames})
    buffer.seek(0)
    return buffer

# =========================================================
# EXPORTS (DB-ONLY — UNCHANGED)
# =========================================================
@app.get("/export/blog-page-links")
def export_blog_page_links():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    bp.blog_url AS blog_page_url,
                    ol.url AS commercial_url,
                    ol.commercial_domain,
                    ol.anchor_text,
                    ol.is_dofollow,
                    cs.is_casino,
                    cs.created_at AS first_seen
                FROM outbound_links ol
                JOIN blog_pages bp ON bp.id = ol.blog_page_id
                JOIN commercial_sites cs ON cs.commercial_domain = ol.commercial_domain
                ORDER BY cs.created_at DESC
                """
            )
            rows = cur.fetchall()

    return StreamingResponse(
        csv_dict_stream(BLOG_PAGE_LINK_FIELDS, rows),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=blog_page_links.csv"},
    )
