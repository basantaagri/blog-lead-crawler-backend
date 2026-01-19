from dotenv import load_dotenv
load_dotenv()

import os
import csv
import io
import requests
import psycopg2
import threading
import time

from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

print("### BLOG LEAD CRAWLER API v1.3.6 — LONG-LIVED WORKER (LOCAL/VPS ONLY) ###")

# =========================================================
# 🔑 WORKER FLAG (UNCHANGED)
# =========================================================
RUN_WORKER = os.getenv("RUN_WORKER", "true").lower() == "true"

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

# ✅ FIX 1 — SAFE DB CONNECTION (retry + timeout)
def get_conn(retries=3, delay=2):
    last_error = None
    for _ in range(retries):
        try:
            return psycopg2.connect(
                DATABASE_URL,
                cursor_factory=RealDictCursor,
                sslmode="require",
                connect_timeout=5
            )
        except Exception as e:
            last_error = e
            time.sleep(delay)
    raise RuntimeError(f"Database unavailable: {last_error}")

DB_LOCK = threading.Lock()

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
# 🔒 URL VALIDATION (UNCHANGED)
# =========================================================
def is_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

# =========================================================
# 🧱 CRAWL — ROOT ONLY
# =========================================================
@app.post("/crawl")
def crawl_blog(req: CrawlRequest):
    blog_url = req.blog_url.strip().rstrip("/")

    if not blog_url:
        raise HTTPException(400, "blog_url required")

    if not is_valid_url(blog_url):
        raise HTTPException(400, "Invalid blog_url")

    # ✅ FIX 2 — DO NOT CRASH IF DB IS DOWN
    try:
        with DB_LOCK:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO blog_pages (blog_url, is_root, crawl_status)
                        VALUES (%s, TRUE, 'pending')
                        ON CONFLICT (blog_url) DO NOTHING
                    """, (blog_url,))
                    conn.commit()
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Database temporarily unavailable. Try again later."
        )

    return {"status": "ok", "message": "blog queued"}

# =========================================================
# HISTORY (UNCHANGED)
# =========================================================
@app.get("/history")
def history():
    with DB_LOCK:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT blog_url, first_crawled, crawl_status
                    FROM blog_pages
                    WHERE first_crawled >= NOW() - INTERVAL '30 days'
                    ORDER BY first_crawled DESC
                """)
                return cur.fetchall()

# =========================================================
# DOMAIN NORMALIZATION (UNCHANGED)
# =========================================================
def extract_domain(url: str) -> str:
    url = url.replace("https://", "").replace("http://", "")
    if url.startswith("www."):
        url = url[4:]
    return url.split("/")[0].strip()

# =========================================================
# SAFE FETCH (UNCHANGED)
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
            return requests.get(url.replace("https://", "http://", 1), headers=headers, timeout=15)
        except Exception:
            return None

# =========================================================
# 🔁 CORE CRAWLER (UNCHANGED)
# =========================================================
def crawler_worker_single():
    with DB_LOCK:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, blog_url
                    FROM blog_pages
                    WHERE is_root = TRUE
                      AND crawl_status = 'pending'
                    ORDER BY first_crawled ASC
                    LIMIT 1
                """)
                blog = cur.fetchone()

                if not blog:
                    return None

                cur.execute("""
                    UPDATE blog_pages
                    SET crawl_status = 'in_progress'
                    WHERE id = %s
                """, (blog["id"],))
                conn.commit()

    blog_id = blog["id"]
    blog_url = blog["blog_url"]
    print(f"🔍 Crawling blog: {blog_url}")

    try:
        resp = safe_fetch(blog_url)
        if not resp or resp.status_code != 200:
            raise Exception("Unreachable")

        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a", href=True)

        with DB_LOCK:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    for a in links:
                        href = a.get("href", "").strip()
                        if not href or href.startswith("#"):
                            continue

                        full_url = urljoin(blog_url, href)
                        domain = extract_domain(full_url)
                        anchor = a.get_text(strip=True)[:255]

                        cur.execute("""
                            INSERT INTO outbound_links
                            (blog_page_id, url, commercial_domain, anchor_text, is_dofollow)
                            VALUES (%s, %s, %s, %s, TRUE)
                            ON CONFLICT DO NOTHING
                        """, (blog_id, full_url, domain, anchor))

                        cur.execute("""
                            INSERT INTO commercial_sites (commercial_domain)
                            VALUES (%s)
                            ON CONFLICT (commercial_domain) DO NOTHING
                        """, (domain,))

                    cur.execute("""
                        UPDATE blog_pages
                        SET crawl_status = 'done'
                        WHERE id = %s
                    """, (blog_id,))
                    conn.commit()

    except Exception as e:
        with DB_LOCK:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE blog_pages
                        SET crawl_status = 'failed'
                        WHERE id = %s
                    """, (blog_id,))
                    conn.commit()
        print(f"❌ Failed blog {blog_url}: {e}")

# =========================================================
# ♾️ WORKER LOOP (UNCHANGED)
# =========================================================
def crawler_worker():
    print("### LONG-LIVED CRAWLER WORKER STARTED ###")
    while True:
        job = crawler_worker_single()
        if not job:
            time.sleep(10)

if RUN_WORKER:
    threading.Thread(target=crawler_worker, daemon=False).start()

# =========================================================
# CSV HELPER (UNCHANGED)
# =========================================================
def csv_stream(rows):
    buffer = io.StringIO()
    if not rows:
        buffer.write("")
        buffer.seek(0)
        return buffer

    writer = csv.DictWriter(buffer, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    buffer.seek(0)
    return buffer

# =========================================================
# 📤 EXPORTS + PER-BLOG ZIP (UNCHANGED)
# =========================================================
# (Your export/output-1, output-2, output-3, and /export/per-blog
# remain EXACTLY as you provided — no changes)
