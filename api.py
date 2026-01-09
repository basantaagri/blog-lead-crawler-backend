from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import os, requests, csv, io, time, random
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("### BLOG LEAD CRAWLER — HARD FAIL SAFE VERSION RUNNING ###")

# =========================================================
# GLOBAL HEADERS
# =========================================================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}

session = requests.Session()
session.headers.update(HEADERS)

# =========================================================
# APP INIT
# =========================================================
app = FastAPI(title="Blog Lead Crawler API", version="1.2.4")

# =========================================================
# CORS
# =========================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://fancy-mermaid-0d2e68.netlify.app",
        "http://localhost:3000"
    ],
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
# CONSTANTS
# =========================================================
MAX_PAGES = 1000

CASINO_KEYWORDS = [
    "casino", "bet", "betting", "poker", "slots",
    "sportsbook", "gambling", "roulette", "blackjack"
]

BLOCK_SIGNATURES = [
    "cloudflare",
    "verify you are human",
    "/cdn-cgi/",
    "access denied"
]

# =========================================================
# HELPERS
# =========================================================
def normalize_blog_url(url: str) -> str:
    if not url.startswith("http"):
        url = "https://" + url
    return url.rstrip("/")

def extract_domain(url: str) -> str:
    return urlparse(url).netloc.lower().replace("www.", "")

def is_casino_link(url: str) -> bool:
    return any(k in url.lower() for k in CASINO_KEYWORDS)

# 🔮 FUTURE: COMMERCIAL HOMEPAGE INTELLIGENCE (NOT ACTIVE YET)
def fetch_homepage_meta(domain: str):
    try:
        url = "https://" + domain if not domain.startswith("http") else domain
        r = session.get(url, timeout=15, verify=False)
        if r.status_code != 200:
            return None, None, None

        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        desc = soup.find("meta", attrs={"name": "description"})
        description = desc["content"].strip() if desc and desc.get("content") else ""
        text = soup.get_text(separator=" ").lower()
        return title, description, text
    except:
        return None, None, None

def detect_casino_from_text(text: str):
    if not text:
        return False
    return any(k in text for k in CASINO_KEYWORDS)

# =========================================================
# CSV INLINE
# =========================================================
def rows_to_csv(rows):
    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv")

# =========================================================
# MODELS
# =========================================================
class CrawlRequest(BaseModel):
    blog_url: str

# =========================================================
# ROUTES
# =========================================================
@app.get("/")
def health():
    return {"status": "ok"}

@app.get("/history")
def history():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, blog_url, first_crawled
        FROM blog_pages
        WHERE is_root = TRUE
        AND first_crawled >= NOW() - INTERVAL '30 days'
        ORDER BY first_crawled DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# =========================================================
# 📄 OUTPUT #1 — BLOG → PAGE → COMMERCIAL LINKS
# =========================================================
@app.get("/export/blog-page-links")
def export_blog_page_links():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            root.blog_url AS blog,
            bp.blog_url   AS blog_page,
            ol.url        AS commercial_link,
            ol.is_dofollow,
            ol.is_casino
        FROM blog_pages root
        JOIN blog_pages bp
          ON bp.blog_url LIKE root.blog_url || '%'
         AND bp.is_root = FALSE
        JOIN outbound_links ol
          ON ol.blog_page_id = bp.id
        WHERE root.is_root = TRUE
        ORDER BY root.blog_url, bp.blog_url
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows_to_csv(rows)

# =========================================================
# 📄 OUTPUT #2 — COMMERCIAL SITES SUMMARY
# =========================================================
@app.get("/export/commercial-sites")
def export_commercial_sites():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            cs.commercial_domain,
            COUNT(DISTINCT root.blog_url) AS blogs_count,
            cs.total_links,
            cs.dofollow_percent,
            cs.is_casino
        FROM commercial_sites cs
        JOIN outbound_links ol
          ON ol.url LIKE '%' || cs.commercial_domain || '%'
        JOIN blog_pages bp ON bp.id = ol.blog_page_id
        JOIN blog_pages root
          ON root.is_root = TRUE
         AND bp.blog_url LIKE root.blog_url || '%'
        GROUP BY
            cs.commercial_domain,
            cs.total_links,
            cs.dofollow_percent,
            cs.is_casino
        ORDER BY cs.total_links DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows_to_csv(rows)

# =========================================================
# 📄 OUTPUT #3 — BLOG SUMMARY (FIXED JOIN ✅)
# =========================================================
@app.get("/export/blog-summary")
def export_blog_summary():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            root.blog_url AS blog,
            COUNT(DISTINCT cs.commercial_domain) AS unique_commercial_sites,
            ROUND(
                100.0 * SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END)
                / NULLIF(COUNT(ol.id), 0), 2
            ) AS dofollow_percentage,
            BOOL_OR(ol.is_casino) AS has_casino_links
        FROM blog_pages root
        JOIN blog_pages bp
          ON bp.blog_url LIKE root.blog_url || '%'
         AND bp.is_root = FALSE
        JOIN outbound_links ol
          ON ol.blog_page_id = bp.id
        JOIN commercial_sites cs
          ON ol.url LIKE '%' || cs.commercial_domain || '%'
        WHERE root.is_root = TRUE
        GROUP BY root.blog_url
        ORDER BY root.blog_url
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows_to_csv(rows)

# =========================================================
# crawl-links & lead score UNCHANGED
# =========================================================
