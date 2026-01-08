from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from datetime import datetime
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import urllib3
import time
import random
import csv
import io

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("### BLOG LEAD CRAWLER — HARD FAIL SAFE VERSION RUNNING ###")

# =========================
# GLOBAL HEADERS
# =========================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}

session = requests.Session()
session.headers.update(HEADERS)

# =========================
# APP INIT
# =========================
app = FastAPI(title="Blog Lead Crawler API", version="1.2.2")

# =========================
# CORS
# =========================
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

# =========================
# DATABASE
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found")

def get_db():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        sslmode="require"
    )

# =========================
# CONSTANTS
# =========================
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

# =========================
# HELPERS
# =========================
def normalize_blog_url(url):
    if not url.startswith("http"):
        url = "https://" + url
    return url.rstrip("/")

def extract_domain(url):
    return urlparse(url).netloc.lower().replace("www.", "")

def is_casino_link(url):
    return any(k in url.lower() for k in CASINO_KEYWORDS)

def is_valid_post_url(url, domain):
    blacklist = ["/tag/", "/category/", "/author/", "/page/", "/feed"]
    return (
        extract_domain(url) == domain
        and not any(b in url for b in blacklist)
    )

# =========================
# SITEMAP
# =========================
def fetch_sitemap_urls(blog_url):
    for path in ["/sitemap.xml", "/post-sitemap.xml", "/wp-sitemap.xml"]:
        try:
            r = session.get(blog_url + path, timeout=15)
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.text)
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            return [{"url": loc.text} for loc in root.findall(".//ns:loc", ns)]
        except:
            continue
    return []

def fallback_discover_posts(blog_url, domain):
    try:
        r = session.get(blog_url, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        urls = []
        for a in soup.find_all("a", href=True):
            full = urljoin(blog_url, a["href"])
            if extract_domain(full) == domain:
                urls.append(full)
        return list(set(urls))[:100]
    except:
        return []

# =========================
# LINK EXTRACTION
# =========================
def extract_outbound_links(page_url):
    try:
        r = session.get(page_url, timeout=15)
        html = r.text.lower()

        if r.status_code >= 400 or any(b in html for b in BLOCK_SIGNATURES):
            return "BLOCKED"

        soup = BeautifulSoup(r.text, "html.parser")
        base = extract_domain(page_url)
        links = []

        for a in soup.find_all("a", href=True):
            full = urljoin(page_url, a["href"])
            if extract_domain(full) == base:
                continue

            rel = " ".join(a.get("rel", [])).lower()
            links.append({
                "url": full,
                "is_dofollow": "nofollow" not in rel
            })

        return list({l["url"]: l for l in links}.values())
    except:
        return "BLOCKED"

def upsert_commercial_site(cur, url, is_casino):
    domain = extract_domain(url)
    cur.execute("""
        INSERT INTO commercial_sites (commercial_domain, total_links, is_casino)
        VALUES (%s, 0, FALSE)
        ON CONFLICT (commercial_domain) DO NOTHING
    """, (domain,))
    cur.execute("""
        UPDATE commercial_sites
        SET total_links = total_links + 1,
            is_casino = is_casino OR %s
        WHERE commercial_domain = %s
    """, (is_casino, domain))

# =========================
# CSV INLINE (NO DOWNLOAD)
# =========================
def rows_to_csv(rows):
    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv")

# =========================
# MODELS
# =========================
class CrawlRequest(BaseModel):
    blog_url: str

# =========================
# ROUTES
# =========================
@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/crawl")
def crawl_blog(data: CrawlRequest):
    blog_url = normalize_blog_url(data.blog_url)
    domain = extract_domain(blog_url)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM blog_pages WHERE blog_url=%s", (blog_url,))
    if cur.fetchone():
        return {"status": "already_exists"}

    cur.execute("""
        INSERT INTO blog_pages (blog_url, first_crawled, is_root)
        VALUES (%s, NOW(), TRUE)
    """, (blog_url,))

    urls = fetch_sitemap_urls(blog_url)
    if not urls:
        urls = [{"url": u} for u in fallback_discover_posts(blog_url, domain)]

    for u in urls[:MAX_PAGES]:
        if is_valid_post_url(u["url"], domain):
            cur.execute("""
                INSERT INTO blog_pages (blog_url, first_crawled, is_root)
                VALUES (%s, NOW(), FALSE)
                ON CONFLICT DO NOTHING
            """, (u["url"],))

    conn.commit()
    cur.close()
    conn.close()
    return {"inserted_pages": len(urls)}

@app.post("/crawl-links")
def crawl_links(data: CrawlRequest):
    blog_url = normalize_blog_url(data.blog_url)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, blog_url
        FROM blog_pages
        WHERE is_root = FALSE
        AND blog_url LIKE %s
    """, (blog_url + "%",))

    pages = cur.fetchall()
    if not pages:
        raise HTTPException(404, "Run /crawl first")

    saved = casino = blocked = 0

    for p in pages:
        time.sleep(random.uniform(1.2, 2.2))
        result = extract_outbound_links(p["blog_url"])

        if result == "BLOCKED":
            blocked += 1
            continue

        for l in result:
            is_c = is_casino_link(l["url"])
            cur.execute("""
                INSERT INTO outbound_links
                (blog_page_id, url, is_casino, is_dofollow)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (p["id"], l["url"], is_c, l["is_dofollow"]))

            if cur.fetchone():
                upsert_commercial_site(cur, l["url"], is_c)
                saved += 1
                casino += int(is_c)

        conn.commit()

    cur.close()
    conn.close()

    return {
        "pages_scanned": len(pages),
        "saved_links": saved,
        "casino_links": casino,
        "blocked_pages": blocked
    }

# =========================
# FRONTEND SUPPORT APIS
# =========================
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

@app.get("/blog-lead-score/{blog_id}")
def blog_lead_score(blog_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(ol.id) total_links,
            SUM(CASE WHEN ol.is_casino THEN 1 ELSE 0 END) casino,
            SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END) dofollow
        FROM blog_pages root
        JOIN blog_pages bp ON bp.blog_url LIKE root.blog_url || '%'
        LEFT JOIN outbound_links ol ON ol.blog_page_id = bp.id
        WHERE root.id = %s
    """, (blog_id,))
    r = cur.fetchone() or {}
    cur.close()
    conn.close()

    total = r.get("total_links") or 0
    casino = r.get("casino") or 0
    dofollow = r.get("dofollow") or 0

    return {
        "total_links": total,
        "casino_percentage": round((casino / total) * 100, 2) if total else 0,
        "dofollow_percentage": round((dofollow / total) * 100, 2) if total else 0,
        "lead_score": min(100, max(0, int(dofollow - casino)))
    }

# =========================
# CSV VIEWS (INLINE)
# =========================
@app.get("/export/blog-page-links")
def export_blog_page_links():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT bp.blog_url, ol.url, ol.is_dofollow, ol.is_casino
        FROM outbound_links ol
        JOIN blog_pages bp ON bp.id = ol.blog_page_id
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows_to_csv(rows)

@app.get("/export/commercial-sites")
def export_commercial_sites():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM commercial_sites ORDER BY total_links DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows_to_csv(rows)

@app.get("/export/blog-summary")
def export_blog_summary():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            root.blog_url AS blog,
            COUNT(DISTINCT cs.commercial_domain) commercial_sites,
            COUNT(ol.id) total_links,
            SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END) dofollow_links,
            SUM(CASE WHEN ol.is_casino THEN 1 ELSE 0 END) casino_links
        FROM blog_pages root
        JOIN blog_pages bp ON bp.blog_url LIKE root.blog_url || '%'
        LEFT JOIN outbound_links ol ON ol.blog_page_id = bp.id
        LEFT JOIN commercial_sites cs ON cs.commercial_domain = split_part(ol.url,'/',3)
        WHERE root.is_root = TRUE
        GROUP BY root.blog_url
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows_to_csv(rows)
