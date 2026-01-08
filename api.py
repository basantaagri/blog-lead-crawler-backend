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
app = FastAPI(title="Blog Lead Crawler API", version="1.2.3")

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

def is_valid_post_url(url: str, domain: str) -> bool:
    blacklist = ["/tag/", "/category/", "/author/", "/page/", "/feed"]
    return extract_domain(url) == domain and not any(b in url for b in blacklist)

# =========================================================
# SITEMAP DISCOVERY
# =========================================================
def fetch_sitemap_urls(blog_url: str):
    for path in ["/sitemap.xml", "/post-sitemap.xml", "/wp-sitemap.xml"]:
        try:
            r = session.get(blog_url + path, timeout=15)
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.text)
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            return [{"url": loc.text.strip()} for loc in root.findall(".//ns:loc", ns)]
        except:
            continue
    return []

def fallback_discover_posts(blog_url: str, domain: str):
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

# =========================================================
# LINK EXTRACTION
# =========================================================
def extract_outbound_links(page_url: str):
    try:
        r = session.get(page_url, timeout=15)
        html = r.text.lower()

        if r.status_code >= 400 or any(b in html for b in BLOCK_SIGNATURES):
            return "BLOCKED"

        soup = BeautifulSoup(r.text, "html.parser")
        base_domain = extract_domain(page_url)
        links = []

        for a in soup.find_all("a", href=True):
            full = urljoin(page_url, a["href"])
            if extract_domain(full) == base_domain:
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
# 🔧 ADDED BACK — crawl-links (NO OTHER CHANGES)
# =========================================================
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
        cur.close()
        conn.close()
        raise HTTPException(404, "Run /crawl first")

    saved = casino = blocked = 0

    for p in pages:
        time.sleep(random.uniform(1.0, 2.0))
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

# =========================================================
# LEAD SCORE (UNCHANGED)
# =========================================================
@app.get("/blog-lead-score/{blog_id}")
def blog_lead_score(blog_id: int):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT blog_url
        FROM blog_pages
        WHERE id = %s AND is_root = TRUE
    """, (blog_id,))
    root = cur.fetchone()

    if not root:
        cur.close()
        conn.close()
        raise HTTPException(404, "Blog not found")

    root_url = root["blog_url"]

    cur.execute("""
        SELECT
            COUNT(ol.id) AS total_links,
            SUM(CASE WHEN ol.is_casino THEN 1 ELSE 0 END) AS casino,
            SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END) AS dofollow
        FROM blog_pages bp
        JOIN outbound_links ol ON ol.blog_page_id = bp.id
        WHERE bp.blog_url LIKE %s
    """, (root_url + "%",))

    r = cur.fetchone() or {}
    cur.close()
    conn.close()

    total = r["total_links"] or 0
    casino = r["casino"] or 0
    dofollow = r["dofollow"] or 0

    return {
        "total_links": total,
        "casino_percentage": round((casino / total) * 100, 2) if total else 0,
        "dofollow_percentage": round((dofollow / total) * 100, 2) if total else 0,
        "lead_score": max(0, min(100, dofollow - casino))
    }
