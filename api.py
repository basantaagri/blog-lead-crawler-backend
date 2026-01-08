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
# GLOBAL BROWSER HEADERS
# =========================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.google.com/"
}

session = requests.Session()
session.headers.update(HEADERS)

# =========================
# APP INIT
# =========================
app = FastAPI(title="Blog Lead Crawler API", version="1.2.0")

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
    "attention required",
    "verify you are human",
    "/cdn-cgi/",
    "access denied"
]

# =========================
# HELPERS
# =========================
def normalize_blog_url(url: str) -> str:
    url = url.strip()
    if not url.startswith("http"):
        url = "https://" + url
    return url.rstrip("/")

def extract_domain(url: str) -> str:
    return urlparse(url).netloc.lower().replace("www.", "")

def is_casino_link(url: str) -> bool:
    return any(k in url.lower() for k in CASINO_KEYWORDS)

def is_valid_post_url(url: str, domain: str) -> bool:
    blacklist = [
        "/tag/", "/category/", "/author/", "/page/",
        "/wp-admin", "/wp-content", "/feed", "/comments"
    ]
    return (
        urlparse(url).netloc.replace("www.", "") == domain
        and not any(b in url for b in blacklist)
    )

# =========================
# SITEMAP HANDLING
# =========================
def fetch_sitemap_urls(blog_url: str) -> list:
    paths = ["/sitemap.xml", "/post-sitemap.xml", "/wp-sitemap.xml"]
    for p in paths:
        try:
            r = session.get(blog_url + p, timeout=15, verify=False)
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.text)
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            urls = []
            for u in root.findall("ns:url", ns):
                loc = u.find("ns:loc", ns)
                if loc is not None:
                    urls.append({"url": loc.text.strip()})
            if urls:
                return urls
        except Exception:
            continue
    return []

# =========================
# FALLBACK DISCOVERY
# =========================
def fallback_discover_posts(blog_url: str, domain: str, limit=100):
    found = []
    try:
        r = session.get(blog_url, timeout=20, verify=False)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            full = urljoin(blog_url, a["href"]).rstrip("/")
            if extract_domain(full) == domain:
                found.append(full)
            if len(found) >= limit:
                break
    except Exception:
        pass
    return list(set(found))

# =========================
# LINK EXTRACTION
# =========================
def extract_outbound_links(page_url: str):
    try:
        r = session.get(page_url, timeout=15, verify=False)
        html = r.text.lower()

        if r.status_code >= 400:
            return "BLOCKED"

        if any(sig in html for sig in BLOCK_SIGNATURES):
            return "BLOCKED"

        soup = BeautifulSoup(r.text, "html.parser")
        base_domain = urlparse(page_url).netloc
        links = []

        for a in soup.find_all("a", href=True):
            full = urljoin(page_url, a["href"])
            if not full.startswith("http"):
                continue
            if urlparse(full).netloc == base_domain:
                continue

            rel = [x.lower() for x in a.get("rel", [])]
            is_dofollow = not any(x in rel for x in ["nofollow", "ugc", "sponsored"])
            links.append({"url": full, "is_dofollow": is_dofollow})

        return list({l["url"]: l for l in links}.values())
    except Exception:
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
# CSV HELPER
# =========================
def rows_to_csv(rows, filename):
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

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
    now = datetime.utcnow()

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM blog_pages WHERE blog_url=%s", (blog_url,))
    if cur.fetchone():
        return {"status": "already_exists"}

    cur.execute("""
        INSERT INTO blog_pages (blog_url, first_crawled, is_root)
        VALUES (%s,%s,TRUE)
    """, (blog_url, now))

    urls = fetch_sitemap_urls(blog_url) or [{"url": u} for u in fallback_discover_posts(blog_url, domain)]

    for u in urls[:MAX_PAGES]:
        if is_valid_post_url(u["url"], domain):
            cur.execute("""
                INSERT INTO blog_pages (blog_url, first_crawled, is_root)
                VALUES (%s,%s,FALSE)
                ON CONFLICT DO NOTHING
            """, (u["url"], now))

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
        WHERE COALESCE(is_root,FALSE)=FALSE
        AND blog_url LIKE %s
    """, (blog_url + "%",))
    pages = cur.fetchall()

    if not pages:
        raise HTTPException(404, "Run /crawl first")

    saved = casino = blocked = 0

    for p in pages:
        time.sleep(random.uniform(1.2, 2.5))
        result = extract_outbound_links(p["blog_url"])

        if result == "BLOCKED":
            blocked += 1
            continue

        for l in result:
            is_c = is_casino_link(l["url"])
            cur.execute("""
                INSERT INTO outbound_links
                (blog_page_id,url,is_casino,is_dofollow)
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
        "blocked_pages": blocked,
        "crawl_mode": "cloud_partial"
    }

# =========================
# CSV EXPORTS
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
    return rows_to_csv(rows, "blog_page_links.csv")

@app.get("/export/commercial-sites")
def export_commercial_sites():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM commercial_sites ORDER BY total_links DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows_to_csv(rows, "commercial_sites.csv")

@app.get("/export/blog-summary")
def export_blog_summary():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            root.blog_url AS blog,
            COUNT(DISTINCT cs.commercial_domain) AS commercial_sites,
            COUNT(ol.id) AS total_links,
            SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END) AS dofollow_links,
            SUM(CASE WHEN ol.is_casino THEN 1 ELSE 0 END) AS casino_links
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
    return rows_to_csv(rows, "blog_summary.csv")
