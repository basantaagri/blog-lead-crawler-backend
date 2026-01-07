from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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
app = FastAPI(title="Blog Lead Crawler API", version="1.1.0")

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
def fetch_child_sitemap(sitemap_url: str) -> list:
    items = []
    try:
        r = session.get(sitemap_url, timeout=15, verify=False)
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.text)
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for u in root.findall("ns:url", ns):
            loc = u.find("ns:loc", ns)
            if loc is not None:
                items.append({"url": loc.text.strip()})
    except Exception:
        pass
    return items

def fetch_sitemap_urls(blog_url: str) -> list:
    sitemap_paths = ["/sitemap.xml", "/post-sitemap.xml", "/wp-sitemap.xml"]
    for path in sitemap_paths:
        try:
            r = session.get(blog_url + path, timeout=15, verify=False)
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
# LINK EXTRACTION (WITH BLOCK DETECTION)
# =========================
def extract_outbound_links(page_url: str):
    try:
        r = session.get(page_url, timeout=15, verify=False)
        html = r.text.lower()

        if r.status_code >= 400:
            return "BLOCKED"

        if any(sig in html for sig in BLOCK_SIGNATURES):
            return "BLOCKED"

        if not r.headers.get("content-type", "").startswith("text/html"):
            return []

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
        cur.close()
        conn.close()
        return {"status": "already_exists"}

    cur.execute("""
        INSERT INTO blog_pages (blog_url, first_crawled, is_root)
        VALUES (%s,%s,TRUE)
    """, (blog_url, now))

    urls = fetch_sitemap_urls(blog_url) or [{"url": u} for u in fallback_discover_posts(blog_url, domain)]

    inserted = 0
    for u in urls:
        if inserted >= MAX_PAGES:
            break
        if not is_valid_post_url(u["url"], domain):
            continue
        cur.execute("""
            INSERT INTO blog_pages (blog_url, first_crawled, is_root)
            VALUES (%s,%s,FALSE)
            ON CONFLICT DO NOTHING
        """, (u["url"], now))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    return {"inserted_pages": inserted}

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
        time.sleep(random.uniform(1.2, 2.5))  # pacing
        result = extract_outbound_links(p["blog_url"])

        if result == "BLOCKED":
            blocked += 1
            cur.execute("""
                UPDATE blog_pages SET crawl_status='blocked'
                WHERE id=%s
            """, (p["id"],))
            conn.commit()
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
