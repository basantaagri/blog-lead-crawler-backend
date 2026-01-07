from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("### BLOG LEAD CRAWLER — HARD FAIL SAFE VERSION RUNNING ###")

# =========================
# APP INIT
# =========================
app = FastAPI(title="Blog Lead Crawler API", version="1.0.0")

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
    blacklist = ["/tag/", "/category/", "/author/", "/page/"]
    return domain in url and not any(b in url for b in blacklist)

# =========================
# SITEMAP
# =========================
def fetch_child_sitemap(sitemap_url: str) -> list:
    items = []
    try:
        r = requests.get(sitemap_url, timeout=15, verify=False)
        root = ET.fromstring(r.text)
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for u in root.findall("ns:url", ns):
            loc = u.find("ns:loc", ns)
            lastmod = u.find("ns:lastmod", ns)
            if loc is not None:
                items.append({
                    "url": loc.text.strip(),
                    "lastmod": lastmod.text.strip() if lastmod is not None else None
                })
    except Exception:
        pass
    return items

def fetch_sitemap_urls(blog_url: str) -> list:
    sitemap_paths = ["/sitemap.xml", "/post-sitemap.xml", "/wp-sitemap.xml"]
    results = []

    for path in sitemap_paths:
        try:
            r = requests.get(blog_url + path, timeout=15, verify=False)
            if r.status_code != 200:
                continue

            root = ET.fromstring(r.text)
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            sitemaps = root.findall("ns:sitemap", ns)
            if sitemaps:
                for sm in sitemaps:
                    loc = sm.find("ns:loc", ns)
                    if loc is not None:
                        results.extend(fetch_child_sitemap(loc.text))
                return results

            for u in root.findall("ns:url", ns):
                loc = u.find("ns:loc", ns)
                lastmod = u.find("ns:lastmod", ns)
                if loc is not None:
                    results.append({
                        "url": loc.text.strip(),
                        "lastmod": lastmod.text.strip() if lastmod is not None else None
                    })

            if results:
                return results
        except Exception:
            continue

    return results

# =========================
# FALLBACK HTML DISCOVERY
# =========================
def fallback_discover_posts(blog_url: str, domain: str, limit=100):
    found = []
    try:
        r = requests.get(blog_url, timeout=20, verify=False)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.find_all("a", href=True):
            full = urljoin(blog_url, a["href"])
            if domain in full and any(x in full for x in ["/blog/", "/post/", "/202"]):
                found.append(full.rstrip("/"))
            if len(found) >= limit:
                break
    except Exception:
        pass

    return list(set(found))

# =========================
# OUTBOUND LINKS
# =========================
def extract_outbound_links(page_url: str) -> list:
    links = []
    try:
        r = requests.get(page_url, timeout=15, verify=False)
        if not r.headers.get("content-type", "").startswith("text/html"):
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        base_domain = urlparse(page_url).netloc

        for a in soup.find_all("a", href=True):
            full = urljoin(page_url, a["href"])
            if not full.startswith("http"):
                continue
            if urlparse(full).netloc == base_domain:
                continue

            rel = [x.lower() for x in a.get("rel", [])]
            is_dofollow = not any(x in rel for x in ["nofollow", "ugc", "sponsored"])

            links.append({
                "url": full,
                "is_dofollow": is_dofollow
            })
    except Exception:
        return []

    return list({l["url"]: l for l in links}.values())

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
        return {"status": "already_exists"}

    cur.execute("""
        INSERT INTO blog_pages (blog_url, first_crawled, is_root)
        VALUES (%s,%s,TRUE)
    """, (blog_url, now))

    urls = fetch_sitemap_urls(blog_url)
    if not urls:
        urls = [{"url": u} for u in fallback_discover_posts(blog_url, domain)]

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

# =========================
# 🔴 FIXED ENDPOINT
# =========================
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

    saved = casino = 0

    for p in pages:
        try:
            links = extract_outbound_links(p["blog_url"])
            if not links:
                continue

            for l in links:
                try:
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

                except Exception:
                    conn.rollback()
                    continue

        except Exception:
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()

    return {"saved_links": saved, "casino_links": casino}
