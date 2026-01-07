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

print("### BLOG LEAD CRAWLER — FINAL VERSION RUNNING ###")

# =========================
# APP INIT
# =========================
app = FastAPI(
    title="Blog Lead Crawler API",
    version="1.0.0"
)

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

def is_within_last_12_months(lastmod: str) -> bool:
    if not lastmod:
        return True
    try:
        post_date = datetime.fromisoformat(lastmod[:10])
        return post_date >= datetime.utcnow() - timedelta(days=365)
    except Exception:
        return True

def is_valid_post_url(url: str, domain: str) -> bool:
    if domain not in url:
        return False
    blacklist = ["/tag/", "/category/", "/author/", "/page/"]
    return not any(b in url for b in blacklist)

def extract_domain(url: str) -> str:
    return urlparse(url).netloc.lower().replace("www.", "")

def is_casino_link(url: str) -> bool:
    return any(k in url.lower() for k in CASINO_KEYWORDS)

# =========================
# SITEMAP FETCH
# =========================
def fetch_child_sitemap(sitemap_url: str) -> list:
    results = []
    try:
        r = requests.get(sitemap_url, timeout=15, verify=False)
        root = ET.fromstring(r.text)
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        for url in root.findall("ns:url", ns):
            loc = url.find("ns:loc", ns)
            lastmod = url.find("ns:lastmod", ns)
            if loc is not None:
                results.append({
                    "url": loc.text.strip(),
                    "lastmod": lastmod.text.strip() if lastmod is not None else None
                })
    except Exception:
        pass

    return results

def fetch_sitemap_urls(blog_url: str) -> list:
    sitemap_paths = ["/sitemap.xml", "/post-sitemap.xml", "/wp-sitemap.xml"]
    results = []

    for path in sitemap_paths:  # ✅ REQUIRED FIX
        try:
            r = requests.get(blog_url + path, timeout=15, verify=False)
            if r.status_code != 200:
                continue

            root = ET.fromstring(r.text)
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            sitemap_tags = root.findall("ns:sitemap", ns)
            if sitemap_tags:
                for sm in sitemap_tags:
                    loc = sm.find("ns:loc", ns)
                    if loc is not None:
                        results.extend(fetch_child_sitemap(loc.text))
                return results

            for url in root.findall("ns:url", ns):
                loc = url.find("ns:loc", ns)
                lastmod = url.find("ns:lastmod", ns)
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
# LINK EXTRACTION
# =========================
def extract_outbound_links(page_url: str):
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(page_url, headers=headers, timeout=20, verify=False)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    base_domain = urlparse(page_url).netloc
    links = []

    for a in soup.find_all("a", href=True):
        full_url = urljoin(page_url, a["href"])
        if not full_url.startswith("http"):
            continue

        domain = urlparse(full_url).netloc
        if domain == base_domain or not domain:
            continue

        rel = [r.lower() for r in a.get("rel", [])]
        is_dofollow = not any(x in rel for x in ["nofollow", "ugc", "sponsored"])

        links.append({"url": full_url, "is_dofollow": is_dofollow})

    return links

def upsert_commercial_site(cur, link_url, is_dofollow, is_casino):
    domain = extract_domain(link_url)

    cur.execute("""
        INSERT INTO commercial_sites (commercial_domain, total_links, dofollow_percent, is_casino)
        VALUES (%s, 0, 0, FALSE)
        ON CONFLICT (commercial_domain) DO NOTHING
    """, (domain,))

    cur.execute("""
        UPDATE commercial_sites
        SET total_links = total_links + 1,
            is_casino = CASE
                WHEN is_casino OR %s THEN TRUE
                ELSE FALSE
            END
        WHERE commercial_domain = %s
    """, (is_casino, domain))

# =========================
# MODELS
# =========================
class CrawlRequest(BaseModel):
    blog_url: str

# =========================
# HEALTH
# =========================
@app.get("/")
def health():
    return {"status": "ok"}

# =========================
# /crawl
# =========================
@app.post("/crawl")
def crawl_blog(data: CrawlRequest):
    blog_url = normalize_blog_url(data.blog_url)
    domain = urlparse(blog_url).netloc
    now = datetime.utcnow()

    conn = get_db()
    cur = conn.cursor()

    try:
        cur.execute("SELECT id FROM blog_pages WHERE blog_url = %s", (blog_url,))
        if cur.fetchone():
            return {"status": "already_exists", "blog": blog_url, "blog_posts_added": 0}

        cur.execute("""
            INSERT INTO blog_pages (blog_url, first_crawled, is_root)
            VALUES (%s, %s, TRUE)
        """, (blog_url, now))

        urls = fetch_sitemap_urls(blog_url)

        inserted = 0
        for item in urls:
            if inserted >= MAX_PAGES:
                break
            if not is_valid_post_url(item["url"], domain):
                continue
            if not is_within_last_12_months(item["lastmod"]):
                continue

            cur.execute("""
                INSERT INTO blog_pages (blog_url, first_crawled, is_root)
                VALUES (%s, %s, FALSE)
                ON CONFLICT (blog_url) DO NOTHING
            """, (item["url"].rstrip("/"), now))

            inserted += 1

        conn.commit()
        return {"status": "inserted", "blog": blog_url, "blog_posts_added": inserted}

    finally:
        cur.close()
        conn.close()

# =========================
# /crawl-links
# =========================
@app.post("/crawl-links")
def crawl_links(data: CrawlRequest):
    blog_url = normalize_blog_url(data.blog_url)

    conn = get_db()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT id, blog_url
            FROM blog_pages
            WHERE is_root = FALSE
              AND blog_url LIKE %s
        """, (blog_url + "%",))

        pages = cur.fetchall()
        if not pages:
            raise HTTPException(status_code=404, detail="Run /crawl first")

        saved = 0
        casino_count = 0

        for p in pages:
            links = extract_outbound_links(p["blog_url"])
            for l in links:
                casino = is_casino_link(l["url"])
                casino_count += int(casino)

                cur.execute("""
                    INSERT INTO outbound_links
                    (blog_page_id, url, is_casino, is_dofollow)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (p["id"], l["url"], casino, l["is_dofollow"]))

                upsert_commercial_site(cur, l["url"], l["is_dofollow"], casino)
                saved += 1

        conn.commit()
        return {"saved_new_links": saved, "casino_links": casino_count}

    finally:
        cur.close()
        conn.close()
