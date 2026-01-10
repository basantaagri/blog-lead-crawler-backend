from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os, requests, csv, io, time, random
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import urllib3
from threading import Thread
from queue import Queue
from datetime import datetime, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("### BLOG LEAD CRAWLER — SAFE PRODUCTION VERSION RUNNING ###")

# =========================================================
# HEADERS
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
# APP
# =========================================================
app = FastAPI(title="Blog Lead Crawler API", version="1.3.2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# DB
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
CASINO_KEYWORDS = [
    "casino", "bet", "betting", "poker", "slots",
    "sportsbook", "gambling", "roulette", "blackjack",
]

BLOCK_SIGNATURES = [
    "cloudflare",
    "verify you are human",
    "/cdn-cgi/",
    "access denied"
]

SOCIAL_DOMAINS = {
    "facebook.com", "twitter.com", "x.com", "linkedin.com",
    "instagram.com", "youtube.com", "t.me",
    "whatsapp.com", "bsky.app"
}

MAX_PAGES_PER_BLOG = 1000

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

# =========================================================
# PAGE DISCOVERY (UNCHANGED)
# =========================================================
def discover_blog_pages(blog_url: str, cur):
    discovered = set()
    visited = set()

    sitemap_seeds = [
        blog_url + "/sitemap.xml",
        blog_url + "/sitemap_index.xml",
        blog_url + "/wp-sitemap.xml"
    ]

    def crawl_sitemap(url):
        if url in visited or len(discovered) >= MAX_PAGES_PER_BLOG:
            return
        visited.add(url)

        try:
            r = session.get(url, timeout=20, verify=False)
            if r.status_code != 200:
                return

            soup = BeautifulSoup(r.text, "xml")

            for sm in soup.find_all("sitemap"):
                loc = sm.find("loc")
                if loc:
                    crawl_sitemap(loc.text.strip())

            for loc in soup.find_all("loc"):
                if len(discovered) >= MAX_PAGES_PER_BLOG:
                    break
                page = loc.text.strip()
                if page.startswith(blog_url):
                    discovered.add(page)

        except Exception:
            return

    for seed in sitemap_seeds:
        crawl_sitemap(seed)
        if discovered:
            break

    if not discovered:
        try:
            r = session.get(blog_url, timeout=15, verify=False)
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                full = urljoin(blog_url, a["href"])
                if extract_domain(full) == extract_domain(blog_url):
                    discovered.add(full)
                if len(discovered) >= MAX_PAGES_PER_BLOG:
                    break
        except Exception:
            pass

    for page in discovered:
        cur.execute("""
            INSERT INTO blog_pages (blog_url, is_root)
            VALUES (%s, FALSE)
            ON CONFLICT (blog_url) DO NOTHING
        """, (page,))

    return len(discovered)

# =========================================================
# LINK EXTRACTION (UNCHANGED)
# =========================================================
def extract_outbound_links(page_url: str):
    try:
        r = session.get(page_url, timeout=15, verify=False)
        html = r.text.lower()

        for sig in BLOCK_SIGNATURES:
            if sig in html:
                return "BLOCKED"

        soup = BeautifulSoup(r.text, "html.parser")
        links = []
        blog_domain = extract_domain(page_url)

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith(("mailto:", "tel:", "#", "javascript:")):
                continue

            full_url = urljoin(page_url, href)
            parsed = urlparse(full_url)
            if not parsed.netloc:
                continue

            link_domain = parsed.netloc.lower().replace("www.", "")
            if link_domain == blog_domain:
                continue

            if any(s in link_domain for s in SOCIAL_DOMAINS):
                continue

            rel = a.get("rel", [])
            is_dofollow = "nofollow" not in [r.lower() for r in rel]

            links.append({
                "url": full_url,
                "is_dofollow": is_dofollow
            })

        return links
    except Exception:
        return []

def upsert_commercial_site(cur, url: str, is_casino: bool):
    domain = extract_domain(url)
    if not domain:
        return

    cur.execute("""
        INSERT INTO commercial_sites
        (commercial_domain, total_links, dofollow_percent, is_casino)
        VALUES (%s, 1, 100.0, %s)
        ON CONFLICT (commercial_domain)
        DO UPDATE SET
            total_links = commercial_sites.total_links + 1,
            is_casino = commercial_sites.is_casino OR EXCLUDED.is_casino
    """, (domain, is_casino))

# =========================================================
# QUEUE WORKER
# =========================================================
crawl_queue = Queue()

def crawl_worker():
    while True:
        blog_url = crawl_queue.get()
        try:
            crawl_links(CrawlRequest(blog_url=blog_url))
        finally:
            crawl_queue.task_done()

Thread(target=crawl_worker, daemon=True).start()

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

@app.post("/crawl")
def crawl_blog(data: CrawlRequest):
    blog_url = normalize_blog_url(data.blog_url)
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO blog_pages (blog_url, is_root)
        VALUES (%s, TRUE)
        ON CONFLICT (blog_url) DO NOTHING
    """, (blog_url,))

    pages = discover_blog_pages(blog_url, cur)

    conn.commit()
    cur.close()
    conn.close()

    return {
        "status": "blog registered",
        "blog": blog_url,
        "pages_discovered": pages
    }

@app.post("/crawl-links")
def crawl_links(data: CrawlRequest):
    blog_url = normalize_blog_url(data.blog_url)
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, blog_url
        FROM blog_pages
        WHERE is_root = FALSE
          AND blog_url ILIKE %s
        LIMIT 1000
    """, ("%" + extract_domain(blog_url) + "%",))

    pages = cur.fetchall()
    if not pages:
        raise HTTPException(404, "No pages found. Run crawl first.")

    for p in pages:
        time.sleep(random.uniform(0.8, 1.5))
        links = extract_outbound_links(p["blog_url"])
        if links == "BLOCKED":
            continue

        for l in links:
            is_c = is_casino_link(l["url"])
            cur.execute("""
                INSERT INTO outbound_links
                (blog_page_id, url, is_casino, is_dofollow)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (p["id"], l["url"], is_c, l["is_dofollow"]))
            upsert_commercial_site(cur, l["url"], is_c)

        conn.commit()

    cur.close()
    conn.close()
    return {"status": "completed"}

# =========================================================
# EXPORTS (FIXED BLOG SUMMARY ONLY)
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
                / NULLIF(COUNT(ol.id), 0),
                2
            ) AS dofollow_percent,
            BOOL_OR(ol.is_casino) AS has_casino_links
        FROM blog_pages root
        JOIN blog_pages bp
          ON bp.blog_url ILIKE '%' || extract_domain(root.blog_url) || '%'
         AND bp.is_root = FALSE
        JOIN outbound_links ol ON ol.blog_page_id = bp.id
        JOIN commercial_sites cs ON cs.commercial_domain = extract_domain(ol.url)
        WHERE root.is_root = TRUE
        GROUP BY root.blog_url
        ORDER BY root.blog_url
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv")
