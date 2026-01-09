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
app = FastAPI(title="Blog Lead Crawler API", version="1.2.7")

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
    "sportsbook", "gambling", "roulette", "blackjack"
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

def is_valid_commercial_link(href: str, blog_domain: str) -> bool:
    if not href:
        return False

    href = href.strip().lower()

    if href.startswith(("mailto:", "tel:", "#", "javascript:")):
        return False

    parsed = urlparse(href)
    if not parsed.netloc:
        return False

    link_domain = parsed.netloc.replace("www.", "")
    blog_domain = blog_domain.replace("www.", "")

    if link_domain == blog_domain:
        return False

    for s in SOCIAL_DOMAINS:
        if s in link_domain:
            return False

    return True

# =========================================================
# LINK EXTRACTION
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
            full_url = urljoin(page_url, a["href"])

            if not is_valid_commercial_link(full_url, blog_domain):
                continue

            commercial_domain = extract_domain(full_url)
            if not commercial_domain:
                continue

            rel = a.get("rel", [])
            is_dofollow = "nofollow" not in [r.lower() for r in rel]

            links.append({
                "url": full_url,
                "is_dofollow": is_dofollow
            })

        return links
    except:
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
# CSV
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
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "blog registered", "blog": blog_url}

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
        LIMIT 20
    """, (blog_url + "%",))

    pages = cur.fetchall()
    if not pages:
        raise HTTPException(404, "No pages found. Run crawl first.")

    for p in pages:
        time.sleep(random.uniform(1.0, 2.0))
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
# HISTORY
# =========================================================
@app.get("/history")
def history():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            root.blog_url,
            COUNT(DISTINCT bp.id) AS total_pages,
            COUNT(DISTINCT cs.commercial_domain) AS unique_commercial_sites,
            ROUND(
                100.0 * SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END)
                / NULLIF(COUNT(ol.id), 0), 2
            ) AS dofollow_percentage,
            BOOL_OR(ol.is_casino) AS has_casino_links
        FROM blog_pages root
        LEFT JOIN blog_pages bp
          ON bp.blog_url LIKE root.blog_url || '%'
         AND bp.is_root = FALSE
        LEFT JOIN outbound_links ol ON ol.blog_page_id = bp.id
        LEFT JOIN commercial_sites cs
          ON ol.url LIKE '%' || cs.commercial_domain || '%'
        WHERE root.is_root = TRUE
        GROUP BY root.blog_url
        ORDER BY root.blog_url
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# =========================================================
# EXPORTS
# =========================================================
@app.get("/export/blog-page-links")
def export_blog_page_links():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            root.blog_url AS blog,
            bp.blog_url AS blog_page,
            ol.url AS commercial_link,
            ol.is_dofollow,
            ol.is_casino
        FROM blog_pages root
        JOIN blog_pages bp
          ON bp.blog_url LIKE root.blog_url || '%'
         AND bp.is_root = FALSE
        JOIN outbound_links ol ON ol.blog_page_id = bp.id
        WHERE root.is_root = TRUE
        ORDER BY root.blog_url, bp.blog_url
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows_to_csv(rows)

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
        JOIN outbound_links ol ON ol.url LIKE '%' || cs.commercial_domain || '%'
        JOIN blog_pages bp ON bp.id = ol.blog_page_id
        JOIN blog_pages root
          ON root.is_root = TRUE
         AND bp.blog_url LIKE root.blog_url || '%'
        GROUP BY cs.commercial_domain, cs.total_links, cs.dofollow_percent, cs.is_casino
        ORDER BY cs.total_links DESC
    """)
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
        JOIN outbound_links ol ON ol.blog_page_id = bp.id
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
# PROGRESS
# =========================================================
@app.get("/progress")
def progress():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            root.blog_url,
            COALESCE(root.crawl_status, 'pending') AS crawl_status,
            COUNT(bp.id) AS pages_discovered
        FROM blog_pages root
        LEFT JOIN blog_pages bp
          ON bp.blog_url LIKE root.blog_url || '%'
         AND bp.is_root = FALSE
        WHERE root.is_root = TRUE
        GROUP BY root.blog_url, root.crawl_status
        ORDER BY root.blog_url
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# =========================================================
# ASYNC
# =========================================================
@app.post("/crawl-async")
def crawl_async(data: CrawlRequest):
    crawl_queue.put(data.blog_url)
    return {"status": "queued", "blog": data.blog_url}
