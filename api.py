from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os, requests, csv, io, time, random
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import urllib3

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
app = FastAPI(title="Blog Lead Crawler API", version="1.2.6")

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

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("#") or href.startswith("mailto:"):
                continue

            full_url = urljoin(page_url, href)
            if extract_domain(full_url) != extract_domain(page_url):
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
# ✅ NEW ENDPOINT — CSV BULK CRAWL (APPENDED ONLY)
# =========================================================
@app.post("/crawl-csv")
def crawl_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    content = file.file.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))

    blogs = []
    for row in reader:
        blog_url = row.get("blog_url")
        if blog_url:
            blogs.append(blog_url.strip())

    if not blogs:
        raise HTTPException(status_code=400, detail="CSV must contain blog_url column")

    results = []

    for blog in blogs:
        try:
            crawl_blog(CrawlRequest(blog_url=blog))
            crawl_links(CrawlRequest(blog_url=blog))
            results.append({"blog": blog, "status": "processed"})
        except:
            results.append({"blog": blog, "status": "failed"})

    return {
        "total": len(blogs),
        "processed": results
    }
