from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os, csv, io, requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse

print("### BLOG LEAD CRAWLER — SAFE SITEMAP INDEX VERSION RUNNING ###")

# =========================================================
# APP INIT
# =========================================================
app = FastAPI(title="Blog Lead Crawler API", version="1.3.7")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
# MODELS
# =========================================================
class CrawlRequest(BaseModel):
    blog_url: str

# =========================================================
# HEALTH
# =========================================================
@app.get("/")
@app.get("/health")
def health():
    return {"status": "ok"}

# =========================================================
# FIX 1 — SITEMAP INDEX + POST DISCOVERY (SAFE)
# =========================================================
def parse_sitemap(url: str, collected: set):
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return

        soup = BeautifulSoup(r.text, "xml")

        # Sitemap index → recurse
        if soup.find("sitemapindex"):
            for loc in soup.find_all("loc"):
                parse_sitemap(loc.text.strip(), collected)

        # URL set → collect posts
        if soup.find("urlset"):
            for loc in soup.find_all("loc"):
                link = loc.text.strip()
                if any(x in link for x in ["/blog/", "/post/", "/202", "/article"]):
                    collected.add(link.rstrip("/"))

    except:
        pass


def discover_blog_posts(blog_url: str):
    discovered = set()

    sitemap_urls = [
        f"{blog_url}/wp-sitemap.xml",
        f"{blog_url}/sitemap.xml"
    ]

    for sm in sitemap_urls:
        parse_sitemap(sm, discovered)

    return list(discovered)[:1000]

# =========================================================
# FIX 2 — OUTBOUND LINK EXTRACTION (UNCHANGED)
# =========================================================
def extract_outbound_links(page_url: str):
    links = []
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(page_url, timeout=10, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")
        base_domain = urlparse(page_url).netloc

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("http") and base_domain not in href:
                links.append(href)

    except:
        pass

    return links

# =========================================================
# CRAWL BLOG (ROOT + POSTS)
# =========================================================
@app.post("/crawl")
def crawl_blog(req: CrawlRequest):
    conn = get_db()
    cur = conn.cursor()

    blog_url = req.blog_url.rstrip("/")

    # Store root
    cur.execute("""
        INSERT INTO blog_pages (blog_url, is_root)
        VALUES (%s, TRUE)
        ON CONFLICT (blog_url) DO NOTHING
    """, (blog_url,))

    # Discover blog posts via sitemap index
    posts = discover_blog_posts(blog_url)
    for url in posts:
        cur.execute("""
            INSERT INTO blog_pages (blog_url, is_root)
            VALUES (%s, FALSE)
            ON CONFLICT (blog_url) DO NOTHING
        """, (url,))

    conn.commit()
    cur.close()
    conn.close()

    return {
        "status": "blog + posts stored",
        "posts_discovered": len(posts)
    }

# =========================================================
# CRAWL LINKS (ACTUAL EXTRACTION)
# =========================================================
@app.post("/crawl-links")
def crawl_links(req: CrawlRequest):
    conn = get_db()
    cur = conn.cursor()

    blog_url = req.blog_url.rstrip("/")

    cur.execute("""
        SELECT id, blog_url
        FROM blog_pages
        WHERE is_root = FALSE
        AND blog_url ILIKE %s
    """, (f"%{blog_url.replace('https://','').replace('http://','')}%",))

    pages = cur.fetchall()
    inserted = 0

    for page in pages:
        links = extract_outbound_links(page["blog_url"])
        for link in links:
            cur.execute("""
                INSERT INTO outbound_links (blog_page_id, url, is_dofollow, is_casino)
                VALUES (%s, %s, TRUE, FALSE)
                ON CONFLICT DO NOTHING
            """, (page["id"], link))
            inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    return {
        "status": "link crawling completed",
        "pages_crawled": len(pages),
        "links_found": inserted
    }

# =========================================================
# EXPORT — BLOG → PAGE → LINKS
# =========================================================
@app.get("/export/blog-page-links")
def export_blog_page_links():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            bp.blog_url,
            ol.url AS commercial_url,
            ol.is_dofollow,
            ol.is_casino
        FROM outbound_links ol
        JOIN blog_pages bp ON bp.id = ol.blog_page_id
        ORDER BY bp.blog_url
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    return StreamingResponse(buf, media_type="text/csv")

# =========================================================
# EXPORT — COMMERCIAL SITES
# =========================================================
@app.get("/export/commercial-sites")
def export_commercial_sites():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            cs.commercial_domain,
            COUNT(ol.id) AS total_links,
            ROUND(
                100.0 * SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END)
                / NULLIF(COUNT(ol.id), 0), 2
            ) AS dofollow_percent,
            BOOL_OR(ol.is_casino) AS is_casino,
            cs.meta_title,
            cs.meta_description,
            cs.homepage_checked,
            COUNT(DISTINCT root.blog_url) AS blogs_linking_count
        FROM commercial_sites cs
        JOIN outbound_links ol
          ON ol.url ILIKE '%' || cs.commercial_domain || '%'
        JOIN blog_pages bp ON bp.id = ol.blog_page_id
        JOIN blog_pages root
          ON root.is_root = TRUE
         AND bp.blog_url ILIKE '%' || replace(replace(root.blog_url,'https://',''),'http://','') || '%'
        GROUP BY
            cs.commercial_domain,
            cs.meta_title,
            cs.meta_description,
            cs.homepage_checked
        ORDER BY total_links DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    return StreamingResponse(buf, media_type="text/csv")

# =========================================================
# EXPORT — BLOG SUMMARY
# =========================================================
@app.get("/export/blog-summary")
def export_blog_summary():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            root.blog_url,
            COUNT(DISTINCT cs.commercial_domain) AS unique_commercial_sites,
            ROUND(
                100.0 * SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END)
                / NULLIF(COUNT(ol.id), 0), 2
            ) AS dofollow_percent,
            BOOL_OR(ol.is_casino) AS casino_present
        FROM blog_pages root
        JOIN blog_pages bp
          ON bp.blog_url ILIKE '%' || replace(replace(root.blog_url,'https://',''),'http://','') || '%'
        JOIN outbound_links ol ON ol.blog_page_id = bp.id
        JOIN commercial_sites cs ON ol.url ILIKE '%' || cs.commercial_domain || '%'
        WHERE root.is_root = TRUE
        GROUP BY root.blog_url
        ORDER BY root.blog_url
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    return StreamingResponse(buf, media_type="text/csv")

# =========================================================
# HISTORY
# =========================================================
@app.get("/history")
def history():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT blog_url, first_crawled, is_root
        FROM blog_pages
        ORDER BY first_crawled DESC
        LIMIT 50
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows

# =========================================================
# PROGRESS
# =========================================================
@app.get("/progress")
def progress():
    return {
        "status": "idle",
        "last_updated": datetime.utcnow().isoformat()
    }
