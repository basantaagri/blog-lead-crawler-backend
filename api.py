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

print("### BLOG LEAD CRAWLER — SAFE WP REST FALLBACK VERSION RUNNING ###")

# =========================================================
# APP INIT
# =========================================================
app = FastAPI(title="Blog Lead Crawler API", version="1.3.8")

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
# SITEMAP PARSER
# =========================================================
def parse_sitemap(url: str, collected: set):
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return

        soup = BeautifulSoup(r.text, "xml")

        if soup.find("sitemapindex"):
            for loc in soup.find_all("loc"):
                parse_sitemap(loc.text.strip(), collected)

        if soup.find("urlset"):
            for loc in soup.find_all("loc"):
                link = loc.text.strip()
                if any(x in link for x in ["/blog/", "/post/", "/202", "/article"]):
                    collected.add(link.rstrip("/"))

    except:
        pass

# =========================================================
# WP REST API FALLBACK (CRITICAL FIX)
# =========================================================
def discover_posts_via_wp_api(blog_url: str):
    posts = set()
    try:
        api = f"{blog_url}/wp-json/wp/v2/posts?per_page=100"
        r = requests.get(api, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            for post in r.json():
                if "link" in post:
                    posts.add(post["link"].rstrip("/"))
    except:
        pass
    return posts

# =========================================================
# BLOG POST DISCOVERY (SAFE)
# =========================================================
def discover_blog_posts(blog_url: str):
    discovered = set()

    sitemap_urls = [
        f"{blog_url}/wp-sitemap.xml",
        f"{blog_url}/sitemap.xml"
    ]

    for sm in sitemap_urls:
        parse_sitemap(sm, discovered)

    # 🔥 FALLBACK — WordPress REST API
    if not discovered:
        discovered |= discover_posts_via_wp_api(blog_url)

    return list(discovered)[:1000]

# =========================================================
# OUTBOUND LINK EXTRACTION
# =========================================================
def extract_outbound_links(page_url: str):
    links = []
    try:
        r = requests.get(page_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
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

    cur.execute("""
        INSERT INTO blog_pages (blog_url, is_root)
        VALUES (%s, TRUE)
        ON CONFLICT (blog_url) DO NOTHING
    """, (blog_url,))

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
# CRAWL LINKS
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
# EXPORTS (UNCHANGED)
# =========================================================
@app.get("/export/blog-page-links")
def export_blog_page_links():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT bp.blog_url, ol.url AS commercial_url, ol.is_dofollow, ol.is_casino
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

@app.get("/export/commercial-sites")
def export_commercial_sites():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT cs.commercial_domain,
               COUNT(ol.id) AS total_links,
               ROUND(100.0 * SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END) / NULLIF(COUNT(ol.id),0),2) AS dofollow_percent,
               BOOL_OR(ol.is_casino) AS is_casino,
               cs.meta_title, cs.meta_description, cs.homepage_checked,
               COUNT(DISTINCT root.blog_url) AS blogs_linking_count
        FROM commercial_sites cs
        JOIN outbound_links ol ON ol.url ILIKE '%' || cs.commercial_domain || '%'
        JOIN blog_pages bp ON bp.id = ol.blog_page_id
        JOIN blog_pages root ON root.is_root = TRUE
         AND bp.blog_url ILIKE '%' || replace(replace(root.blog_url,'https://',''),'http://','') || '%'
        GROUP BY cs.commercial_domain, cs.meta_title, cs.meta_description, cs.homepage_checked
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

@app.get("/export/blog-summary")
def export_blog_summary():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT root.blog_url,
               COUNT(DISTINCT cs.commercial_domain) AS unique_commercial_sites,
               ROUND(100.0 * SUM(CASE WHEN ol.is_dofollow THEN 1 ELSE 0 END) / NULLIF(COUNT(ol.id),0),2) AS dofollow_percent,
               BOOL_OR(ol.is_casino) AS casino_present
        FROM blog_pages root
        JOIN blog_pages bp ON bp.blog_url ILIKE '%' || replace(replace(root.blog_url,'https://',''),'http://','') || '%'
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

@app.get("/progress")
def progress():
    return {
        "status": "idle",
        "last_updated": datetime.utcnow().isoformat()
    }
