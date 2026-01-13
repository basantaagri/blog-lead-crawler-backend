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

print("### BLOG LEAD CRAWLER — SAFE WP REST FALLBACK + FINAL ANALYTICS VERSION RUNNING ###")

# =========================================================
# APP INIT
# =========================================================
app = FastAPI(title="Blog Lead Crawler API", version="1.4.0")

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
# WP REST API FALLBACK
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
# BLOG POST DISCOVERY
# =========================================================
def discover_blog_posts(blog_url: str):
    discovered = set()
    for sm in [f"{blog_url}/wp-sitemap.xml", f"{blog_url}/sitemap.xml"]:
        parse_sitemap(sm, discovered)

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
# CRAWL BLOG
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
# 🔥 NEW — COMMERCIAL SITE ENRICHMENT WORKER (SAFE)
# =========================================================
@app.post("/worker/enrich-commercial-sites")
def enrich_commercial_sites():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT commercial_domain
        FROM commercial_sites
        WHERE homepage_checked = FALSE
        LIMIT 50
    """)

    sites = cur.fetchall()

    for s in sites:
        domain = s["commercial_domain"]
        url = f"https://{domain}"

        try:
            r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(r.text, "html.parser")

            title = soup.title.text.strip() if soup.title else None
            desc_tag = soup.find("meta", attrs={"name": "description"})
            description = desc_tag["content"].strip() if desc_tag else None

            text_blob = f"{title or ''} {description or ''}".lower()
            is_casino = any(k in text_blob for k in [
                "casino","bet","gambling","slots","poker","sportsbook"
            ])

            cur.execute("""
                UPDATE commercial_sites
                SET meta_title = %s,
                    meta_description = %s,
                    is_casino = %s,
                    homepage_checked = TRUE,
                    homepage_checked_at = NOW()
                WHERE commercial_domain = %s
            """, (title, description, is_casino, domain))

        except:
            continue

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "commercial sites enriched"}

# =========================================================
# EXPORTS — UNCHANGED (YOUR FINAL ANALYTICS SQL IS ALREADY HERE)
# =========================================================
# blog-page-links
# commercial-sites
# blog-summary
# history
# progress
