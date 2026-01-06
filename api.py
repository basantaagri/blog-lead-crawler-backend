from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.errors import UniqueViolation
import os

# Crawling imports
import requests
from bs4 import BeautifulSoup
import tldextract
from urllib.parse import urljoin
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# APP INIT
# =========================
app = FastAPI(
    title="Blog Lead Crawler API",
    version="1.0.0"
)

# =========================
# ✅ CORS (PRODUCTION FIXED)
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
CASINO_KEYWORDS = [
    "casino", "bet", "betting", "poker", "slots", "sportsbook",
    "wager", "odds", "gambling", "roulette", "blackjack"
]

# =========================
# HEALTH
# =========================
@app.get("/")
def health():
    return {"status": "ok"}

# =========================
# HISTORY (USED BY FRONTEND)
# =========================
@app.get("/history")
def last_30_days():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, blog_url, first_crawled
            FROM blog_pages
            WHERE first_crawled >= %s
            ORDER BY first_crawled DESC
        """, (datetime.utcnow() - timedelta(days=30),))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

# =========================
# MODELS
# =========================
class CrawlRequest(BaseModel):
    blog_url: str

# =========================
# INSERT BLOG
# =========================
@app.post("/crawl")
def crawl_blog(data: CrawlRequest):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO blog_pages (blog_url, first_crawled)
            VALUES (%s, %s)
            RETURNING id
        """, (data.blog_url, datetime.utcnow()))
        blog_id = cur.fetchone()["id"]
        conn.commit()
        return {"status": "inserted", "id": blog_id}
    except UniqueViolation:
        conn.rollback()
        raise HTTPException(status_code=409, detail="Blog URL already exists")
    finally:
        cur.close()
        conn.close()

# =========================
# LINK EXTRACTION
# =========================
def extract_outbound_links(page_url: str):
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(page_url, headers=headers, timeout=20, verify=False)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    base_domain = tldextract.extract(page_url).registered_domain
    links = []

    for a in soup.find_all("a", href=True):
        full_url = urljoin(page_url, a["href"])
        if not full_url.startswith("http"):
            continue

        domain = tldextract.extract(full_url).registered_domain
        if domain == base_domain or not domain:
            continue

        rel = [r.lower() for r in a.get("rel", [])]
        is_dofollow = not any(x in rel for x in ["nofollow", "ugc", "sponsored"])

        links.append({
            "url": full_url,
            "is_dofollow": is_dofollow
        })

    return links

def is_casino_link(url: str) -> bool:
    return any(k in url.lower() for k in CASINO_KEYWORDS)

# =========================
# CRAWL LINKS
# =========================
@app.post("/crawl-links")
def crawl_links(data: CrawlRequest):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM blog_pages WHERE blog_url=%s",
            (data.blog_url,)
        )
        blog = cur.fetchone()
        if not blog:
            raise HTTPException(status_code=404, detail="Insert blog first")

        blog_id = blog["id"]
        links = extract_outbound_links(data.blog_url)

        saved = 0
        casino_count = 0

        for l in links:
            casino = is_casino_link(l["url"])
            casino_count += int(casino)
            try:
                cur.execute("""
                    INSERT INTO outbound_links
                    (blog_page_id, url, is_casino, is_dofollow)
                    VALUES (%s,%s,%s,%s)
                """, (blog_id, l["url"], casino, l["is_dofollow"]))
                saved += 1
            except Exception:
                conn.rollback()

        conn.commit()
        return {
            "total_outbound_links": len(links),
            "saved_new_links": saved,
            "casino_links_count": casino_count
        }
    finally:
        cur.close()
        conn.close()

# =========================
# LEAD SCORE (USED BY FRONTEND)
# =========================
@app.get("/blog-lead-score/{blog_id}")
def blog_lead_score(blog_id: int):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE is_casino) AS casino,
                COUNT(*) FILTER (WHERE is_dofollow) AS dofollow
            FROM outbound_links
            WHERE blog_page_id=%s
        """, (blog_id,))
        r = cur.fetchone()

        total = r["total"] or 0
        casino_pct = (r["casino"] / total) * 100 if total else 0
        dofollow_pct = (r["dofollow"] / total) * 100 if total else 0

        score = round(
            (60 if casino_pct == 0 else max(0, 60 * (1 - casino_pct / 20))) +
            (dofollow_pct / 100) * 30 +
            (10 if 5 <= total <= 50 else 5),
            2
        )

        return {
            "blog_id": blog_id,
            "total_links": total,
            "casino_percentage": round(casino_pct, 2),
            "dofollow_percentage": round(dofollow_pct, 2),
            "lead_score": score
        }
    finally:
        cur.close()
        conn.close()
