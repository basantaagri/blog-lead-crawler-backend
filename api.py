from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import os, requests, csv, io, time, random
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("### BLOG LEAD CRAWLER — HARD FAIL SAFE VERSION RUNNING ###")

# =========================================================
# GLOBAL HEADERS
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
# APP INIT
# =========================================================
app = FastAPI(title="Blog Lead Crawler API", version="1.2.5")

# =========================================================
# CORS
# =========================================================
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
# REQUIRED HELPERS
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
# HOMEPAGE ENRICHMENT
# =========================================================
def fetch_homepage_meta(domain: str):
    try:
        r = session.get("https://" + domain, timeout=15, verify=False)
        if r.status_code != 200:
            return None, None, None

        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.title.string.strip() if soup.title else ""
        desc_tag = soup.find("meta", attrs={"name": "description"})
        desc = desc_tag["content"].strip() if desc_tag else ""
        text = soup.get_text(separator=" ").lower()
        return title, desc, text
    except:
        return None, None, None

def detect_casino_from_text(text: str):
    return any(k in text for k in CASINO_KEYWORDS) if text else False

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

@app.post("/crawl-links")
def crawl_links(data: CrawlRequest):
    blog_url = normalize_blog_url(data.blog_url)
    conn = get_db()
    cur = conn.cursor()

    # 🟢 STEP 1 — BATCH LIMIT APPLIED
    cur.execute("""
        SELECT id, blog_url
        FROM blog_pages
        WHERE is_root = FALSE
        AND blog_url LIKE %s
        LIMIT 10
    """, (blog_url + "%",))

    pages = cur.fetchall()
    if not pages:
        raise HTTPException(404, "Run /crawl first")

    saved = casino = blocked = 0

    for p in pages:
        time.sleep(random.uniform(1, 2))
        result = extract_outbound_links(p["blog_url"])

        if result == "BLOCKED":
            blocked += 1
            continue

        for l in result:
            is_c = is_casino_link(l["url"])
            cur.execute("""
                INSERT INTO outbound_links
                (blog_page_id, url, is_casino, is_dofollow)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (p["id"], l["url"], is_c, l["is_dofollow"]))

            if cur.fetchone():
                upsert_commercial_site(cur, l["url"], is_c)

                domain = extract_domain(l["url"])

                # 🟢 STEP 2 — SKIP ALREADY ENRICHED
                cur.execute("""
                    SELECT homepage_checked
                    FROM commercial_sites
                    WHERE commercial_domain = %s
                    AND homepage_checked = FALSE
                """, (domain,))
                cs = cur.fetchone()

                if cs:
                    title, desc, text = fetch_homepage_meta(domain)
                    homepage_is_casino = detect_casino_from_text(
                        f"{title or ''} {desc or ''} {text or ''}"
                    )
                    cur.execute("""
                        UPDATE commercial_sites
                        SET
                            meta_title = %s,
                            meta_description = %s,
                            is_casino = %s,
                            homepage_checked = TRUE
                        WHERE commercial_domain = %s
                    """, (title, desc, homepage_is_casino, domain))

                saved += 1
                casino += int(is_c)

        conn.commit()

    cur.close()
    conn.close()

    return {
        "pages_scanned": len(pages),
        "saved_links": saved,
        "casino_links": casino,
        "blocked_pages": blocked
    }
