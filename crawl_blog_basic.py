import requests
import mysql.connector
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime

# ---------------- CONFIG ----------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (BlogLeadCrawler/1.0)"
}

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "basant@12345",
    "database": "blog_lead_crawler"
}

# ---------------- DB CONNECT ----------------
connection = mysql.connector.connect(**DB_CONFIG)
cursor = connection.cursor(dictionary=True)

# ---------------- FETCH ONE BLOG ----------------
cursor.execute("SELECT id, blog_url FROM blogs LIMIT 1")
blog = cursor.fetchone()

if not blog:
    print("❌ No blog found in database")
    exit()

blog_id = blog["id"]
blog_url = blog["blog_url"].rstrip("/")

print(f"🔍 Crawling blog homepage: {blog_url}")

# ---------------- FETCH HOMEPAGE ----------------
try:
    response = requests.get(blog_url, headers=HEADERS, timeout=15)
    response.raise_for_status()
except Exception as e:
    print("❌ Failed to fetch homepage:", e)
    exit()

soup = BeautifulSoup(response.text, "html.parser")

# ---------------- EXTRACT INTERNAL LINKS ----------------
base_netloc = urlparse(blog_url).netloc
internal_pages = set()

for a in soup.find_all("a", href=True):
    href = a["href"].strip()
    full_url = urljoin(blog_url, href)
    parsed = urlparse(full_url)

    # Only internal URLs
    if parsed.netloc != base_netloc:
        continue

    # Skip homepage root
    if parsed.path in ["", "/"]:
        continue

    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
    internal_pages.add(clean_url)

print(f"📄 Found {len(internal_pages)} internal pages")

# ---------------- INSERT INTO DB ----------------
inserted = 0
today = datetime.now().date()

for page_url in internal_pages:
    cursor.execute("""
        SELECT id FROM blog_pages 
        WHERE blog_id = %s AND page_url = %s
    """, (blog_id, page_url))

    if cursor.fetchone():
        continue

    cursor.execute("""
        INSERT INTO blog_pages (blog_id, page_url, post_date)
        VALUES (%s, %s, %s)
    """, (blog_id, page_url, today))

    inserted += 1

connection.commit()

print(f"✅ Inserted {inserted} new pages")

cursor.close()
connection.close()
