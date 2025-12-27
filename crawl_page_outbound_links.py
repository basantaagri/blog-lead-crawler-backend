import requests
import mysql.connector
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

HEADERS = {
    "User-Agent": "Mozilla/5.0 (BlogLeadCrawler/1.0)"
}

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "basant@12345",
    "database": "blog_lead_crawler"
}

conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(dictionary=True)

cursor.execute("""
    SELECT bp.blog_id, bp.page_url, b.blog_url
    FROM blog_pages bp
    JOIN blogs b ON b.id = bp.blog_id
""")

pages = cursor.fetchall()
print(f"🔍 Pages to crawl: {len(pages)}")

inserted = 0

for row in pages:
    blog_id = row["blog_id"]
    page_url = row["page_url"]
    blog_domain = urlparse(row["blog_url"]).netloc

    try:
        r = requests.get(page_url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            continue
    except:
        continue

    soup = BeautifulSoup(r.text, "html.parser")

    for a in soup.find_all("a", href=True):
        full_url = urljoin(page_url, a["href"])
        parsed = urlparse(full_url)

        if not parsed.netloc or parsed.netloc == blog_domain:
            continue

        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        cursor.execute("""
            SELECT id FROM page_outbound_links
            WHERE page_url=%s AND outbound_url=%s
        """, (page_url, clean_url))

        if cursor.fetchone():
            continue

        cursor.execute("""
            INSERT INTO page_outbound_links (blog_id, page_url, outbound_url)
            VALUES (%s, %s, %s)
        """, (blog_id, page_url, clean_url))

        inserted += 1

conn.commit()
print(f"✅ Outbound links inserted: {inserted}")

cursor.close()
conn.close()
