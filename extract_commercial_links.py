import requests
import mysql.connector
from bs4 import BeautifulSoup
from urllib.parse import urlparse

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="basant@12345",   # your MySQL password
    database="blog_lead_crawler"
)

cursor = connection.cursor(dictionary=True)

# Get pages to scan
cursor.execute("""
SELECT bp.id AS page_id, bp.page_url, b.blog_url
FROM blog_pages bp
JOIN blogs b ON bp.blog_id = b.id
""")

pages = cursor.fetchall()

if not pages:
    print("❌ No pages found to scan")
    exit()

print(f"🔍 Scanning {len(pages)} pages for commercial links")

for page in pages:
    page_id = page["page_id"]
    page_url = page["page_url"]
    blog_domain = urlparse(page["blog_url"]).netloc

    try:
        r = requests.get(page_url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception:
        continue

    for a in soup.find_all("a", href=True):
        href = a["href"]
        rel = a.get("rel", [])

        parsed = urlparse(href)

        # Only external links
        if parsed.netloc and parsed.netloc != blog_domain:
            is_dofollow = 0 if "nofollow" in rel else 1

            cursor.execute("""
                INSERT INTO commercial_links (page_id, commercial_url, is_dofollow)
                VALUES (%s, %s, %s)
            """, (page_id, href, is_dofollow))

connection.commit()
cursor.close()
connection.close()

print("✅ Commercial links extracted and saved")
