import csv
import requests
import mysql.connector
from urllib.parse import urlparse

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "basant@12345",
    "database": "blog_lead_crawler"
}

CSV_FILE = "input_blogs.csv"
TIMEOUT = 10

def is_blog_alive(url):
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={
            "User-Agent": "Mozilla/5.0 (BlogLeadCrawler)"
        })
        return r.status_code < 400
    except:
        return False

def normalize_url(url):
    url = url.strip().rstrip("/")
    if not url.startswith("http"):
        url = "https://" + url
    return url

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    added = 0
    skipped_dead = 0
    skipped_duplicate = 0

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            blog_url = normalize_url(row["blog_url"])

            # Check duplicate
            cursor.execute(
                "SELECT id FROM blogs WHERE blog_url = %s",
                (blog_url,)
            )
            if cursor.fetchone():
                skipped_duplicate += 1
                continue

            # Check alive
            if not is_blog_alive(blog_url):
                skipped_dead += 1
                continue

            cursor.execute(
                "INSERT INTO blogs (blog_url) VALUES (%s)",
                (blog_url,)
            )
            added += 1

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Blogs added:", added)
    print("⚠️ Duplicates skipped:", skipped_duplicate)
    print("❌ Dead blogs skipped:", skipped_dead)

if __name__ == "__main__":
    main()
