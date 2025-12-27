import mysql.connector
from urllib.parse import urlparse

# CHANGE THIS TO ANY BLOG YOU WANT TO TEST
BLOG_URL = "https://example.com"

# Extract domain
parsed = urlparse(BLOG_URL)
domain = parsed.netloc.replace("www.", "")

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="basant@12345",   # use your MySQL password
    database="blog_lead_crawler"
)

cursor = connection.cursor()

# Insert blog (ignore if already exists)
cursor.execute("""
INSERT IGNORE INTO blogs (blog_url, domain)
VALUES (%s, %s)
""", (BLOG_URL, domain))

connection.commit()

print("✅ Blog inserted (or already exists)")

# Read blogs back
cursor.execute("SELECT id, blog_url, domain, first_crawled FROM blogs")

rows = cursor.fetchall()

print("\n📌 Blogs in database:")
for row in rows:
    print(row)

cursor.close()
connection.close()
