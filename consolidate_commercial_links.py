import mysql.connector
from urllib.parse import urlparse

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="basant@12345",   # use your MySQL password
    database="blog_lead_crawler"
)

cursor = connection.cursor(dictionary=True)

# Fetch all commercial links
cursor.execute("""
SELECT commercial_url, is_dofollow
FROM commercial_links
""")

rows = cursor.fetchall()

if not rows:
    print("❌ No commercial links found")
    exit()

summary = {}

for row in rows:
    domain = urlparse(row["commercial_url"]).netloc.replace("www.", "")
    is_dofollow = row["is_dofollow"]

    if domain not in summary:
        summary[domain] = {
            "total": 0,
            "dofollow": 0,
            "nofollow": 0
        }

    summary[domain]["total"] += 1
    if is_dofollow:
        summary[domain]["dofollow"] += 1
    else:
        summary[domain]["nofollow"] += 1

print("\n📌 Consolidated Commercial Links Report:\n")

for domain, data in summary.items():
    dofollow_percent = round((data["dofollow"] / data["total"]) * 100, 2)
    print(
        f"{domain} | "
        f"Total Links: {data['total']} | "
        f"Dofollow: {data['dofollow']} | "
        f"Nofollow: {data['nofollow']} | "
        f"Dofollow %: {dofollow_percent}%"
    )

cursor.close()
connection.close()
