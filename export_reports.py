import csv
import mysql.connector
from urllib.parse import urlparse

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="basant@12345",   # change if your password is different
    database="blog_lead_crawler"
)

cursor = connection.cursor(dictionary=True)

# =========================
# OUTPUT FILE 1
# Page-level commercial links
# =========================
cursor.execute("""
SELECT
    b.domain AS blog_domain,
    bp.page_url,
    cl.commercial_url,
    cl.is_dofollow
FROM commercial_links cl
JOIN blog_pages bp ON cl.page_id = bp.id
JOIN blogs b ON bp.blog_id = b.id
""")

rows = cursor.fetchall()

with open("output_1_page_level_links.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["blog_domain", "page_url", "commercial_url", "is_dofollow"])

    for r in rows:
        writer.writerow([
            r["blog_domain"],
            r["page_url"],
            r["commercial_url"],
            "dofollow" if r["is_dofollow"] else "nofollow"
        ])

print("✅ Output 1 generated")

# =========================
# OUTPUT FILE 2
# Consolidated commercial sites
# =========================
cursor.execute("""
SELECT commercial_url, is_dofollow
FROM commercial_links
""")

rows = cursor.fetchall()
summary = {}

for r in rows:
    domain = urlparse(r["commercial_url"]).netloc.replace("www.", "")

    if domain not in summary:
        summary[domain] = {"total": 0, "dofollow": 0}

    summary[domain]["total"] += 1
    if r["is_dofollow"]:
        summary[domain]["dofollow"] += 1

with open("output_2_consolidated_commercial_sites.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["commercial_domain", "total_links", "dofollow_percent", "nofollow_percent"])

    for domain, data in summary.items():
        dofollow_pct = round((data["dofollow"] / data["total"]) * 100, 2)
        nofollow_pct = 100 - dofollow_pct
        writer.writerow([domain, data["total"], dofollow_pct, nofollow_pct])

print("✅ Output 2 generated")

# =========================
# OUTPUT FILE 3
# Blog-level summary
# =========================
cursor.execute("""
SELECT
    b.blog_url,
    cl.commercial_url,
    cl.is_dofollow
FROM commercial_links cl
JOIN blog_pages bp ON cl.page_id = bp.id
JOIN blogs b ON bp.blog_id = b.id
""")

rows = cursor.fetchall()
blogs = {}

for r in rows:
    blog = r["blog_url"]
    domain = urlparse(r["commercial_url"]).netloc.replace("www.", "")

    if blog not in blogs:
        blogs[blog] = {
            "domains": set(),
            "total": 0,
            "dofollow": 0
        }

    blogs[blog]["domains"].add(domain)
    blogs[blog]["total"] += 1
    if r["is_dofollow"]:
        blogs[blog]["dofollow"] += 1

with open("output_3_blog_summary.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "blog_url",
        "unique_commercial_domains",
        "dofollow_percent",
        "casino_related"
    ])

    for blog, data in blogs.items():
        dofollow_pct = round((data["dofollow"] / data["total"]) * 100, 2)
        writer.writerow([
            blog,
            len(data["domains"]),
            dofollow_pct,
            "unknown"
        ])

print("✅ Output 3 generated")

cursor.close()
connection.close()
