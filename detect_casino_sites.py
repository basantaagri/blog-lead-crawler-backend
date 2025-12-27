import requests
import mysql.connector
from bs4 import BeautifulSoup
from urllib.parse import urlparse

CASINO_KEYWORDS = [
    "casino", "bet", "betting", "gambling", "slots",
    "poker", "roulette", "blackjack", "jackpot",
    "sportsbook", "wager", "odds", "bonus", "free spins"
]

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="basant@12345",
    database="blog_lead_crawler"
)

cursor = connection.cursor(dictionary=True)

# Get unique commercial domains
cursor.execute("""
SELECT DISTINCT commercial_url
FROM commercial_links
""")

urls = cursor.fetchall()

print(f"🔍 Checking {len(urls)} commercial sites for casino content")

for row in urls:
    domain = urlparse(row["commercial_url"]).netloc.replace("www.", "")
    homepage = f"https://{domain}"

    try:
        r = requests.get(homepage, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception:
        continue

    text = ""
    if soup.title:
        text += soup.title.get_text().lower() + " "

    desc = soup.find("meta", attrs={"name": "description"})
    if desc and desc.get("content"):
        text += desc["content"].lower() + " "

    body_text = soup.get_text(" ", strip=True).lower()
    text += body_text[:3000]  # limit size

    matched = [k for k in CASINO_KEYWORDS if k in text]
    is_casino = 1 if matched else 0

    cursor.execute("""
        INSERT INTO commercial_site_analysis
        (commercial_domain, is_casino, matched_keywords)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            is_casino = VALUES(is_casino),
            matched_keywords = VALUES(matched_keywords)
    """, (domain, is_casino, ", ".join(matched)))

    print(f"{domain} → {'CASINO' if is_casino else 'NON-CASINO'}")

connection.commit()
cursor.close()
connection.close()

print("✅ Casino detection completed")
