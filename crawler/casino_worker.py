import time
import os
import requests
import psycopg2
from bs4 import BeautifulSoup
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

CASINO_KEYWORDS = [
    "casino",
    "gambling",
    "betting",
    "slots",
    "poker",
    "blackjack",
    "roulette",
    "sportsbook",
    "sports betting",
]

def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        sslmode="require",
    )

def is_casino_content(text: str) -> bool:
    text = (text or "").lower()
    return any(keyword in text for keyword in CASINO_KEYWORDS)

def enrich_domain(domain: str):
    url = f"https://{domain}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        )
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return None
    except Exception:
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    title = soup.title.string.strip() if soup.title else ""
    description = ""

    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        description = meta_desc.get("content", "").strip()

    combined_text = f"{title} {description}"
    casino_flag = is_casino_content(combined_text)

    return {
        "meta_title": title,
        "meta_description": description,
        "is_casino": casino_flag,
    }

def casino_worker():
    print("🎰 Casino enrichment worker started")

    while True:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT commercial_domain
                        FROM commercial_sites
                        WHERE meta_title IS NULL
                        LIMIT 1
                        """
                    )
                    row = cur.fetchone()

            if not row:
                time.sleep(10)
                continue

            domain = row["commercial_domain"]
            print(f"🔍 Enriching casino data for: {domain}")

            result = enrich_domain(domain)

            if not result:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE commercial_sites
                            SET meta_title = '',
                                meta_description = '',
                                is_casino = FALSE
                            WHERE commercial_domain = %s
                            """,
                            (domain,),
                        )
                        conn.commit()
                continue

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE commercial_sites
                        SET meta_title = %s,
                            meta_description = %s,
                            is_casino = %s
                        WHERE commercial_domain = %s
                        """,
                        (
                            result["meta_title"],
                            result["meta_description"],
                            result["is_casino"],
                            domain,
                        ),
                    )
                    conn.commit()

            print(f"✅ Enriched {domain} | casino={result['is_casino']}")

        except Exception as e:
            print("❌ Casino worker error:", e)
            time.sleep(5)

if __name__ == "__main__":
    casino_worker()
