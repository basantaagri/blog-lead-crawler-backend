from bs4 import BeautifulSoup
from datetime import datetime
import re

def extract_post_date(html):
    soup = BeautifulSoup(html, "html.parser")

    # 1. <time datetime="2024-06-12">
    time_tag = soup.find("time")
    if time_tag:
        dt = time_tag.get("datetime")
        if dt:
            try:
                return datetime.fromisoformat(dt[:10]).date()
            except:
                pass

    # 2. meta property="article:published_time"
    meta = soup.find("meta", {"property": "article:published_time"})
    if meta and meta.get("content"):
        try:
            return datetime.fromisoformat(meta["content"][:10]).date()
        except:
            pass

    # 3. Regex date fallback (YYYY-MM-DD)
    text = soup.get_text(" ", strip=True)
    match = re.search(r"\b(20\d{2})[-/](\d{2})[-/](\d{2})\b", text)
    if match:
        try:
            return datetime.strptime(match.group(0), "%Y-%m-%d").date()
        except:
            pass

    return None
