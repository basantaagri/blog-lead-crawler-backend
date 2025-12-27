import mysql.connector

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="basant@12345",
    database="blog_lead_crawler"
)

cursor = connection.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS commercial_site_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    commercial_domain VARCHAR(255) UNIQUE,
    is_casino BOOLEAN,
    matched_keywords TEXT,
    checked_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

connection.commit()
cursor.close()
connection.close()

print("✅ Casino analysis table ready")
