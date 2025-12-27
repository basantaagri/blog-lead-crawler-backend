import mysql.connector

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="basant@12345",
    database="blog_lead_crawler"
)

cursor = connection.cursor()

cursor.execute("""
ALTER TABLE blog_pages
ADD COLUMN post_date DATE NULL
""")

connection.commit()
cursor.close()
connection.close()

print("✅ post_date column added to blog_pages")
