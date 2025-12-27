import mysql.connector

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="basant@12345",   # use your MySQL password
    database="blog_lead_crawler"
)

cursor = connection.cursor()

# Table: blogs
cursor.execute("""
CREATE TABLE IF NOT EXISTS blogs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    blog_url VARCHAR(500) UNIQUE,
    domain VARCHAR(255),
    first_crawled DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

# Table: blog_pages
cursor.execute("""
CREATE TABLE IF NOT EXISTS blog_pages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    blog_id INT,
    page_url VARCHAR(500),
    crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (blog_id) REFERENCES blogs(id)
)
""")

# Table: commercial_links
cursor.execute("""
CREATE TABLE IF NOT EXISTS commercial_links (
    id INT AUTO_INCREMENT PRIMARY KEY,
    page_id INT,
    commercial_url VARCHAR(500),
    is_dofollow BOOLEAN,
    FOREIGN KEY (page_id) REFERENCES blog_pages(id)
)
""")

# Table: crawl_runs (history)
cursor.execute("""
CREATE TABLE IF NOT EXISTS crawl_runs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    blog_url VARCHAR(500),
    pages_crawled INT,
    run_date DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

connection.commit()
print("✅ All tables created successfully")

cursor.close()
connection.close()
