import mysql.connector

# Connect to MySQL (no database yet)
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="basant@12345"   # use your MySQL password
)

cursor = connection.cursor()

# Create database
cursor.execute("CREATE DATABASE IF NOT EXISTS blog_lead_crawler")
print("✅ Database 'blog_lead_crawler' created or already exists")

cursor.close()
connection.close()
