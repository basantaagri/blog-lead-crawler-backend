import mysql.connector

try:
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="basant@12345",   # put your MySQL password here
        database="mysql"
    )

    if connection.is_connected():
        print("✅ SUCCESS: Python is connected to MySQL")

except mysql.connector.Error as error:
    print("❌ ERROR:", error)

finally:
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("🔒 Connection closed")
