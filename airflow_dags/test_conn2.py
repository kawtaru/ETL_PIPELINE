import pyodbc

def test_sql_server_connection():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=mssql,1433;"
        "DATABASE=master;"
        "UID=sa;"
        "PWD=test1234*;"
        "TrustServerCertificate=yes;"
    )

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION;")
            row = cursor.fetchone()
            print("✅ Connected successfully!")
            print(f"SQL Server version: {row[0]}")
    except Exception as e:
        print("❌ Connection failed:", e)

if __name__ == "__main__":
    test_sql_server_connection()
