from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import time
import json


@task
def test_mssql_connection_enhanced():
    """Enhanced MSSQL connection test with better error handling.
    Returns a summary dict which is pushed to XCom.
    """
    max_retries = 3
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} to connect to MSSQL...")

            hook = MsSqlHook(mssql_conn_id="mssql_default")

            # Test basic connection
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Test simple query
            cursor.execute("SELECT 1 as test_value")
            result = cursor.fetchone()
            basic_value = result[0] if result else None
            print(f"OK: Basic query test passed: {basic_value}")

            # Test version query
            cursor.execute("SELECT @@VERSION as version")
            version_result = cursor.fetchone()
            version_str = (version_result[0] if version_result else "")
            print(f"OK: MSSQL Version (first 100 chars): {version_str[:100]}...")

            # List databases (optional)
            cursor.execute("SELECT name FROM sys.databases ")  # Skip system DBs
            databases = [row[0] for row in cursor.fetchall()]
            print(f"OK: Found {len(databases)} user databases")

            cursor.close()
            conn.close()

            summary = {
                "basic_query_value": basic_value,
                "version": version_str,
                "databases": databases,
                "status": "SUCCESS",
            }
            print("Summary:\n" + json.dumps(summary, indent=2))
            return summary

        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")

            if attempt < max_retries - 1:
                print(f"Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)
            else:
                print("All connection attempts failed")
                raise


@task
def show_result(summary: dict):
    print("MSSQL connection test summary (from XCom):")
    print(json.dumps(summary, indent=2))


with DAG(
    "test_mssql_connection_enhanced",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:
    result = test_mssql_connection_enhanced()
    show_result(result)

