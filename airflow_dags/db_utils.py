# pyright: reportMissingImports=false
"""
db_utils.py - Centralized database utilities and helpers
Handles engine creation, SQL execution, and common operations.
"""

import logging
import re
from typing import Optional, List, Any
from urllib.parse import quote_plus

import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


# =====================================================================
# DRIVER & ENGINE CREATION
# =====================================================================
def get_odbc_driver(conn_id: str = "mssql_default") -> str:
    """
    Detect and return the best available ODBC driver.
    Falls back through candidates if one is not available.
    """
    from config import ODBC_DRIVERS
    
    conn = BaseHook.get_connection(conn_id)
    try:
        extras = conn.extra_dejson or {}
    except Exception:
        extras = {}
    
    # Check if driver is specified in connection extras
    explicit_driver = extras.get("odbc_driver") or extras.get("driver")
    if explicit_driver:
        return explicit_driver
    
    # Try candidates in order
    available = pyodbc.drivers()
    for driver in ODBC_DRIVERS:
        if driver in available:
            logger.info(f"Using ODBC driver: {driver}")
            return driver
    
    logger.warning("No known ODBC driver found, using default")
    return ODBC_DRIVERS[0]


def make_sqlalchemy_engine(
    conn_id: str = "mssql_default",
    schema: str = "master",
    echo: bool = False
) -> Engine:
    """
    Create a SQLAlchemy engine from an Airflow connection.
    
    Args:
        conn_id: Airflow connection ID (default: mssql_default)
        schema: Database/schema to connect to
        echo: Enable SQL logging (default: False)
    
    Returns:
        SQLAlchemy engine
    
    Raises:
        ValueError: If connection cannot be established
    """
    conn = BaseHook.get_connection(conn_id)
    host = conn.host
    port = conn.port or 1433
    user = conn.login or ""
    pwd = conn.get_password() or ""
    
    try:
        extras = conn.extra_dejson or {}
    except Exception:
        extras = {}
    
    encrypt = str(extras.get("Encrypt", "yes")).lower()
    trust = str(extras.get("TrustServerCertificate", "yes")).lower()
    driver = get_odbc_driver(conn_id)
    
    params = f"driver={quote_plus(driver)}&Encrypt={encrypt}&TrustServerCertificate={trust}"
    url = f"mssql+pyodbc://{quote_plus(user)}:{quote_plus(pwd)}@{host}:{port}/{quote_plus(schema)}?{params}"
    
    try:
        engine = create_engine(
            url,
            fast_executemany=True,
            use_setinputsizes=False,
            echo=echo,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Engine created for {schema}@{host}")
        return engine
    except Exception as e:
        logger.error(f"Failed to create engine: {e}")
        raise ValueError(f"Cannot connect to {schema}@{host}: {e}")


def make_pyodbc_connection(conn_id: str = "mssql_default") -> pyodbc.Connection:
    """
    Create a raw pyodbc connection for DDL operations with autocommit.
    Useful for operations like CREATE DATABASE that need autocommit.
    
    Args:
        conn_id: Airflow connection ID
    
    Returns:
        pyodbc.Connection with autocommit=True
    """
    conn = BaseHook.get_connection(conn_id)
    driver = get_odbc_driver(conn_id)
    
    conn_str = (
        f"Driver={{{driver}}};"
        f"Server={conn.host},{conn.port or 1433};"
        f"UID={conn.login or ''};"
        f"PWD={conn.get_password() or ''};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )
    
    pyodbc_conn = pyodbc.connect(conn_str)
    pyodbc_conn.autocommit = True
    logger.debug(f"Created pyodbc connection to {conn.host}")
    return pyodbc_conn


# =====================================================================
# SQL EXECUTION HELPERS
# =====================================================================
def split_sql_statements(sql: str) -> List[str]:
    """
    Split SQL into statements by semicolon, but preserve BEGIN/END blocks.
    
    Args:
        sql: SQL script (may contain multiple statements)
    
    Returns:
        List of individual SQL statements
    """
    if "BEGIN" in sql.upper() and "END" in sql.upper():
        # If script contains explicit BEGIN/END, return as single statement
        logger.debug("Detected BEGIN/END block; returning SQL as single statement")
        return [sql.strip()]
    
    # Otherwise split by semicolon
    statements = [s.strip() for s in re.split(r";\s*(?=\b)", sql) if s.strip()]
    return statements


def execute_sql_with_transaction(
    sql: str,
    conn_id: str = "mssql_default",
    schema: str = "master",
) -> None:
    """
    Execute SQL script within a transaction.
    Automatically handles BEGIN/END blocks.
    
    Args:
        sql: SQL script to execute
        conn_id: Airflow connection ID
        schema: Database/schema to connect to
    """
    engine = make_sqlalchemy_engine(conn_id, schema)
    statements = split_sql_statements(sql)
    
    with engine.begin() as conn:
        for i, stmt in enumerate(statements, 1):
            logger.info(f"Executing statement {i}/{len(statements)}: {stmt[:60]}...")
            try:
                conn.execute(text(stmt))
                logger.debug(f"Statement {i} completed successfully")
            except Exception as e:
                logger.error(f"Error executing statement {i}: {e}")
                raise


def execute_sql_with_autocommit(
    sql: str,
    conn_id: str = "mssql_default",
) -> None:
    """
    Execute SQL script with autocommit (outside transaction).
    Useful for DDL like CREATE DATABASE.
    
    Args:
        sql: SQL script to execute
        conn_id: Airflow connection ID
    """
    pyodbc_conn = make_pyodbc_connection(conn_id)
    cursor = pyodbc_conn.cursor()
    statements = split_sql_statements(sql)
    
    try:
        for i, stmt in enumerate(statements, 1):
            logger.info(f"Executing statement {i}/{len(statements)} (autocommit): {stmt[:60]}...")
            cursor.execute(stmt)
            logger.debug(f"Statement {i} completed successfully")
    finally:
        cursor.close()
        pyodbc_conn.close()


# =====================================================================
# MERGE & UPSERT HELPERS
# =====================================================================
def execute_merge(
    merge_sql: str,
    conn_id: str = "mssql_default",
    schema: str = "master",
) -> int:
    """
    Execute a MERGE statement safely.
    
    Args:
        merge_sql: MERGE statement
        conn_id: Airflow connection ID
        schema: Database/schema
    
    Returns:
        Number of rows affected (ROWCOUNT)
    """
    engine = make_sqlalchemy_engine(conn_id, schema)
    
    with engine.begin() as conn:
        logger.info(f"Executing MERGE: {merge_sql[:80]}...")
        result = conn.execute(text(merge_sql))
        rowcount = result.rowcount
        logger.info(f"MERGE completed; rows affected: {rowcount}")
        return rowcount


# =====================================================================
# QUERY & FETCH HELPERS
# =====================================================================
def fetch_scalar(
    query: str,
    conn_id: str = "mssql_default",
    schema: str = "master",
    params: dict = None,
) -> Any:
    """
    Execute a query and return first column of first row.
    
    Args:
        query: SELECT query
        conn_id: Airflow connection ID
        schema: Database/schema
        params: Query parameters
    
    Returns:
        Scalar value or None
    """
    engine = make_sqlalchemy_engine(conn_id, schema)
    
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        row = result.first()
        return row[0] if row else None


def fetch_one(
    query: str,
    conn_id: str = "mssql_default",
    schema: str = "master",
    params: dict = None,
) -> tuple:
    """
    Execute a query and return first row as tuple.
    
    Args:
        query: SELECT query
        conn_id: Airflow connection ID
        schema: Database/schema
        params: Query parameters
    
    Returns:
        First row as tuple or None
    """
    engine = make_sqlalchemy_engine(conn_id, schema)
    
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return result.first()


def fetch_all(
    query: str,
    conn_id: str = "mssql_default",
    schema: str = "master",
    params: dict = None,
) -> List[tuple]:
    """
    Execute a query and return all rows.
    
    Args:
        query: SELECT query
        conn_id: Airflow connection ID
        schema: Database/schema
        params: Query parameters
    
    Returns:
        List of rows (tuples)
    """
    engine = make_sqlalchemy_engine(conn_id, schema)
    
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return result.fetchall()


# =====================================================================
# BULK OPERATIONS
# =====================================================================
def truncate_table(
    table_full: str,
    conn_id: str = "mssql_default",
    schema: str = "master",
) -> None:
    """
    Truncate a table (delete all rows).
    
    Args:
        table_full: Full table name (schema.table)
        conn_id: Airflow connection ID
        schema: Database/schema
    """
    sql = f"TRUNCATE TABLE {table_full};"
    execute_sql_with_transaction(sql, conn_id, schema)
    logger.info(f"Truncated table: {table_full}")


def drop_table(
    table_full: str,
    conn_id: str = "mssql_default",
    schema: str = "master",
) -> None:
    """
    Drop a table if it exists.
    
    Args:
        table_full: Full table name (schema.table)
        conn_id: Airflow connection ID
        schema: Database/schema
    """
    sql = f"IF OBJECT_ID('{table_full}','U') IS NOT NULL DROP TABLE {table_full};"
    execute_sql_with_transaction(sql, conn_id, schema)
    logger.info(f"Dropped table: {table_full}")


def table_exists(
    table_full: str,
    conn_id: str = "mssql_default",
    schema: str = "master",
) -> bool:
    """
    Check if a table exists.
    
    Args:
        table_full: Full table name (schema.table)
        conn_id: Airflow connection ID
        schema: Database/schema
    
    Returns:
        True if table exists, False otherwise
    """
    result = fetch_scalar(
        f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{table_full.split('.')[-1]}'",
        conn_id, schema
    )
    return result > 0 if result else False


def get_row_count(
    table_full: str,
    conn_id: str = "mssql_default",
    schema: str = "master",
) -> int:
    """
    Get the number of rows in a table.
    
    Args:
        table_full: Full table name (schema.table)
        conn_id: Airflow connection ID
        schema: Database/schema
    
    Returns:
        Row count
    """
    result = fetch_scalar(f"SELECT COUNT(*) FROM {table_full}", conn_id, schema)
    return result or 0