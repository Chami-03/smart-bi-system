from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Generator, Optional

import pandas as pd
import mysql.connector
from mysql.connector import Error

from config import Config
from database.models import ALTER_SALES_TABLE_SQL, CREATE_DATABASE_SQL, CREATE_SALES_TABLE_SQL, SALES_TABLE_INDEXES


def _safe_str(value: object, max_len: int) -> str:
    text = str(value) if value is not None else ""
    return text[:max_len]


@contextmanager
def mysql_connection(database: Optional[str] = None) -> Generator[mysql.connector.MySQLConnection, None, None]:
    connection = mysql.connector.connect(
        host=Config.MYSQL_HOST,
        port=Config.MYSQL_PORT,
        user=Config.MYSQL_USER,
        password=Config.MYSQL_PASSWORD,
        database=database,
        autocommit=False,
    )
    try:
        yield connection
    finally:
        connection.close()


def initialize_database() -> None:
    try:
        with mysql_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_DATABASE_SQL)
            connection.commit()

        with mysql_connection(Config.MYSQL_DATABASE) as connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_SALES_TABLE_SQL)
                for alter_sql in ALTER_SALES_TABLE_SQL:
                    cursor.execute(alter_sql)
                for index_name, column_name in SALES_TABLE_INDEXES.items():
                    cursor.execute(
                        """
                        SELECT COUNT(1)
                        FROM information_schema.statistics
                        WHERE table_schema = %s
                          AND table_name = 'sales_data'
                          AND index_name = %s
                        """,
                        (Config.MYSQL_DATABASE, index_name),
                    )
                    exists = cursor.fetchone()[0] > 0
                    if not exists:
                        cursor.execute(f"CREATE INDEX {index_name} ON sales_data({column_name})")
            connection.commit()
    except Error as exc:
        raise RuntimeError(f"Failed to initialize database: {exc}") from exc


def replace_sales_data(df: pd.DataFrame, source_file: str) -> int:
    insert_sql = """
    INSERT INTO sales_data (
        order_id, order_date, customer_id, product, category, quantity, price, revenue, source_file
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = [
        (
            _safe_str(row.OrderID, 255),
            row.Date.date(),
            _safe_str(row.CustomerID, 255),
            _safe_str(row.Product, 255),
            _safe_str(row.Category, 255),
            int(row.Quantity),
            float(row.Price),
            float(row.Revenue),
            _safe_str(source_file, 512),
        )
        for row in df.itertuples(index=False)
    ]

    try:
        with mysql_connection(Config.MYSQL_DATABASE) as connection:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE sales_data")
                if records:
                    cursor.executemany(insert_sql, records)
            connection.commit()
    except Error as exc:
        raise RuntimeError(f"Failed to store dataset in MySQL: {exc}") from exc

    return len(records)


def fetch_sales_dataframe(filters: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    filters = filters or {}
    where_clauses = []
    values = []

    if filters.get("date_from"):
        where_clauses.append("order_date >= %s")
        values.append(filters["date_from"])
    if filters.get("date_to"):
        where_clauses.append("order_date <= %s")
        values.append(filters["date_to"])
    if filters.get("category"):
        where_clauses.append("category = %s")
        values.append(filters["category"])
    if filters.get("product"):
        where_clauses.append("product = %s")
        values.append(filters["product"])

    query = """
    SELECT order_id AS OrderID,
           order_date AS Date,
           customer_id AS CustomerID,
           product AS Product,
           category AS Category,
           quantity AS Quantity,
           price AS Price,
           revenue AS Revenue,
           source_file AS SourceFile,
           uploaded_at AS UploadedAt
    FROM sales_data
    """

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " ORDER BY order_date ASC"

    try:
        with mysql_connection(Config.MYSQL_DATABASE) as connection:
            return pd.read_sql(query, con=connection, params=values, parse_dates=["Date", "UploadedAt"])
    except Error as exc:
        raise RuntimeError(f"Failed to read dataset from MySQL: {exc}") from exc
