from __future__ import annotations

import getpass
import os
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)


def main() -> None:
    host = os.getenv("MYSQL_HOST", "localhost")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "root")
    database = os.getenv("MYSQL_DATABASE", "smartbi_db")

    password = os.getenv("MYSQL_PASSWORD", "")
    if not password:
        password = getpass.getpass(f"Enter MySQL password for '{user}': ")

    print(f"Connecting to MySQL on {host}:{port} as {user}...")
    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        autocommit=False,
    )

    create_database_sql = f"CREATE DATABASE IF NOT EXISTS {database}"
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sales_data (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        order_id VARCHAR(255) NOT NULL,
        order_date DATE NOT NULL,
        customer_id VARCHAR(255) NOT NULL,
        product VARCHAR(255) NOT NULL,
        category VARCHAR(255) NOT NULL,
        quantity INT NOT NULL,
        price DECIMAL(12, 2) NOT NULL,
        revenue DECIMAL(14, 2) NOT NULL,
        source_file VARCHAR(512) NOT NULL,
        uploaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """

    alter_table_sql = [
        "ALTER TABLE sales_data MODIFY order_id VARCHAR(255) NOT NULL",
        "ALTER TABLE sales_data MODIFY customer_id VARCHAR(255) NOT NULL",
        "ALTER TABLE sales_data MODIFY product VARCHAR(255) NOT NULL",
        "ALTER TABLE sales_data MODIFY category VARCHAR(255) NOT NULL",
        "ALTER TABLE sales_data MODIFY source_file VARCHAR(512) NOT NULL",
    ]

    indexes = {
        "idx_sales_order_date": "order_date",
        "idx_sales_product": "product",
        "idx_sales_customer": "customer_id",
        "idx_sales_category": "category",
    }

    try:
        with connection.cursor() as cursor:
            cursor.execute(create_database_sql)
            cursor.execute(f"USE {database}")
            cursor.execute(create_table_sql)
            for alter_sql in alter_table_sql:
                cursor.execute(alter_sql)

            for index_name, column_name in indexes.items():
                cursor.execute(
                    """
                    SELECT COUNT(1)
                    FROM information_schema.statistics
                    WHERE table_schema = %s
                      AND table_name = 'sales_data'
                      AND index_name = %s
                    """,
                    (database, index_name),
                )
                exists = cursor.fetchone()[0] > 0
                if not exists:
                    cursor.execute(f"CREATE INDEX {index_name} ON sales_data({column_name})")

        connection.commit()
        print(f"Database '{database}' and required table/indexes are ready.")
        print("Tip: save MYSQL_PASSWORD in .env to avoid entering it each run.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
