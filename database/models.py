CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS smartbi_db"

CREATE_SALES_TABLE_SQL = """
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

ALTER_SALES_TABLE_SQL = [
    "ALTER TABLE sales_data MODIFY order_id VARCHAR(255) NOT NULL",
    "ALTER TABLE sales_data MODIFY customer_id VARCHAR(255) NOT NULL",
    "ALTER TABLE sales_data MODIFY product VARCHAR(255) NOT NULL",
    "ALTER TABLE sales_data MODIFY category VARCHAR(255) NOT NULL",
    "ALTER TABLE sales_data MODIFY source_file VARCHAR(512) NOT NULL",
]

SALES_TABLE_INDEXES = {
    "idx_sales_order_date": "order_date",
    "idx_sales_product": "product",
    "idx_sales_customer": "customer_id",
    "idx_sales_category": "category",
}
