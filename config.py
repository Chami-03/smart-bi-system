import os
import secrets
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY") or secrets.token_hex(32)
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024

    UPLOAD_FOLDER = str(BASE_DIR / "uploads" / "datasets")
    PROCESSED_DATA_PATH = str(BASE_DIR / "uploads" / "datasets" / "processed_latest.csv")

    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "smartbi_db")

    REQUIRED_COLUMNS = [
        "OrderID",
        "Date",
        "CustomerID",
        "Product",
        "Category",
        "Quantity",
        "Price",
    ]

    ALLOWED_EXTENSIONS = {"csv"}
