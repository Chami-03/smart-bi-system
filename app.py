from __future__ import annotations

import os
import io
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
from flask import Flask, jsonify, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from config import Config
from database.db_connection import fetch_sales_dataframe, initialize_database, mysql_connection
from services.data_processor import DataProcessor
from services.forecast_model import RevenueForecaster
from services.insight_engine import InsightEngine


app = Flask(__name__)
app.config.from_object(Config)

Path(app.config["UPLOAD_FOLDER"]).mkdir(parents=True, exist_ok=True)

processor = DataProcessor()
forecaster = RevenueForecaster()
insight_engine = InsightEngine()


try:
    initialize_database()
except RuntimeError as exc:
    # Startup should continue so the API can still return diagnostics when DB is unreachable.
    print(exc)


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in app.config["ALLOWED_EXTENSIONS"]


def read_uploaded_csv(file_path: str) -> pd.DataFrame:
    read_attempts = [
        {"encoding": "utf-8", "sep": None, "engine": "python"},
        {"encoding": "utf-8-sig", "sep": None, "engine": "python"},
        {"encoding": "latin-1", "sep": None, "engine": "python"},
        {"encoding": "utf-8", "sep": ";", "engine": "python"},
        {"encoding": "utf-8", "sep": "\t", "engine": "python"},
    ]

    last_error = None
    for options in read_attempts:
        try:
            df = pd.read_csv(file_path, **options)
            if df is not None and not df.empty and len(df.columns) > 1:
                return df
        except Exception as exc:
            last_error = exc

    raise ValueError(f"Could not parse CSV file. Please verify delimiter/encoding. Details: {last_error}")


def _read_filters() -> Dict[str, str]:
    return {
        "date_from": request.args.get("date_from", "").strip(),
        "date_to": request.args.get("date_to", "").strip(),
        "category": request.args.get("category", "").strip(),
        "product": request.args.get("product", "").strip(),
    }


@app.route("/")
def index():
    return redirect(url_for("dashboard_page"))


@app.route("/upload")
def upload_page():
    return render_template("upload.html")


@app.route("/dashboard")
def dashboard_page():
    return render_template("dashboard.html")


@app.route("/upload-dataset", methods=["POST"])
def upload_dataset():
    if "file" not in request.files:
        return jsonify({"success": False, "message": "No file in request."}), 400

    file = request.files["file"]
    if not file or file.filename == "":
        return jsonify({"success": False, "message": "No file selected."}), 400

    if not allowed_file(file.filename):
        return jsonify({"success": False, "message": "Only CSV files are allowed."}), 400

    safe_name = secure_filename(file.filename)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    saved_name = f"{timestamp}_{safe_name}"
    file_path = os.path.join(app.config["UPLOAD_FOLDER"], saved_name)
    file.save(file_path)

    try:
        raw_df = read_uploaded_csv(file_path)
        cleaned_df, cleaning_report = processor.validate_and_clean(raw_df)
        stored_rows = processor.store_dataset(cleaned_df, saved_name)

        return jsonify(
            {
                "success": True,
                "message": "Dataset uploaded and processed successfully.",
                "stored_rows": stored_rows,
                "cleaning_report": {
                    "original_rows": cleaning_report.original_rows,
                    "cleaned_rows": cleaning_report.cleaned_rows,
                    "removed_rows": cleaning_report.removed_rows,
                    "mapping_report": cleaning_report.mapping_report,
                },
            }
        )
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 400


@app.route("/reset-dataset", methods=["POST"])
def reset_dataset():
    try:
        with mysql_connection(Config.MYSQL_DATABASE) as connection:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE sales_data")
            connection.commit()
        return jsonify({"success": True, "message": "Dataset reset successfully."})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route("/process-data", methods=["GET"])
def process_data():
    try:
        analytics = processor.process_analytics(_read_filters())
        return jsonify({"success": True, "analytics": analytics})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route("/get-analytics", methods=["GET"])
def get_analytics():
    try:
        analytics = processor.process_analytics(_read_filters())
        return jsonify({"success": True, "analytics": analytics})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route("/get-forecast", methods=["GET"])
def get_forecast():
    try:
        analytics = processor.process_analytics(_read_filters())
        forecast = forecaster.forecast_revenue(analytics.get("monthly_sales", []))
        return jsonify({"success": True, "forecast": forecast})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route("/get-insights", methods=["GET"])
def get_insights():
    try:
        analytics = processor.process_analytics(_read_filters())
        forecast = forecaster.forecast_revenue(analytics.get("monthly_sales", []))
        insights = insight_engine.generate_insights(analytics, forecast)
        return jsonify({"success": True, "insights": insights})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route("/download-processed-dataset", methods=["GET"])
def download_processed_dataset():
    try:
        df = fetch_sales_dataframe()
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500

    if df.empty:
        return jsonify({"success": False, "message": "No data available in MySQL yet."}), 404

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    memory_file = io.BytesIO(buffer.getvalue().encode("utf-8"))
    memory_file.seek(0)

    return send_file(
        memory_file,
        mimetype="text/csv",
        as_attachment=True,
        download_name="processed_dataset.csv",
    )


@app.route("/export-filtered-chart-data", methods=["GET"])
def export_filtered_chart_data():
    try:
        df = fetch_sales_dataframe(_read_filters())
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500

    if df.empty:
        return jsonify({"success": False, "message": "No filtered data available to export."}), 404

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    memory_file = io.BytesIO(buffer.getvalue().encode("utf-8"))
    memory_file.seek(0)

    return send_file(
        memory_file,
        mimetype="text/csv",
        as_attachment=True,
        download_name="filtered_chart_data.csv",
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "smartbi-dashboard"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
