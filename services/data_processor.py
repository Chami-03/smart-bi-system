from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from config import Config
from database.db_connection import fetch_sales_dataframe, replace_sales_data


@dataclass
class CleaningReport:
    original_rows: int
    cleaned_rows: int
    removed_rows: int
    mapping_report: Dict[str, object]


class DataProcessor:
    def __init__(self) -> None:
        self.required_columns = Config.REQUIRED_COLUMNS
        self.column_aliases = {
            "OrderID": [
                "orderid",
                "order_id",
                "order",
                "orderno",
                "order_no",
                "invoiceid",
                "invoice_id",
                "invoice",
                "invoice_no",
                "transactionid",
                "transaction_id",
                "transaction_no",
                "billid",
                "bill_no",
            ],
            "Date": [
                "date",
                "orderdate",
                "order_date",
                "transactiondate",
                "transaction_date",
                "purchasedate",
                "purchase_date",
                "salesdate",
                "sales_date",
                "txndate",
                "txn_date",
                "datetime",
                "timestamp",
            ],
            "CustomerID": [
                "customerid",
                "customer_id",
                "customer",
                "clientid",
                "client_id",
                "client",
                "buyerid",
                "buyer_id",
                "customercode",
                "customer_code",
                "accountid",
                "account_id",
            ],
            "Product": [
                "product",
                "productname",
                "product_name",
                "producttitle",
                "product_title",
                "item",
                "itemname",
                "item_name",
                "itemdesc",
                "item_desc",
                "sku",
                "description",
                "productcode",
                "product_code",
                "itemcode",
                "item_code",
                "material",
            ],
            "Category": [
                "category",
                "productcategory",
                "product_category",
                "segment",
                "department",
                "group",
                "sub_category",
                "subcategory",
                "class",
            ],
            "Quantity": [
                "quantity",
                "qty",
                "units",
                "unitssold",
                "units_sold",
                "count",
                "volume",
                "pieces",
                "pcs",
            ],
            "Price": [
                "price",
                "unitprice",
                "unit_price",
                "salesprice",
                "sales_price",
                "rate",
                "amount",
                "value",
                "unitcost",
                "unit_cost",
                "cost",
            ],
            "Revenue": [
                "revenue",
                "sales",
                "salesamount",
                "sales_amount",
                "total",
                "totalamount",
                "total_amount",
                "turnover",
                "grosssales",
                "gross_sales",
                "netsales",
                "net_sales",
            ],
        }

    @staticmethod
    def _normalize_column_name(name: str) -> str:
        return re.sub(r"[^a-z0-9]", "", str(name).strip().lower())

    @staticmethod
    def _to_numeric(series: pd.Series) -> pd.Series:
        cleaned = series.astype(str).str.replace(r"[^0-9.\-]", "", regex=True)
        return pd.to_numeric(cleaned, errors="coerce")

    @staticmethod
    def _is_text_like(series: pd.Series) -> float:
        sample = series.dropna().astype(str).str.strip()
        if sample.empty:
            return 0.0
        has_letters = sample.str.contains(r"[A-Za-z]", regex=True)
        return float(has_letters.mean())

    @staticmethod
    def _parse_date_ratio(series: pd.Series) -> float:
        as_text = series.astype(str).str.strip()
        parsed_default = pd.to_datetime(as_text, errors="coerce")
        parsed_dayfirst = pd.to_datetime(as_text, errors="coerce", dayfirst=True)
        numeric = pd.to_numeric(as_text, errors="coerce")

        # Avoid treating small numeric business metrics (e.g. 3.9, 4.2) as epoch dates.
        abs_median = float(numeric.dropna().abs().median()) if numeric.notna().any() else 0.0
        parsed_epoch = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
        parsed_epoch_ms = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
        if abs_median >= 1_000_000_000:  # likely epoch seconds
            parsed_epoch = pd.to_datetime(numeric, unit="s", errors="coerce")
        if abs_median >= 1_000_000_000_000:  # likely epoch milliseconds
            parsed_epoch_ms = pd.to_datetime(numeric, unit="ms", errors="coerce")

        def plausible_ratio(dt_series: pd.Series) -> float:
            if dt_series.isna().all():
                return 0.0
            in_range = dt_series.dt.year.between(1990, 2100)
            return float(in_range.mean())

        return float(
            max(
                plausible_ratio(parsed_default),
                plausible_ratio(parsed_dayfirst),
                plausible_ratio(parsed_epoch),
                plausible_ratio(parsed_epoch_ms),
            )
        )

    @staticmethod
    def _parse_date_series(series: pd.Series) -> pd.Series:
        as_text = series.astype(str).str.strip()
        parsed_default = pd.to_datetime(as_text, errors="coerce")
        parsed_dayfirst = pd.to_datetime(as_text, errors="coerce", dayfirst=True)
        numeric = pd.to_numeric(as_text, errors="coerce")

        abs_median = float(numeric.dropna().abs().median()) if numeric.notna().any() else 0.0
        parsed_epoch = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
        parsed_epoch_ms = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
        if abs_median >= 1_000_000_000:
            parsed_epoch = pd.to_datetime(numeric, unit="s", errors="coerce")
        if abs_median >= 1_000_000_000_000:
            parsed_epoch_ms = pd.to_datetime(numeric, unit="ms", errors="coerce")

        candidates = [parsed_default, parsed_dayfirst, parsed_epoch, parsed_epoch_ms]

        def score(dt_series: pd.Series) -> float:
            if dt_series.isna().all():
                return 0.0
            return float(dt_series.dt.year.between(1990, 2100).mean())

        best = max(candidates, key=score)
        # Ensure dtype stability on assignment.
        return pd.Series(best.to_numpy(dtype="datetime64[ns]"), index=series.index)

    @staticmethod
    def _token_hit_count(normalized_name: str, tokens: List[str]) -> int:
        return sum(1 for token in tokens if token in normalized_name)

    def _auto_map_columns(self, raw_columns: List[str], raw_df: pd.DataFrame) -> Dict[str, str]:
        normalized_to_original: Dict[str, str] = {}
        for column in raw_columns:
            key = self._normalize_column_name(column)
            if key and key not in normalized_to_original:
                normalized_to_original[key] = column

        mapped: Dict[str, str] = {}
        used_columns = set()

        for canonical, aliases in self.column_aliases.items():
            candidates = aliases + [canonical]
            for candidate in candidates:
                normalized = self._normalize_column_name(candidate)
                original = normalized_to_original.get(normalized)
                if original and original not in used_columns:
                    mapped[canonical] = original
                    used_columns.add(original)
                    break

        profiles = {}
        for column in raw_columns:
            normalized = self._normalize_column_name(column)
            series = raw_df[column]
            numeric = self._to_numeric(series)
            numeric_ratio = float(numeric.notna().mean())
            integer_ratio = float((numeric.dropna() % 1 == 0).mean()) if numeric.notna().any() else 0.0
            unique_count = int(series.astype(str).nunique(dropna=True))
            unique_ratio = float(unique_count / max(len(series), 1))
            text_ratio = self._is_text_like(series)
            date_ratio = self._parse_date_ratio(series)
            median_abs = float(numeric.dropna().abs().median()) if numeric.notna().any() else 0.0
            profiles[column] = {
                "normalized": normalized,
                "numeric_ratio": numeric_ratio,
                "integer_ratio": integer_ratio,
                "unique_count": unique_count,
                "unique_ratio": unique_ratio,
                "text_ratio": text_ratio,
                "date_ratio": date_ratio,
                "median_abs": median_abs,
            }

        # Heuristic fallback for date if no alias matched.
        if "Date" not in mapped:
            best_col = None
            best_ratio = 0.0
            for column in raw_columns:
                if column in used_columns:
                    continue
                ratio = profiles[column]["date_ratio"]
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_col = column
            if best_col and best_ratio >= 0.5:
                mapped["Date"] = best_col
                used_columns.add(best_col)

        if "OrderID" not in mapped:
            best_col = None
            best_score = 0.0
            for column in raw_columns:
                if column in used_columns:
                    continue
                p = profiles[column]
                token_hits = self._token_hit_count(p["normalized"], ["order", "invoice", "bill", "txn", "transaction"])
                score = token_hits * 0.6 + p["unique_ratio"] * 0.4
                if score > best_score and p["text_ratio"] >= 0.3 and p["unique_ratio"] >= 0.5:
                    best_score = score
                    best_col = column
            if best_col:
                mapped["OrderID"] = best_col
                used_columns.add(best_col)

        if "CustomerID" not in mapped:
            best_col = None
            best_score = 0.0
            for column in raw_columns:
                if column in used_columns:
                    continue
                p = profiles[column]
                token_hits = self._token_hit_count(p["normalized"], ["customer", "client", "buyer", "account"])
                score = token_hits * 0.7 + p["unique_ratio"] * 0.3
                if score > best_score and p["text_ratio"] >= 0.3:
                    best_score = score
                    best_col = column
            if best_col:
                mapped["CustomerID"] = best_col
                used_columns.add(best_col)

        # Heuristic fallback for product if no alias matched.
        if "Product" not in mapped:
            best_col = None
            best_score = 0.0
            for column in raw_columns:
                if column in used_columns:
                    continue

                normalized = self._normalize_column_name(column)
                if any(token in normalized for token in ["customer", "client", "buyer", "order", "invoice", "date", "qty", "price", "amount", "revenue"]):
                    continue

                p = profiles[column]
                text_ratio = p["text_ratio"]
                unique_ratio = p["unique_ratio"]
                avg_length = float(raw_df[column].astype(str).str.len().replace([np.inf, -np.inf], np.nan).fillna(0).mean())
                length_score = min(avg_length / 20.0, 1.0)
                score = text_ratio * 0.6 + unique_ratio * 0.2 + length_score * 0.2
                if score > best_score and text_ratio >= 0.5 and p["date_ratio"] < 0.35:
                    best_score = score
                    best_col = column

            if best_col:
                mapped["Product"] = best_col
                used_columns.add(best_col)

        if "Category" not in mapped:
            best_col = None
            best_score = 0.0
            for column in raw_columns:
                if column in used_columns:
                    continue
                p = profiles[column]
                if p["text_ratio"] < 0.6:
                    continue
                low_cardinality_score = 1.0 if p["unique_count"] <= max(20, int(len(raw_df) * 0.3)) else 0.0
                token_hits = self._token_hit_count(p["normalized"], ["category", "segment", "department", "group", "class"])
                score = token_hits * 0.6 + low_cardinality_score * 0.4
                if score > best_score:
                    best_score = score
                    best_col = column
            if best_col:
                mapped["Category"] = best_col
                used_columns.add(best_col)

        # Heuristic fallback for numeric columns if aliases did not match.
        numeric_candidates: List[Tuple[str, float]] = []
        for column in raw_columns:
            if column in used_columns:
                continue
            numeric_ratio = profiles[column]["numeric_ratio"]
            if numeric_ratio >= 0.7:
                numeric_candidates.append((column, numeric_ratio))

        numeric_candidates.sort(key=lambda x: x[1], reverse=True)

        if "Revenue" not in mapped:
            best_col = None
            best_score = 0.0
            for column, _ in numeric_candidates:
                p = profiles[column]
                token_hits = self._token_hit_count(p["normalized"], ["revenue", "sales", "total", "turnover", "amount", "value"])
                score = token_hits * 0.7 + min(p["median_abs"] / 10000.0, 1.0) * 0.3
                if score > best_score:
                    best_score = score
                    best_col = column
            if best_col and best_score > 0:
                mapped["Revenue"] = best_col
                used_columns.add(best_col)

        if "Price" not in mapped and numeric_candidates:
            best_col = None
            best_score = -1.0
            for column, _ in numeric_candidates:
                if column in used_columns:
                    continue
                p = profiles[column]
                token_hits = self._token_hit_count(p["normalized"], ["price", "rate", "unit", "cost"])
                score = token_hits * 0.8 + (1.0 - min(p["median_abs"] / 100000.0, 1.0)) * 0.2
                if score > best_score:
                    best_score = score
                    best_col = column
            if best_col:
                mapped["Price"] = best_col
                used_columns.add(best_col)

        if "Quantity" not in mapped:
            best_col = None
            best_score = -1.0
            for candidate, _ in numeric_candidates:
                if candidate in used_columns:
                    continue
                p = profiles[candidate]
                token_hits = self._token_hit_count(p["normalized"], ["qty", "quantity", "units", "count", "pcs", "pieces", "volume"])
                integer_bonus = p["integer_ratio"]
                magnitude_penalty = min(p["median_abs"] / 1000000.0, 1.0)
                score = token_hits * 0.7 + integer_bonus * 0.4 - magnitude_penalty * 0.4
                if score > best_score:
                    best_score = score
                    best_col = candidate
            if best_col:
                mapped["Quantity"] = best_col
                used_columns.add(best_col)

        return mapped

    def validate_and_clean(self, raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, CleaningReport]:
        df = raw_df.copy()

        normalized_columns = {column: column.strip() for column in df.columns}
        df.rename(columns=normalized_columns, inplace=True)

        mapped_columns = self._auto_map_columns(df.columns.tolist(), df)
        mapping_warnings: List[str] = []

        # Pick a text-like fallback product column if explicit mapping is missing.
        if "Product" not in mapped_columns:
            excluded = {mapped_columns[key] for key in mapped_columns.keys() if key in mapped_columns}
            fallback_product = None
            best_score = 0.0
            for column in df.columns:
                if column in excluded:
                    continue
                text_ratio = self._is_text_like(df[column])
                unique_ratio = float(df[column].astype(str).nunique(dropna=True) / max(len(df), 1))
                score = text_ratio * 0.7 + unique_ratio * 0.3
                if score > best_score and text_ratio >= 0.4:
                    best_score = score
                    fallback_product = column
            if fallback_product:
                mapped_columns["Product"] = fallback_product
                mapping_warnings.append(f"Product mapped by heuristic from column '{fallback_product}'.")

        selected = pd.DataFrame()

        selected["OrderID"] = (
            df[mapped_columns["OrderID"]].astype(str).str.strip()
            if "OrderID" in mapped_columns
            else [f"ORD-{idx + 1}" for idx in range(len(df))]
        )
        if "OrderID" not in mapped_columns:
            mapping_warnings.append("OrderID missing; generated synthetic order IDs.")
        if "Date" in mapped_columns:
            selected["Date"] = df[mapped_columns["Date"]]
        else:
            # Fallback to synthetic daily dates when source data has no date field.
            selected["Date"] = pd.date_range(end=pd.Timestamp.today().normalize(), periods=len(df), freq="D")
            mapping_warnings.append("Date missing; generated synthetic daily dates.")
        selected["CustomerID"] = (
            df[mapped_columns["CustomerID"]].astype(str).str.strip()
            if "CustomerID" in mapped_columns
            else "UNKNOWN_CUSTOMER"
        )
        if "CustomerID" not in mapped_columns:
            mapping_warnings.append("CustomerID missing; used default UNKNOWN_CUSTOMER.")
        selected["Product"] = (
            df[mapped_columns["Product"]].astype(str).str.strip()
            if "Product" in mapped_columns
            else "Unknown Product"
        )
        if "Product" not in mapped_columns:
            mapping_warnings.append("Product missing; used default Unknown Product.")
        selected["Category"] = (
            df[mapped_columns["Category"]].astype(str).str.strip()
            if "Category" in mapped_columns
            else "Uncategorized"
        )
        if "Category" not in mapped_columns:
            mapping_warnings.append("Category missing; used default Uncategorized.")

        if "Quantity" in mapped_columns:
            selected["Quantity"] = self._to_numeric(df[mapped_columns["Quantity"]])
        else:
            selected["Quantity"] = np.nan
            mapping_warnings.append("Quantity missing; derived/fallback quantity logic applied.")

        if "Price" in mapped_columns:
            selected["Price"] = self._to_numeric(df[mapped_columns["Price"]])
        else:
            selected["Price"] = np.nan
            mapping_warnings.append("Price missing; derived/fallback price logic applied.")

        revenue_series = self._to_numeric(df[mapped_columns["Revenue"]]) if "Revenue" in mapped_columns else pd.Series(np.nan, index=df.index)

        # Derive missing value columns using best-effort logic.
        if selected["Quantity"].isna().all() and selected["Price"].isna().all() and revenue_series.notna().any():
            selected["Quantity"] = 1
            selected["Price"] = revenue_series

        missing_price = selected["Price"].isna() & revenue_series.notna() & selected["Quantity"].notna() & (selected["Quantity"] > 0)
        selected.loc[missing_price, "Price"] = revenue_series[missing_price] / selected.loc[missing_price, "Quantity"]

        missing_qty = selected["Quantity"].isna() & revenue_series.notna() & selected["Price"].notna() & (selected["Price"] > 0)
        selected.loc[missing_qty, "Quantity"] = revenue_series[missing_qty] / selected.loc[missing_qty, "Price"]

        selected["Quantity"] = selected["Quantity"].fillna(1)
        selected["Price"] = selected["Price"].fillna(revenue_series)
        selected["Price"] = selected["Price"].fillna(0)

        # If quantity mapping looks wrong (IDs, very large values), fallback to 1.
        selected["Quantity"] = selected["Quantity"].where(selected["Quantity"].between(1, 100000), 1)

        # If price is invalid but revenue exists, use revenue as price with quantity=1.
        if "Revenue" in mapped_columns:
            invalid_price = selected["Price"].isna() | (selected["Price"] < 0)
            selected.loc[invalid_price, "Price"] = revenue_series[invalid_price]
            selected.loc[invalid_price, "Quantity"] = 1

        df = selected
        original_rows = len(df)

        df["Date"] = self._parse_date_series(df["Date"])
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
        df["Price"] = pd.to_numeric(df["Price"], errors="coerce")

        for col in ["OrderID", "CustomerID", "Product", "Category"]:
            df[col] = df[col].astype(str).str.strip()

        df["OrderID"] = df["OrderID"].replace("", np.nan).fillna(pd.Series([f"ORD-{idx + 1}" for idx in range(len(df))]))
        df["CustomerID"] = df["CustomerID"].replace("", np.nan).fillna("UNKNOWN_CUSTOMER")
        df["Product"] = df["Product"].replace("", np.nan).fillna("Unknown Product")
        df["Category"] = df["Category"].replace("", np.nan).fillna("Uncategorized")

        df = df.dropna(subset=["Date", "Quantity", "Price"])
        df = df[df["Quantity"] > 0]
        df = df[df["Price"] >= 0]
        df = df[df["Quantity"] <= 1000000]
        df["Price"] = df["Price"].clip(lower=0, upper=9_999_999_999.99)
        df = df.drop_duplicates(subset=["OrderID", "Product", "CustomerID", "Date"], keep="last")

        df["Quantity"] = df["Quantity"].astype(int)
        df["Price"] = df["Price"].astype(float)
        df["Revenue"] = (df["Quantity"] * df["Price"]).clip(lower=0, upper=999_999_999_999.99)

        df = df.sort_values("Date").reset_index(drop=True)

        cleaned_rows = len(df)
        mapping_report = {
            "mapped_columns": {
                "OrderID": mapped_columns.get("OrderID"),
                "Date": mapped_columns.get("Date"),
                "CustomerID": mapped_columns.get("CustomerID"),
                "Product": mapped_columns.get("Product"),
                "Category": mapped_columns.get("Category"),
                "Quantity": mapped_columns.get("Quantity"),
                "Price": mapped_columns.get("Price"),
                "Revenue": mapped_columns.get("Revenue"),
            },
            "warnings": mapping_warnings,
        }
        report = CleaningReport(
            original_rows=original_rows,
            cleaned_rows=cleaned_rows,
            removed_rows=original_rows - cleaned_rows,
            mapping_report=mapping_report,
        )

        return df, report

    def store_dataset(self, cleaned_df: pd.DataFrame, source_file: str) -> int:
        return replace_sales_data(cleaned_df, source_file)

    def process_analytics(self, filters: Dict[str, str] | None = None) -> Dict[str, object]:
        df = fetch_sales_dataframe(filters)

        if df.empty:
            return {
                "monthly_sales": [],
                "top_products": [],
                "customer_spending": [],
                "product_demand": [],
                "average_order_value": 0.0,
                "total_revenue": 0.0,
                "total_orders": 0,
                "total_customers": 0,
                "total_products": 0,
                "revenue_growth_pct": 0.0,
                "customer_concentration_ratio": 0.0,
                "repeat_customer_rate": 0.0,
                "summary_statistics": {},
                "available_filters": {
                    "categories": [],
                    "products": [],
                    "date_min": None,
                    "date_max": None,
                },
            }

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"])  # Defensive handling for malformed DB records.

        monthly_sales_df = (
            df.groupby(df["Date"].dt.to_period("M"))["Revenue"]
            .sum()
            .reset_index()
            .rename(columns={"Date": "month_period"})
        )
        monthly_sales_df["month"] = monthly_sales_df["month_period"].astype(str)
        monthly_sales = [
            {"month": row.month, "revenue": float(row.Revenue)}
            for row in monthly_sales_df[["month", "Revenue"]].itertuples(index=False)
        ]

        top_products_df = df.groupby("Product", as_index=False)["Revenue"].sum().sort_values("Revenue", ascending=False)
        top_products = [
            {"product": row.Product, "revenue": float(row.Revenue)}
            for row in top_products_df.itertuples(index=False)
        ]

        customer_spending_df = (
            df.groupby("CustomerID", as_index=False)["Revenue"].sum().sort_values("Revenue", ascending=False).head(10)
        )
        customer_spending = [
            {"customer": str(row.CustomerID), "revenue": float(row.Revenue)}
            for row in customer_spending_df.itertuples(index=False)
        ]

        product_demand_df = (
            df.groupby("Product", as_index=False)["Quantity"].sum().sort_values("Quantity", ascending=False).head(10)
        )
        product_demand = [
            {"product": row.Product, "quantity": int(row.Quantity)}
            for row in product_demand_df.itertuples(index=False)
        ]

        order_values = df.groupby("OrderID", as_index=False)["Revenue"].sum()
        average_order_value = float(order_values["Revenue"].mean()) if not order_values.empty else 0.0

        total_revenue = float(df["Revenue"].sum())
        total_orders = int(df["OrderID"].nunique())
        total_customers = int(df["CustomerID"].nunique())
        total_products = int(df["Product"].nunique())

        revenue_growth_pct = 0.0
        if len(monthly_sales_df) >= 2:
            previous = float(monthly_sales_df.iloc[-2]["Revenue"])
            current = float(monthly_sales_df.iloc[-1]["Revenue"])
            if previous > 0:
                revenue_growth_pct = ((current - previous) / previous) * 100

        customer_total_df = df.groupby("CustomerID", as_index=False)["Revenue"].sum().sort_values("Revenue", ascending=False)
        top_customer_count = max(1, int(np.ceil(len(customer_total_df) * 0.2)))
        top_segment_revenue = float(customer_total_df.head(top_customer_count)["Revenue"].sum()) if not customer_total_df.empty else 0.0
        customer_concentration_ratio = (top_segment_revenue / total_revenue * 100) if total_revenue > 0 else 0.0

        customer_order_counts = df.groupby("CustomerID")["OrderID"].nunique()
        repeat_customers = int((customer_order_counts > 1).sum())
        repeat_customer_rate = (repeat_customers / total_customers * 100) if total_customers > 0 else 0.0

        summary_statistics = {
            "revenue_mean": float(df["Revenue"].mean()),
            "revenue_median": float(df["Revenue"].median()),
            "revenue_std": float(df["Revenue"].std(ddof=0)) if len(df) > 1 else 0.0,
            "quantity_mean": float(df["Quantity"].mean()),
            "price_mean": float(df["Price"].mean()),
        }

        available_filters = {
            "categories": sorted(df["Category"].dropna().astype(str).unique().tolist()),
            "products": sorted(df["Product"].dropna().astype(str).unique().tolist()),
            "date_min": df["Date"].min().strftime("%Y-%m-%d") if not df.empty else None,
            "date_max": df["Date"].max().strftime("%Y-%m-%d") if not df.empty else None,
        }

        return {
            "monthly_sales": monthly_sales,
            "top_products": top_products,
            "customer_spending": customer_spending,
            "product_demand": product_demand,
            "average_order_value": round(average_order_value, 2),
            "total_revenue": round(total_revenue, 2),
            "total_orders": total_orders,
            "total_customers": total_customers,
            "total_products": total_products,
            "revenue_growth_pct": round(revenue_growth_pct, 2),
            "customer_concentration_ratio": round(customer_concentration_ratio, 2),
            "repeat_customer_rate": round(repeat_customer_rate, 2),
            "summary_statistics": summary_statistics,
            "available_filters": available_filters,
        }
