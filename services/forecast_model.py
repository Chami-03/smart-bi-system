from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score


class RevenueForecaster:
    def forecast_revenue(self, monthly_sales: List[Dict[str, float]]) -> Dict[str, object]:
        history_df = pd.DataFrame(monthly_sales)
        if history_df.empty:
            return {
                "historical": [],
                "forecast": [],
                "model_metrics": {"observations": 0, "r2_score": None},
            }

        history_df["revenue"] = pd.to_numeric(history_df["revenue"], errors="coerce").fillna(0.0)
        history_df = history_df.dropna(subset=["month"]).reset_index(drop=True)

        if history_df.empty:
            return {
                "historical": [],
                "forecast": [],
                "model_metrics": {"observations": 0, "r2_score": None},
            }

        history_df["month_period"] = pd.PeriodIndex(history_df["month"], freq="M")
        history_df = history_df.sort_values("month_period").reset_index(drop=True)

        y = history_df["revenue"].to_numpy(dtype=float)
        x = np.arange(len(history_df)).reshape(-1, 1)

        if len(history_df) == 1:
            last_value = float(y[-1])
            forecast_values = [last_value, last_value, last_value]
            score = None
        else:
            model = LinearRegression()
            model.fit(x, y)
            y_hat = model.predict(x)
            score = float(r2_score(y, y_hat))

            future_x = np.arange(len(history_df), len(history_df) + 3).reshape(-1, 1)
            forecast_values = [max(0.0, float(value)) for value in model.predict(future_x)]

        last_month = history_df.iloc[-1]["month_period"]
        forecast_months = [(last_month + i).strftime("%Y-%m") for i in range(1, 4)]

        return {
            "historical": [
                {"month": row.month, "revenue": float(row.revenue)}
                for row in history_df[["month", "revenue"]].itertuples(index=False)
            ],
            "forecast": [
                {"month": month, "revenue": float(value)}
                for month, value in zip(forecast_months, forecast_values)
            ],
            "model_metrics": {
                "observations": int(len(history_df)),
                "r2_score": score,
            },
        }
