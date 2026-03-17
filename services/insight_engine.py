from __future__ import annotations

from typing import Dict, List


class InsightEngine:
    def generate_insights(self, analytics: Dict[str, object], forecast: Dict[str, object]) -> List[str]:
        insights: List[str] = []

        growth = float(analytics.get("revenue_growth_pct", 0.0) or 0.0)
        if growth > 10:
            insights.append("Sales increased significantly compared to the previous period.")
        elif growth < -10:
            insights.append("Sales declined notably compared to the previous period.")

        top_products = analytics.get("top_products", [])
        total_revenue = float(analytics.get("total_revenue", 0.0) or 0.0)
        if top_products and total_revenue > 0:
            top_share = (float(top_products[0]["revenue"]) / total_revenue) * 100
            if top_share > 40:
                insights.append("This product is the primary revenue driver.")

        concentration_ratio = float(analytics.get("customer_concentration_ratio", 0.0) or 0.0)
        if concentration_ratio > 60:
            insights.append("A small customer segment contributes a large portion of total sales.")

        avg_order_value = float(analytics.get("average_order_value", 0.0) or 0.0)
        if avg_order_value > 0:
            insights.append(f"Average order value is {avg_order_value:,.2f}, useful for monitoring pricing strategy.")

        forecast_values = [float(item["revenue"]) for item in forecast.get("forecast", [])]
        historical_values = [float(item["revenue"]) for item in forecast.get("historical", [])]
        if forecast_values and historical_values:
            if sum(forecast_values) / len(forecast_values) > sum(historical_values) / len(historical_values):
                insights.append("Revenue forecast suggests positive momentum over the next three months.")
            else:
                insights.append("Revenue forecast indicates potential slowdown; monitor demand and promotions closely.")

        if not insights:
            insights.append("No exceptional trend detected yet. Continue monitoring incoming sales data for clearer signals.")

        return insights
