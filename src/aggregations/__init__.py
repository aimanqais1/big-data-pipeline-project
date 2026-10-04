"""
Phase 2 — Step 2 & Step 3B: MongoDB Aggregation Reports & Materialized Views Package.
Exports the 5 approved read-only analytical aggregation reports on `orders_validated`
and the Materialized Views manager functions.
"""
from src.aggregations.reports import (
    AGGREGATION_REGISTRY,
    daily_sales_summary,
    execute_aggregation_by_name,
    list_available_aggregations,
    order_status_distribution,
    payment_method_analysis,
    sales_by_city,
    top_products,
)
from src.aggregations.materialized_views import (
    ensure_mv_indexes,
    get_daily_sales_mv,
    get_mv_refresh_status,
    get_top_products_mv,
    refresh_all_materialized_views,
    refresh_daily_sales_mv,
    refresh_top_products_mv,
)

__all__ = [
    "daily_sales_summary",
    "sales_by_city",
    "payment_method_analysis",
    "order_status_distribution",
    "top_products",
    "AGGREGATION_REGISTRY",
    "list_available_aggregations",
    "execute_aggregation_by_name",
    "ensure_mv_indexes",
    "refresh_daily_sales_mv",
    "refresh_top_products_mv",
    "refresh_all_materialized_views",
    "get_daily_sales_mv",
    "get_top_products_mv",
    "get_mv_refresh_status",
]
