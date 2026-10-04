"""
Phase 2 Query & Index Package for Big Data E-Commerce Pipeline.
Exposes the 5 approved practical queries and the 3 approved index/explain helpers.
"""
from src.queries.order_queries import (
    orders_by_city_and_status,
    customer_order_history,
    high_value_orders_by_date_range,
    orders_by_product_sku,
    payment_settlement_audit,
    list_available_queries,
    execute_query_by_name,
    QUERY_REGISTRY,
)
from src.queries.index_manager import (
    PHASE2_INDEX_SPECS,
    create_phase2_indexes,
    explain_cursor_execution_stats,
)

__all__ = [
    "orders_by_city_and_status",
    "customer_order_history",
    "high_value_orders_by_date_range",
    "orders_by_product_sku",
    "payment_settlement_audit",
    "list_available_queries",
    "execute_query_by_name",
    "QUERY_REGISTRY",
    "PHASE2_INDEX_SPECS",
    "create_phase2_indexes",
    "explain_cursor_execution_stats",
]
