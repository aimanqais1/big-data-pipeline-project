"""
Phase 2 — Step 1C: Analytical Index Manager & Explain Execution Module.
Manages the 3 approved Phase 2 indexes on `orders_validated` and provides
`explain("executionStats")` inspection for the 3 primary Explain candidate queries:
1. orders_by_city_and_status -> idx_city_status_date: (city ASC, status ASC, order_date DESC)
2. customer_order_history -> idx_customer_date: (customer_id ASC, order_date DESC)
3. high_value_orders_by_date_range -> idx_order_date_desc: (order_date DESC)
"""
import logging
from typing import Dict, Any, List

from pymongo import ASCENDING, DESCENDING

from config.settings import MONGO_DB_NAME, VALIDATED_COLLECTION
from src.mongo_setup import get_database

logger = logging.getLogger(__name__)

# Approved Phase 2 Analytical Index Definitions (Strictly 3 Indexes)
PHASE2_INDEX_SPECS: List[Dict[str, Any]] = [
    {
        "name": "idx_city_status_date",
        "keys": [("city", ASCENDING), ("status", ASCENDING), ("order_date", DESCENDING)],
        "type": "Compound Index",
        "primary_query": "orders_by_city_and_status",
        "rationale": "ESR compound index supporting equality on city and status and descending sort on order_date.",
    },
    {
        "name": "idx_customer_date",
        "keys": [("customer_id", ASCENDING), ("order_date", DESCENDING)],
        "type": "Compound Index",
        "primary_query": "customer_order_history",
        "rationale": "High-selectivity customer_id lookup paired with descending order_date sort.",
    },
    {
        "name": "idx_order_date_desc",
        "keys": [("order_date", DESCENDING)],
        "type": "Single-Field Descending Index",
        "primary_query": "high_value_orders_by_date_range",
        "rationale": "Supports ISO-8601 lexical date range filtering and descending order_date sort.",
    },
]


def create_phase2_indexes(db_name: str = MONGO_DB_NAME) -> Dict[str, Any]:
    """
    Create the 3 approved Phase 2 analytical indexes on `orders_validated`.
    Does not drop or alter existing Phase 1 indexes (`_id_`, `uniq_id_order`).
    """
    db = get_database(db_name=db_name)
    col = db[VALIDATED_COLLECTION]

    created_names: List[str] = []
    for spec in PHASE2_INDEX_SPECS:
        idx_name = col.create_index(spec["keys"], name=spec["name"], unique=False)
        created_names.append(idx_name)
        logger.info(f"Ensured index '{idx_name}' on {db_name}.{VALIDATED_COLLECTION}: {spec['keys']}")

    all_indexes = [
        {
            "name": idx["name"],
            "key": dict(idx["key"]),
            "unique": idx.get("unique", False),
        }
        for idx in col.list_indexes()
    ]

    return {
        "database": db_name,
        "collection": VALIDATED_COLLECTION,
        "created_indexes": created_names,
        "all_indexes": all_indexes,
    }


def extract_plan_stages(stage_node: Dict[str, Any]) -> List[str]:
    """Recursively extract the sequence of execution stage names from a MongoDB plan tree."""
    if not stage_node or not isinstance(stage_node, dict):
        return []
    stages = []
    if "stage" in stage_node:
        stage_label = stage_node["stage"]
        if "indexName" in stage_node:
            stage_label = f"{stage_label}({stage_node['indexName']})"
        stages.append(stage_label)
    if "inputStage" in stage_node:
        stages.extend(extract_plan_stages(stage_node["inputStage"]))
    if "inputStages" in stage_node:
        for child in stage_node["inputStages"]:
            stages.extend(extract_plan_stages(child))
    return stages


def explain_cursor_execution_stats(cursor) -> Dict[str, Any]:
    """
    Execute `.explain()` on a PyMongo cursor and extract the key `executionStats`
    and winning plan details into a clean, JSON-serializable dictionary.
    """
    raw_explain = cursor.explain()
    exec_stats = raw_explain.get("executionStats", {})
    query_planner = raw_explain.get("queryPlanner", {})
    winning_plan = query_planner.get("winningPlan", {})
    exec_stages = exec_stats.get("executionStages", {})

    plan_chain = " <- ".join(extract_plan_stages(exec_stages or winning_plan))

    return {
        "nReturned": exec_stats.get("nReturned"),
        "executionTimeMillis": exec_stats.get("executionTimeMillis"),
        "totalKeysExamined": exec_stats.get("totalKeysExamined"),
        "totalDocsExamined": exec_stats.get("totalDocsExamined"),
        "plan_summary": plan_chain,
        "winning_plan": winning_plan,
        "execution_stages": exec_stages,
    }
