from pathlib import Path
import sys
import time

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient, ASCENDING, DESCENDING
from config.settings import MONGO_URI, MONGO_DATABASE, VALIDATED_COLLECTION


def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]


# ============================================================
# 1. THE 5 PRACTICAL QUERIES
# ============================================================

def _clean_docs(cursor):
    docs = []
    for doc in cursor:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return docs


def query_orders_by_customer(db, customer_id="CUST-1001", limit=10):
    """
    Query 1: Retrieve order history for a specific customer sorted by order date descending.
    Matches Index: Compound Index on ('customer_id', 1), ('order_date', -1)
    """
    collection = db[VALIDATED_COLLECTION]
    cursor = collection.find({"customer_id": customer_id}).sort("order_date", DESCENDING).limit(limit)
    return _clean_docs(cursor)


def query_orders_by_city_and_status(db, city="Sana'a", status="DELIVERED", limit=10):
    """
    Query 2: Filter orders by city and delivery status.
    Matches Index: Single Index on ('city', 1) or Single Index on ('status', 1)
    """
    collection = db[VALIDATED_COLLECTION]
    cursor = collection.find({"city": city, "status": status}).limit(limit)
    return _clean_docs(cursor)


def query_orders_by_date_range(db, start_date="2024-01-01", end_date="2024-12-31", limit=10):
    """
    Query 3: Filter orders within a date range.
    Matches Index: Index on ('order_date', 1) or Compound Index
    """
    collection = db[VALIDATED_COLLECTION]
    cursor = collection.find({
        "order_date": {"$gte": start_date, "$lte": end_date}
    }).limit(limit)
    return _clean_docs(cursor)


def query_orders_by_email_or_phone(db, contact_value="user@example.com", limit=10):
    """
    Query 4: Search customer order by email or phone.
    """
    collection = db[VALIDATED_COLLECTION]
    cursor = collection.find({
        "$or": [
            {"email": contact_value},
            {"customer_email": contact_value},
            {"phone": contact_value},
            {"customer_phone": contact_value}
        ]
    }).limit(limit)
    return _clean_docs(cursor)


def query_high_value_orders(db, min_amount=500.0, limit=10):
    """
    Query 5: Filter high-value orders where total_amount >= min_amount.
    """
    collection = db[VALIDATED_COLLECTION]
    cursor = collection.find({
        "total_amount": {"$gte": min_amount}
    }).sort("total_amount", DESCENDING).limit(limit)
    return _clean_docs(cursor)


# ============================================================
# 2. INDEX MANAGEMENT (3 INDEXES, 1 COMPOUND)
# ============================================================

INDEX_DEFINITIONS = [
    {
        "name": "idx_customer_order_date",
        "keys": [("customer_id", ASCENDING), ("order_date", DESCENDING)],
        "type": "Compound Index",
        "description": "Optimizes customer order history lookups sorted by date"
    },
    {
        "name": "idx_status",
        "keys": [("status", ASCENDING)],
        "type": "Single Index",
        "description": "Accelerates status-based filtering (DELIVERED, PENDING, CANCELLED)"
    },
    {
        "name": "idx_city",
        "keys": [("city", ASCENDING)],
        "type": "Single Index",
        "description": "Accelerates regional/city-based filtering"
    }
]


def create_indexes(db):
    """
    Create the 3 required indexes on validated collection.
    """
    collection = db[VALIDATED_COLLECTION]
    created = []
    for idx in INDEX_DEFINITIONS:
        name = collection.create_index(idx["keys"], name=idx["name"])
        created.append({"name": name, "type": idx["type"], "description": idx["description"]})
    return created


def drop_indexes(db):
    """
    Drop custom indexes to allow before/after comparison testing.
    """
    collection = db[VALIDATED_COLLECTION]
    for idx in INDEX_DEFINITIONS:
        try:
            collection.drop_index(idx["name"])
        except Exception:
            pass


def get_existing_indexes(db):
    """
    List all current indexes on validated collection.
    """
    collection = db[VALIDATED_COLLECTION]
    return list(collection.list_indexes())


# ============================================================
# 3. EXPLAIN ANALYSIS (executionStats)
# ============================================================

def run_explain_for_query(collection, find_clause, sort_clause=None):
    """
    Executes explain("executionStats") for a given query filter and optional sort.
    """
    cursor = collection.find(find_clause)
    if sort_clause:
        cursor = cursor.sort(sort_clause)
    
    explanation = collection.database.command(
        "explain",
        {
            "find": collection.name,
            "filter": find_clause,
            **({"sort": dict(sort_clause)} if sort_clause else {})
        },
        verbosity="executionStats"
    )

    stats = explanation.get("executionStats", {})
    query_planner = explanation.get("queryPlanner", {})
    winning_plan = query_planner.get("winningPlan", {})

    stage = winning_plan.get("stage", "UNKNOWN")
    if stage == "LIMIT" and "inputStage" in winning_plan:
        stage = winning_plan["inputStage"].get("stage", stage)

    return {
        "executionTimeMillis": stats.get("executionTimeMillis", 0),
        "totalDocsExamined": stats.get("totalDocsExamined", 0),
        "nReturned": stats.get("nReturned", 0),
        "winningPlanStage": stage,
        "isIndexUsed": stage != "COLLSCAN"
    }


def compare_query_explain_stats(db):
    """
    Runs explain analysis for 3 queries BEFORE and AFTER index creation.
    Returns structured comparison results.
    """
    collection = db[VALIDATED_COLLECTION]

    queries_to_test = [
        {
            "id": "Q1_customer_orders",
            "name": "Customer Orders Sorted by Date",
            "filter": {"customer_id": "CUST-1001"},
            "sort": [("order_date", DESCENDING)],
            "index_target": "Compound Index: idx_customer_order_date"
        },
        {
            "id": "Q2_city_status",
            "name": "Orders by City & Status",
            "filter": {"city": "Sana'a", "status": "DELIVERED"},
            "sort": None,
            "index_target": "Single Index: idx_city or idx_status"
        },
        {
            "id": "Q3_status_filter",
            "name": "Orders by Delivery Status",
            "filter": {"status": "DELIVERED"},
            "sort": None,
            "index_target": "Single Index: idx_status"
        }
    ]

    results = []

    # 1. Test BEFORE indexes (Drop indexes first)
    drop_indexes(db)
    time.sleep(0.2)

    for q in queries_to_test:
        before_stats = run_explain_for_query(collection, q["filter"], q["sort"])
        q_result = {
            "query_id": q["id"],
            "query_name": q["name"],
            "index_target": q["index_target"],
            "before_index": before_stats,
            "after_index": None,
            "performance_gain": None
        }
        results.append(q_result)

    # 2. Create Indexes
    create_indexes(db)
    time.sleep(0.2)

    # 3. Test AFTER indexes
    for i, q in enumerate(queries_to_test):
        after_stats = run_explain_for_query(collection, q["filter"], q["sort"])
        results[i]["after_index"] = after_stats
        
        before_docs = results[i]["before_index"]["totalDocsExamined"]
        after_docs = after_stats["totalDocsExamined"]
        
        doc_reduction = before_docs - after_docs
        doc_reduction_pct = (doc_reduction / max(before_docs, 1)) * 100

        results[i]["performance_gain"] = {
            "docs_examined_reduction": doc_reduction,
            "docs_examined_reduction_pct": round(doc_reduction_pct, 2),
            "stage_change": f"{results[i]['before_index']['winningPlanStage']} -> {after_stats['winningPlanStage']}"
        }

    return results


if __name__ == "__main__":
    print("=" * 60)
    print("TESTING QUERIES & INDEXES (PHASE 1)")
    print("=" * 60)
    
    db = get_db()
    
    # 1. Create indexes
    created = create_indexes(db)
    print("\nCreated Indexes:")
    for idx in created:
        print(f" - [{idx['type']}] {idx['name']}: {idx['description']}")

    # 2. Run explain analysis
    print("\nRunning Explain Comparison (Before vs After)...")
    comparison = compare_query_explain_stats(db)

    for res in comparison:
        print(f"\n--- {res['query_id']}: {res['query_name']} ---")
        print(f"  Target Index  : {res['index_target']}")
        print(f"  BEFORE Index  : Stage={res['before_index']['winningPlanStage']}, Docs Examined={res['before_index']['totalDocsExamined']}, Time={res['before_index']['executionTimeMillis']}ms")
        print(f"  AFTER Index   : Stage={res['after_index']['winningPlanStage']}, Docs Examined={res['after_index']['totalDocsExamined']}, Time={res['after_index']['executionTimeMillis']}ms")
        print(f"  Improvement   : {res['performance_gain']['stage_change']} (Docs Examined Reduced by {res['performance_gain']['docs_examined_reduction_pct']}%)")
