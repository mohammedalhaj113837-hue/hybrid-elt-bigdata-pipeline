from pathlib import Path
import sys
import json

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Reconfigure stdout for Windows console UTF-8 support
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DATABASE, VALIDATED_COLLECTION


def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]


# Expression helper to safely convert total_amount string (e.g. "161,000.00") to double
NUMERIC_AMOUNT_EXPR = {
    "$convert": {
        "input": {
            "$replaceAll": {
                "input": {
                    "$replaceAll": {
                        "input": {"$ifNull": ["$total_amount", "0"]},
                        "find": ",",
                        "replacement": ""
                    }
                },
                "find": " ",
                "replacement": ""
            }
        },
        "to": "double",
        "onError": 0.0,
        "onNull": 0.0
    }
}


# ============================================================
# THE 5 AGGREGATION REPORTS
# ============================================================

def get_sales_by_city(db):
    """
    Report 1: Total revenue, total orders, and average order amount grouped by city.
    """
    collection = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$addFields": {"num_amount": NUMERIC_AMOUNT_EXPR}},
        {
            "$group": {
                "_id": "$city",
                "total_orders": {"$sum": 1},
                "total_revenue": {"$sum": "$num_amount"},
                "avg_order_value": {"$avg": "$num_amount"}
            }
        },
        {"$sort": {"total_revenue": -1}},
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "total_orders": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "avg_order_value": {"$round": ["$avg_order_value", 2]}
            }
        }
    ]
    return list(collection.aggregate(pipeline))


def get_top_products(db, limit=10):
    """
    Report 2: Best-selling products by total quantity sold and revenue generated.
    Supports both array 'items' and JSON string 'items_json'.
    """
    collection = db[VALIDATED_COLLECTION]
    product_stats = {}

    # Query items_json or items
    cursor = collection.find(
        {"$or": [{"items": {"$ne": None}}, {"items_json": {"$ne": None}}]},
        {"items": 1, "items_json": 1}
    ).limit(50000)

    for doc in cursor:
        items_data = doc.get("items")
        if not items_data and doc.get("items_json"):
            try:
                raw_json = doc.get("items_json")
                if isinstance(raw_json, str):
                    items_data = json.loads(raw_json)
            except Exception:
                items_data = []

        if not isinstance(items_data, list):
            continue

        for item in items_data:
            if not isinstance(item, dict):
                continue
            
            name = item.get("name") or item.get("product_name") or item.get("item_name") or "منتج غير معروف"
            
            raw_qty = item.get("qty") or item.get("quantity") or 1
            try:
                # Convert Arabic digits if present
                if isinstance(raw_qty, str):
                    raw_qty = raw_qty.translate(str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789"))
                qty = int(raw_qty)
            except Exception:
                qty = 1

            raw_tot = item.get("total") or item.get("price") or item.get("unit_price") or 0.0
            try:
                rev = float(raw_tot)
            except Exception:
                rev = 0.0

            if name not in product_stats:
                product_stats[name] = {"total_quantity_sold": 0, "total_revenue": 0.0}
            
            product_stats[name]["total_quantity_sold"] += qty
            product_stats[name]["total_revenue"] += rev

    results = [
        {
            "product_name": p_name,
            "total_quantity_sold": stats["total_quantity_sold"],
            "total_revenue": round(stats["total_revenue"], 2)
        }
        for p_name, stats in product_stats.items()
    ]
    results.sort(key=lambda x: x["total_quantity_sold"], reverse=True)
    return results[:limit]


def get_top_customers(db, limit=10):
    """
    Report 3: Top spending customers by total expenditure and completed orders.
    """
    collection = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$addFields": {"num_amount": NUMERIC_AMOUNT_EXPR}},
        {
            "$group": {
                "_id": "$customer_id",
                "total_spent": {"$sum": "$num_amount"},
                "total_orders": {"$sum": 1},
                "avg_order_value": {"$avg": "$num_amount"}
            }
        },
        {"$sort": {"total_spent": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id",
                "total_spent": {"$round": ["$total_spent", 2]},
                "total_orders": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]}
            }
        }
    ]
    return list(collection.aggregate(pipeline))


def get_sales_by_period(db):
    """
    Report 4: Daily sales summary showing total orders and total revenue per date.
    """
    collection = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$addFields": {"num_amount": NUMERIC_AMOUNT_EXPR}},
        {
            "$group": {
                "_id": "$order_date",
                "total_orders": {"$sum": 1},
                "total_revenue": {"$sum": "$num_amount"},
                "avg_order_value": {"$avg": "$num_amount"}
            }
        },
        {"$sort": {"_id": -1}},
        {
            "$project": {
                "_id": 0,
                "order_date": "$_id",
                "total_orders": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "avg_order_value": {"$round": ["$avg_order_value", 2]}
            }
        }
    ]
    return list(collection.aggregate(pipeline))


def get_orders_by_status(db):
    """
    Report 5: Distribution of orders grouped by delivery status (DELIVERED, PENDING, CANCELLED).
    """
    collection = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$addFields": {"num_amount": NUMERIC_AMOUNT_EXPR}},
        {
            "$group": {
                "_id": "$status",
                "order_count": {"$sum": 1},
                "total_amount": {"$sum": "$num_amount"}
            }
        },
        {"$sort": {"order_count": -1}},
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "order_count": 1,
                "total_amount": {"$round": ["$total_amount", 2]}
            }
        }
    ]
    return list(collection.aggregate(pipeline))


# Registry dictionary mapping report names to functions
REPORTS_REGISTRY = {
    "sales_by_city": get_sales_by_city,
    "top_products": get_top_products,
    "top_customers": get_top_customers,
    "sales_by_period": get_sales_by_period,
    "orders_by_status": get_orders_by_status,
}


def run_report(report_name, db=None):
    """
    Executes a report by name.
    """
    if report_name not in REPORTS_REGISTRY:
        raise ValueError(f"Unknown report name: '{report_name}'. Available: {list(REPORTS_REGISTRY.keys())}")
    
    if db is None:
        db = get_db()
    
    return REPORTS_REGISTRY[report_name](db)


if __name__ == "__main__":
    print("=" * 60)
    print("TESTING AGGREGATION REPORTS (PHASE 2)")
    print("=" * 60)

    db = get_db()
    for name, func in REPORTS_REGISTRY.items():
        results = func(db)
        print(f"\n--- Report: {name} (Returned {len(results)} rows) ---")
        for row in results[:3]:  # Print top 3 sample rows
            print(f"  {row}")
