from pathlib import Path
import sys
import time

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient, UpdateOne
from config.settings import MONGO_URI, MONGO_DATABASE, VALIDATED_COLLECTION

MV_DAILY_SALES = "mv_daily_sales_summary"
MV_TOP_PRODUCTS = "mv_top_products_summary"
WATERMARK_COLLECTION = "mv_watermarks"


def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]


def get_last_watermark(db, mv_name):
    """Get last processed timestamp watermark for incremental refresh."""
    coll = db[WATERMARK_COLLECTION]
    doc = coll.find_one({"mv_name": mv_name})
    return doc.get("last_at_ingested", 0.0) if doc else 0.0


def set_last_watermark(db, mv_name, watermark_ts):
    """Update timestamp watermark for incremental refresh."""
    coll = db[WATERMARK_COLLECTION]
    coll.update_one(
        {"mv_name": mv_name},
        {"$set": {"last_at_ingested": watermark_ts, "updated_at": time.time()}},
        upsert=True
    )


# ============================================================
# 1. MATERIALIZED VIEW: daily_sales_summary
# ============================================================

def refresh_daily_sales_summary(db, incremental=True):
    """
    Refresh daily_sales_summary view.
    If incremental=True, processes only orders ingested after last_watermark and merges results.
    """
    validated = db[VALIDATED_COLLECTION]
    mv_coll = db[MV_DAILY_SALES]

    match_stage = {}
    last_wm = 0.0
    if incremental:
        last_wm = get_last_watermark(db, MV_DAILY_SALES)
        if last_wm > 0:
            match_stage = {"at_ingested": {"$gt": last_wm}}

    pipeline = []
    if match_stage:
        pipeline.append({"$match": match_stage})

    # Group daily sales
    pipeline.extend([
        {
            "$group": {
                "_id": "$order_date",
                "inc_orders": {"$sum": 1},
                "inc_revenue": {"$sum": "$total_amount"},
                "latest_ingested": {"$max": "$at_ingested"}
            }
        },
        {"$match": {"_id": {"$ne": None}}}
    ])

    aggregated_chunks = list(validated.aggregate(pipeline))

    if not aggregated_chunks:
        return {
            "view_name": "daily_sales_summary",
            "status": "NO_NEW_DATA",
            "updated_documents": 0,
            "incremental": incremental
        }

    max_ingested_ts = last_wm
    operations = []

    for chunk in aggregated_chunks:
        date_key = chunk["_id"]
        inc_orders = chunk["inc_orders"]
        inc_rev = chunk["inc_revenue"]
        chunk_ts = chunk.get("latest_ingested", 0.0) or 0.0
        if chunk_ts > max_ingested_ts:
            max_ingested_ts = chunk_ts

        if incremental and last_wm > 0:
            # Incremental update: increment existing counts
            operations.append(
                UpdateOne(
                    {"_id": date_key},
                    {
                        "$inc": {
                            "total_orders": inc_orders,
                            "total_revenue": inc_rev
                        },
                        "$set": {
                            "order_date": date_key,
                            "last_updated": time.time()
                        }
                    },
                    upsert=True
                )
            )
        else:
            # Full recalculation / initial sync
            operations.append(
                UpdateOne(
                    {"_id": date_key},
                    {
                        "$set": {
                            "order_date": date_key,
                            "total_orders": inc_orders,
                            "total_revenue": round(inc_rev, 2),
                            "last_updated": time.time()
                        }
                    },
                    upsert=True
                )
            )

    if operations:
        mv_coll.bulk_write(operations)
        if max_ingested_ts > 0:
            set_last_watermark(db, MV_DAILY_SALES, max_ingested_ts)

    return {
        "view_name": "daily_sales_summary",
        "status": "SUCCESS",
        "updated_documents": len(operations),
        "incremental": incremental
    }


# ============================================================
# 2. MATERIALIZED VIEW: top_products_summary
# ============================================================

def refresh_top_products_summary(db, incremental=True):
    """
    Refresh top_products_summary view.
    Aggregates top selling products and saves into mv_top_products_summary collection.
    """
    mv_coll = db[MV_TOP_PRODUCTS]
    last_wm = get_last_watermark(db, MV_TOP_PRODUCTS) if incremental else 0.0

    from src.aggregations import get_top_products
    products = get_top_products(db, limit=50)

    if not products:
        return {
            "view_name": "top_products_summary",
            "status": "NO_NEW_DATA",
            "updated_documents": 0,
            "incremental": incremental
        }

    operations = []
    now_ts = time.time()
    for prod in products:
        p_name = prod["product_name"]
        operations.append(
            UpdateOne(
                {"_id": p_name},
                {
                    "$set": {
                        "product_name": p_name,
                        "total_quantity_sold": prod["total_quantity_sold"],
                        "total_revenue": prod["total_revenue"],
                        "last_updated": now_ts
                    }
                },
                upsert=True
            )
        )

    if operations:
        mv_coll.bulk_write(operations)
        set_last_watermark(db, MV_TOP_PRODUCTS, now_ts)

    return {
        "view_name": "top_products_summary",
        "status": "SUCCESS",
        "updated_documents": len(operations),
        "incremental": incremental
    }



def refresh_all_materialized_views(db=None, incremental=True):
    """
    Triggers refresh for all Materialized Views.
    """
    if db is None:
        db = get_db()

    res1 = refresh_daily_sales_summary(db, incremental=incremental)
    res2 = refresh_top_products_summary(db, incremental=incremental)

    return {
        "daily_sales_summary": res1,
        "top_products_summary": res2,
        "timestamp": time.time()
    }


def get_materialized_view_data(mv_name, db=None, limit=20):
    """Retrieve contents of a Materialized View collection."""
    if db is None:
        db = get_db()

    coll_map = {
        "daily_sales_summary": MV_DAILY_SALES,
        "top_products_summary": MV_TOP_PRODUCTS
    }
    if mv_name not in coll_map:
        raise ValueError(f"Unknown materialized view: {mv_name}")

    coll = db[coll_map[mv_name]]
    return list(coll.find({}, {"_id": 0}).limit(limit))


if __name__ == "__main__":
    print("=" * 60)
    print("TESTING MATERIALIZED VIEWS REFRESH (PHASE 3)")
    print("=" * 60)

    db = get_db()
    res = refresh_all_materialized_views(db, incremental=False)
    print("Full Refresh Result:", res)

    inc_res = refresh_all_materialized_views(db, incremental=True)
    print("Incremental Refresh Result:", inc_res)
