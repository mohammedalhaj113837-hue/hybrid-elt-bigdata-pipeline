from pymongo import MongoClient
import json
import sys

# Configure UTF-8 for console
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

client = MongoClient('mongodb://localhost:27017')
db = client['midterm_data_pipeline2']
coll = db['orders_validated']

pipeline_city = [
    {
        "$addFields": {
            "num_amount": {
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
        }
    },
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

res = list(coll.aggregate(pipeline_city))
print("CITY SALES RESULT (Sample 3):")
for r in res[:3]:
    print(" ", r)
