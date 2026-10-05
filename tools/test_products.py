from pymongo import MongoClient
import json
import sys

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

client = MongoClient('mongodb://localhost:27017')
db = client['midterm_data_pipeline2']
coll = db['orders_validated']

# Fetch sample documents with items_json
docs = coll.find({"items_json": {"$ne": None}}, {"items_json": 1}).limit(5)
for d in docs:
    val = d.get('items_json')
    print("items_json sample type:", type(val), "content:", repr(val[:100]) if isinstance(val, str) else val)
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            print("  Parsed JSON:", parsed)
        except Exception as e:
            print("  Parse error:", e)
