from pymongo import MongoClient
import json

client = MongoClient('mongodb://localhost:27017')
db = client['midterm_data_pipeline2']
doc = db['orders_validated'].find_one()

print("Document keys:", list(doc.keys()))
print("total_amount value:", repr(doc.get('total_amount')))
print("items_json value:", repr(doc.get('items_json')))
