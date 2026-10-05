from pathlib import Path
import sys

# Add project root to Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


def get_mongo_client():
    """
    Create and return a MongoDB client.
    """
    return MongoClient(MONGO_URI)


def get_database(client):
    """
    Return the project database.
    """
    return client[MONGO_DATABASE]


def get_collections(database):
    """
    Return the project collections.
    """
    return {
        "raw": database[RAW_COLLECTION],
        "validated": database[VALIDATED_COLLECTION],
        "quarantine": database[QUARANTINE_COLLECTION],
    }


def reset_and_initialize_db(drop_existing=False):
    """
    Initialize MongoDB database and create unique indexes.
    If drop_existing=True, clears existing collections for a clean run.
    """
    client = get_mongo_client()
    try:
        client.admin.command("ping")
        db = get_database(client)
        collections = get_collections(db)

        print("=" * 60)
        print("MONGODB INITIALIZATION & SETUP")
        print("=" * 60)
        print(f"MongoDB URI : {MONGO_URI}")
        print(f"Database    : {MONGO_DATABASE}")

        if drop_existing:
            print("Clearing old collection data for fresh initialization...")
            collections["raw"].delete_many({})
            collections["validated"].delete_many({})
            collections["quarantine"].delete_many({})
            print("Done clearing collections.")

        # Ensure Unique Index on order_id for orders_validated (Idempotency requirement)
        collections["validated"].create_index("order_id", unique=True, name="idx_order_id_unique")

        print("Raw Collection        :", collections["raw"].name)
        print("Validated Collection  :", collections["validated"].name, "(Unique Index on order_id)")
        print("Quarantine Collection :", collections["quarantine"].name)
        print("=" * 60)
        print("MONGODB READY FOR PIPELINE EXECUTION!")
        print("=" * 60)
    finally:
        client.close()


if __name__ == "__main__":
    reset_and_initialize_db(drop_existing=False)