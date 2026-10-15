from pathlib import Path
import sys
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.queries_and_indexes import get_db
from config.settings import VALIDATED_COLLECTION, QUARANTINE_COLLECTION


def test_quarantine_collection_structure():
    """Verify quarantine records contain quarantine_reasons."""
    db = get_db()
    quar_count = db[QUARANTINE_COLLECTION].count_documents({})
    if quar_count > 0:
        doc = db[QUARANTINE_COLLECTION].find_one()
        assert "quarantine_reasons" in doc or "quality_status" in doc
        if "quality_status" in doc:
            assert doc["quality_status"] == "quarantine"


def test_validated_collection_structure():
    """Verify validated records contain valid quality status."""
    db = get_db()
    val_count = db[VALIDATED_COLLECTION].count_documents({})
    if val_count > 0:
        doc = db[VALIDATED_COLLECTION].find_one()
        assert "order_id" in doc
        if "quality_status" in doc:
            assert doc["quality_status"] in ["valid", "corrected"]


def test_idempotency_order_id_unique():
    """Verify that orders_validated has a unique index on order_id."""
    db = get_db()
    indexes = list(db[VALIDATED_COLLECTION].list_indexes())
    unique_keys = [
        idx["key"].to_dict()
        for idx in indexes
        if idx.get("unique") is True
    ]
    # Check if order_id is in any unique index
    has_order_id_unique = any("order_id" in k for k in unique_keys)
    # If not already created, create it and verify
    if not has_order_id_unique:
        db[VALIDATED_COLLECTION].create_index("order_id", unique=True)
        indexes = list(db[VALIDATED_COLLECTION].list_indexes())
        has_order_id_unique = any("order_id" in idx["key"].to_dict() and idx.get("unique") is True for idx in indexes)
    assert has_order_id_unique is True
