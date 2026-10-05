from pathlib import Path
import sys
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.queries_indexes import (
    get_db,
    create_indexes,
    drop_indexes,
    query_orders_by_customer,
    query_orders_by_city_and_status,
    query_orders_by_date_range,
    query_orders_by_email_or_phone,
    query_high_value_orders,
    compare_query_explain_stats,
    INDEX_DEFINITIONS
)


def test_index_definitions():
    """Verify index count and presence of compound index."""
    assert len(INDEX_DEFINITIONS) >= 3
    compound_indexes = [idx for idx in INDEX_DEFINITIONS if idx["type"] == "Compound Index"]
    assert len(compound_indexes) >= 1


def test_create_and_drop_indexes():
    """Verify index creation and cleanup."""
    db = get_db()
    created = create_indexes(db)
    assert len(created) == 3
    drop_indexes(db)


def test_queries_execution():
    """Verify all 5 queries execute without errors."""
    db = get_db()
    q1 = query_orders_by_customer(db, limit=5)
    q2 = query_orders_by_city_and_status(db, limit=5)
    q3 = query_orders_by_date_range(db, limit=5)
    q4 = query_orders_by_email_or_phone(db, limit=5)
    q5 = query_high_value_orders(db, limit=5)
    
    assert isinstance(q1, list)
    assert isinstance(q2, list)
    assert isinstance(q3, list)
    assert isinstance(q4, list)
    assert isinstance(q5, list)


def test_explain_comparison():
    """Verify explain stats produces valid comparisons."""
    db = get_db()
    results = compare_query_explain_stats(db)
    assert len(results) == 3
    for res in results:
        assert "before_index" in res
        assert "after_index" in res
        assert "winningPlanStage" in res["before_index"]
        assert "winningPlanStage" in res["after_index"]
