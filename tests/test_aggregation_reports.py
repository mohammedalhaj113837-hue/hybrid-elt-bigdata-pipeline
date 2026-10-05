from pathlib import Path
import sys
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.aggregations import (
    get_db,
    get_sales_by_city,
    get_top_products,
    get_top_customers,
    get_sales_by_period,
    get_orders_by_status,
    run_report,
    REPORTS_REGISTRY
)


def test_reports_registry_contains_5_reports():
    """Verify registry has at least 5 reports."""
    assert len(REPORTS_REGISTRY) == 5
    expected_reports = [
        "sales_by_city",
        "top_products",
        "top_customers",
        "sales_by_period",
        "orders_by_status"
    ]
    for r in expected_reports:
        assert r in REPORTS_REGISTRY


def test_individual_reports_execution():
    """Verify all 5 aggregation functions execute properly and return list."""
    db = get_db()
    r1 = get_sales_by_city(db)
    r2 = get_top_products(db)
    r3 = get_top_customers(db)
    r4 = get_sales_by_period(db)
    r5 = get_orders_by_status(db)

    assert isinstance(r1, list)
    assert isinstance(r2, list)
    assert isinstance(r3, list)
    assert isinstance(r4, list)
    assert isinstance(r5, list)


def test_run_report_by_name():
    """Verify run_report works for valid names and raises ValueError for invalid names."""
    db = get_db()
    result = run_report("sales_by_city", db)
    assert isinstance(result, list)

    with pytest.raises(ValueError):
        run_report("invalid_report_name", db)
