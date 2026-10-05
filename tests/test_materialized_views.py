from pathlib import Path
import sys
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.materialized_views import (
    get_db,
    refresh_daily_sales_summary,
    refresh_top_products_summary,
    refresh_all_materialized_views,
    get_materialized_view_data,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS
)


def test_full_and_incremental_refresh():
    """Verify full and incremental refresh of Materialized Views."""
    db = get_db()
    
    # 1. Full Refresh
    full_res = refresh_all_materialized_views(db, incremental=False)
    assert "daily_sales_summary" in full_res
    assert "top_products_summary" in full_res
    assert full_res["daily_sales_summary"]["status"] in ["SUCCESS", "NO_NEW_DATA"]

    # 2. Incremental Refresh
    inc_res = refresh_all_materialized_views(db, incremental=True)
    assert "daily_sales_summary" in inc_res
    assert "top_products_summary" in inc_res
    assert inc_res["daily_sales_summary"]["status"] in ["SUCCESS", "NO_NEW_DATA"]


def test_get_materialized_view_data():
    """Verify retrieving data from Materialized Views."""
    db = get_db()
    data1 = get_materialized_view_data("daily_sales_summary", db)
    data2 = get_materialized_view_data("top_products_summary", db)

    assert isinstance(data1, list)
    assert isinstance(data2, list)

    with pytest.raises(ValueError):
        get_materialized_view_data("non_existent_mv", db)
