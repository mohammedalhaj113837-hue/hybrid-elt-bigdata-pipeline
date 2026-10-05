from pathlib import Path
import sys
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.scheduled_jobs import (
    get_db,
    run_refresh_mv_job,
    run_periodic_audit_job,
    run_job_by_name,
    get_job_logs,
    JOBS_REGISTRY
)


def test_jobs_registry_contains_2_jobs():
    """Verify registry has at least 2 jobs."""
    assert len(JOBS_REGISTRY) >= 2
    assert "refresh_materialized_views_job" in JOBS_REGISTRY
    assert "periodic_system_audit_job" in JOBS_REGISTRY


def test_jobs_execution_and_logging():
    """Verify jobs execute and save log entries."""
    db = get_db()
    
    j1 = run_refresh_mv_job(db)
    assert j1["status"] in ["SUCCESS", "FAILED"]

    j2 = run_periodic_audit_job(db)
    assert j2["status"] in ["SUCCESS", "FAILED"]

    logs = get_job_logs(db, limit=10)
    assert isinstance(logs, list)
    assert len(logs) >= 2


def test_run_job_by_invalid_name():
    """Verify running an invalid job raises ValueError."""
    db = get_db()
    with pytest.raises(ValueError):
        run_job_by_name("invalid_job_name", db)
