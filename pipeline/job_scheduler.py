from pathlib import Path
import sys
import time
from datetime import datetime

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DATABASE, VALIDATED_COLLECTION, QUARANTINE_COLLECTION
from src.materialized_views import refresh_all_materialized_views

JOBS_LOG_COLLECTION = "job_logs"


def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]


def log_job_execution(db, job_name, start_time, end_time, status, details=None, error_msg=None):
    """Save job execution log to MongoDB job_logs collection."""
    coll = db[JOBS_LOG_COLLECTION]
    duration_sec = round(end_time - start_time, 4)
    log_doc = {
        "job_name": job_name,
        "start_time": datetime.fromtimestamp(start_time).isoformat(),
        "end_time": datetime.fromtimestamp(end_time).isoformat(),
        "duration_seconds": duration_sec,
        "status": status,
        "details": details or {},
        "error": error_msg,
        "created_at": time.time()
    }
    coll.insert_one(log_doc)
    log_doc["_id"] = str(log_doc["_id"])
    return log_doc


# ============================================================
# JOB 1: Refresh Materialized Views Job
# ============================================================

def run_refresh_mv_job(db=None):
    """
    Job 1: Performs incremental refresh on all Materialized Views.
    """
    if db is None:
        db = get_db()

    start_ts = time.time()
    job_name = "refresh_materialized_views_job"

    try:
        mv_results = refresh_all_materialized_views(db, incremental=True)
        end_ts = time.time()
        log_doc = log_job_execution(
            db,
            job_name=job_name,
            start_time=start_ts,
            end_time=end_ts,
            status="SUCCESS",
            details=mv_results
        )
        return {"status": "SUCCESS", "job_name": job_name, "log": log_doc}
    except Exception as e:
        end_ts = time.time()
        log_doc = log_job_execution(
            db,
            job_name=job_name,
            start_time=start_ts,
            end_time=end_ts,
            status="FAILED",
            error_msg=str(e)
        )
        return {"status": "FAILED", "job_name": job_name, "error": str(e), "log": log_doc}


# ============================================================
# JOB 2: Periodic Data Pipeline Audit Job
# ============================================================

def run_periodic_audit_job(db=None):
    """
    Job 2: Audits collection counts (validated, quarantine) and reports health.
    """
    if db is None:
        db = get_db()

    start_ts = time.time()
    job_name = "periodic_system_audit_job"

    try:
        val_count = db[VALIDATED_COLLECTION].count_documents({})
        quar_count = db[QUARANTINE_COLLECTION].count_documents({})
        
        audit_details = {
            "validated_orders_count": val_count,
            "quarantine_orders_count": quar_count,
            "total_processed_orders": val_count + quar_count,
            "system_health": "HEALTHY"
        }

        end_ts = time.time()
        log_doc = log_job_execution(
            db,
            job_name=job_name,
            start_time=start_ts,
            end_time=end_ts,
            status="SUCCESS",
            details=audit_details
        )
        return {"status": "SUCCESS", "job_name": job_name, "log": log_doc}
    except Exception as e:
        end_ts = time.time()
        log_doc = log_job_execution(
            db,
            job_name=job_name,
            start_time=start_ts,
            end_time=end_ts,
            status="FAILED",
            error_msg=str(e)
        )
        return {"status": "FAILED", "job_name": job_name, "error": str(e), "log": log_doc}


# Job Registry mapping
JOBS_REGISTRY = {
    "refresh_materialized_views_job": run_refresh_mv_job,
    "periodic_system_audit_job": run_periodic_audit_job
}


def run_job_by_name(job_name, db=None):
    """Trigger a job manually by name."""
    if job_name not in JOBS_REGISTRY:
        raise ValueError(f"Unknown job name: '{job_name}'. Available: {list(JOBS_REGISTRY.keys())}")
    
    return JOBS_REGISTRY[job_name](db)


def get_job_logs(db=None, limit=20):
    """Retrieve history of job execution logs."""
    if db is None:
        db = get_db()
    coll = db[JOBS_LOG_COLLECTION]
    return list(coll.find({}, {"_id": 0}).sort("created_at", -1).limit(limit))


if __name__ == "__main__":
    print("=" * 60)
    print("TESTING SCHEDULED JOBS (PHASE 4)")
    print("=" * 60)

    db = get_db()

    # Run Job 1 manually
    res1 = run_refresh_mv_job(db)
    print("Job 1 Result:", res1["status"])

    # Run Job 2 manually
    res2 = run_periodic_audit_job(db)
    print("Job 2 Result:", res2["status"])

    # Print logs
    logs = get_job_logs(db, limit=5)
    print(f"\nExecution Logs ({len(logs)} entries):")
    for l in logs:
        print(f" - [{l['status']}] {l['job_name']} @ {l['start_time']} (Duration: {l['duration_seconds']}s)")
