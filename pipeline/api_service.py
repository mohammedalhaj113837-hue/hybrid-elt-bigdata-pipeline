from pathlib import Path
import sys
import time

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any

from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DATABASE

from src.queries_indexes import (
    create_indexes,
    get_existing_indexes,
    compare_query_explain_stats,
    query_orders_by_customer,
    query_orders_by_city_and_status,
    query_orders_by_date_range,
    query_orders_by_email_or_phone,
    query_high_value_orders
)
from src.aggregations import run_report, REPORTS_REGISTRY
from src.materialized_views import refresh_all_materialized_views, get_materialized_view_data
from src.scheduled_jobs import run_job_by_name, get_job_logs, JOBS_REGISTRY
from src.file_router import choose_engine
from src.batch_loader import load_csv_to_raw


app = FastAPI(
    title="Big Data Phase 2 - Unified Operations API",
    description="Unified API interface for testing Ingestion, Queries, Indexes, Aggregations, Materialized Views, and Scheduled Jobs.",
    version="2.0.0"
)


def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]


class IngestRequest(BaseModel):
    file_path: Optional[str] = "data/small_sample.csv"


# ============================================================
# 1. HEALTH CHECK
# ============================================================

@app.get("/health", tags=["Health"])
def health_check():
    """Returns system and database health status."""
    try:
        db = get_db()
        db.command("ping")
        db_status = "CONNECTED"
    except Exception as e:
        db_status = f"DISCONNECTED: {str(e)}"

    return {
        "status": "UP",
        "database": db_status,
        "timestamp": time.time()
    }


# ============================================================
# 2. DATA INGESTION (MIDTERM PIPELINE GATEWAY)
# ============================================================

@app.post("/ingest", tags=["Ingestion"])
def ingest_data(request: Optional[IngestRequest] = None):
    """
    Triggers the midterm ingestion pipeline via file_router.py.
    """
    file_path_str = request.file_path if request and request.file_path else "data/small_sample.csv"
    target_file = PROJECT_ROOT / file_path_str
    
    if not target_file.exists():
        # Fallback to absolute or check standard data folder
        target_file = Path(file_path_str)

    if not target_file.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {file_path_str}")

    try:
        engine = choose_engine(str(target_file))
        if engine == "python_batch":
            res = load_csv_to_raw(file_path=str(target_file))
        else:
            from src.elt_million_pipeline import run_million_elt_pipeline
            res = run_million_elt_pipeline()
            
        return {
            "status": "SUCCESS",
            "file": str(target_file.name),
            "engine": engine,
            "result": res
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")


# ============================================================
# 3. INDEXES MANAGEMENT
# ============================================================

@app.post("/indexes", tags=["Indexes"])
def create_project_indexes():
    """
    Creates required indexes (Compound + Single Indexes) on validated collection.
    """
    try:
        db = get_db()
        created = create_indexes(db)
        return {
            "status": "SUCCESS",
            "indexes_created_count": len(created),
            "created_indexes": created
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Index creation failed: {str(e)}")


# ============================================================
# 4. QUERIES & EXPLAIN
# ============================================================

@app.get("/queries", tags=["Queries"])
def list_queries():
    """
    Lists all available queries and explain analysis option.
    """
    return {
        "available_queries": [
            "customer_orders",
            "city_status",
            "date_range",
            "email_or_phone",
            "high_value_orders",
            "explain_analysis"
        ],
        "description": "Pass query name to GET /queries/{name} to execute."
    }


@app.get("/queries/{name}", tags=["Queries"])
def execute_query(name: str, limit: int = 10, value: Optional[str] = None):
    """
    Executes a specific query by name or runs explain performance analysis.
    """
    db = get_db()
    
    if name == "explain_analysis":
        try:
            comparison = compare_query_explain_stats(db)
            return {"status": "SUCCESS", "explain_comparison": comparison}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Explain analysis failed: {str(e)}")
    
    elif name == "customer_orders":
        cust_id = value or "CUST-1001"
        res = query_orders_by_customer(db, customer_id=cust_id, limit=limit)
        return {"query": name, "count": len(res), "results": res}
        
    elif name == "city_status":
        city = value or "Sana'a"
        res = query_orders_by_city_and_status(db, city=city, limit=limit)
        return {"query": name, "count": len(res), "results": res}
        
    elif name == "date_range":
        res = query_orders_by_date_range(db, limit=limit)
        return {"query": name, "count": len(res), "results": res}

    elif name == "email_or_phone":
        contact = value or "user@example.com"
        res = query_orders_by_email_or_phone(db, contact_value=contact, limit=limit)
        return {"query": name, "count": len(res), "results": res}

    elif name == "high_value_orders":
        min_amt = float(value) if value else 500.0
        res = query_high_value_orders(db, min_amount=min_amt, limit=limit)
        return {"query": name, "count": len(res), "results": res}

    else:
        raise HTTPException(status_code=404, detail=f"Query '{name}' not found. Available: customer_orders, city_status, date_range, email_or_phone, high_value_orders, explain_analysis")


# ============================================================
# 5. AGGREGATION REPORTS
# ============================================================

@app.get("/aggregations", tags=["Aggregations"])
def list_aggregations():
    """
    Lists all 5 available aggregation reports.
    """
    return {
        "reports": list(REPORTS_REGISTRY.keys()),
        "description": "Pass report name to GET /aggregations/{name} to execute."
    }


@app.get("/aggregations/{name}", tags=["Aggregations"])
def execute_aggregation(name: str):
    """
    Executes a specific aggregation report.
    """
    try:
        db = get_db()
        results = run_report(name, db)
        return {
            "report_name": name,
            "row_count": len(results),
            "data": results
        }
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Report execution failed: {str(e)}")


# ============================================================
# 6. MATERIALIZED VIEWS
# ============================================================

@app.post("/refresh-mv", tags=["Materialized Views"])
def refresh_materialized_views(incremental: bool = True):
    """
    Triggers incremental refresh of all Materialized Views.
    """
    try:
        db = get_db()
        res = refresh_all_materialized_views(db, incremental=incremental)
        return {"status": "SUCCESS", "refresh_results": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Materialized view refresh failed: {str(e)}")


@app.get("/materialized-views/{name}", tags=["Materialized Views"])
def fetch_materialized_view(name: str, limit: int = 20):
    """
    Retrieves data stored inside a Materialized View collection.
    """
    try:
        db = get_db()
        data = get_materialized_view_data(name, db, limit=limit)
        return {"view_name": name, "count": len(data), "data": data}
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))


# ============================================================
# 7. SCHEDULED JOBS
# ============================================================

@app.get("/jobs", tags=["Scheduled Jobs"])
def list_jobs_and_logs(limit: int = 10):
    """
    Lists registered jobs and recent execution history logs.
    """
    db = get_db()
    logs = get_job_logs(db, limit=limit)
    return {
        "registered_jobs": list(JOBS_REGISTRY.keys()),
        "logs_count": len(logs),
        "recent_execution_logs": logs
    }


@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"])
def trigger_job_manually(name: str):
    """
    Manually triggers execution of a scheduled job.
    """
    try:
        db = get_db()
        res = run_job_by_name(name, db)
        return {"status": "COMPLETED", "job_result": res}
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Job execution failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
