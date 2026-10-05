from pathlib import Path
import sys
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastapi.testclient import TestClient
from src.api import app

client = TestClient(app)


def test_api_health():
    """Test GET /health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "UP"


def test_api_indexes():
    """Test POST /indexes endpoint."""
    response = client.post("/indexes")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"


def test_api_queries():
    """Test GET /queries and GET /queries/{name} endpoints."""
    res_list = client.get("/queries")
    assert res_list.status_code == 200

    res_q1 = client.get("/queries/customer_orders")
    assert res_q1.status_code == 200

    res_invalid = client.get("/queries/invalid_query_name")
    assert res_invalid.status_code == 404


def test_api_aggregations():
    """Test GET /aggregations and GET /aggregations/{name} endpoints."""
    res_list = client.get("/aggregations")
    assert res_list.status_code == 200
    
    res_report = client.get("/aggregations/sales_by_city")
    assert res_report.status_code == 200

    res_invalid = client.get("/aggregations/invalid_report")
    assert res_invalid.status_code == 404


def test_api_refresh_mv():
    """Test POST /refresh-mv endpoint."""
    response = client.post("/refresh-mv")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"


def test_api_jobs():
    """Test GET /jobs and POST /jobs/{name}/run endpoints."""
    res_list = client.get("/jobs")
    assert res_list.status_code == 200

    res_run = client.post("/jobs/periodic_system_audit_job/run")
    assert res_run.status_code == 200

    res_invalid = client.post("/jobs/invalid_job/run")
    assert res_invalid.status_code == 404
