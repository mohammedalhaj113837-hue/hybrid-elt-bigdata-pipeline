# Project settings
#
# Every value can be overridden with an environment variable or with a
# `.env` file in the project root (copy `.env.example` to `.env`).
# No third-party package is required to read the `.env` file.

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv(path: Path) -> None:
    """Minimal .env loader (KEY=VALUE lines). Existing env vars win."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv(PROJECT_ROOT / ".env")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "midterm_data_pipeline2")

# MongoDB collections
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# File routing & Input Data Files
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200"))
SAMPLE_ROWS = int(os.getenv("SAMPLE_ROWS", "100000"))

DATA_DIR = PROJECT_ROOT / "data"
INPUT_CSV_FILE = os.getenv("INPUT_CSV_FILE", "small_sample.csv")
MILLION_CSV_FILE = "million_sample.csv"

# PySpark
# "local[*]" works everywhere. Set SPARK_MASTER=spark://<host>:7077 in .env
# to use a standalone cluster.
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# FastAPI
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Project
PROJECT_NAME = "hybrid-elt-bigdata-pipeline"
