# Project settings

# MongoDB
MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "midterm_data_pipeline2"

# MongoDB collections
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# File routing & Input Data Files
SMALL_FILE_THRESHOLD_MB = 200
SAMPLE_ROWS = 100_000

INPUT_CSV_FILE = "small_sample.csv"
MILLION_CSV_FILE = "million_sample.csv"

# Project
PROJECT_NAME = "midterm-data-pipeline2"
