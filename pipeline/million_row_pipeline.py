from pathlib import Path
import sys
import time

# Reconfigure stdout for Windows console UTF-8 support
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ============================================================
# PROJECT PATH
# ============================================================


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ============================================================
# SPARK
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# MONGODB
# ============================================================

from pymongo import MongoClient, UpdateOne

# ============================================================
# PROJECT SETTINGS
# ============================================================

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)

# ============================================================
# QUALITY RULES
# ============================================================

from quality_rules import apply_all_quality_rules


# ============================================================
# SETTINGS
# ============================================================

SPARK_MASTER = "spark://172.22.144.1:7077"

MILLION_FILE = (
    PROJECT_ROOT
    / "data"
    / "million_sample.csv"
)


# ============================================================
# CREATE SPARK SESSION
# ============================================================

def is_master_reachable(master_url):
    try:
        if master_url.startswith("spark://"):
            parts = master_url.replace("spark://", "").split(":")
            host = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 7077
            import socket
            with socket.create_connection((host, port), timeout=1.5):
                return True
    except Exception:
        pass
    return False


def create_spark_session():
    if is_master_reachable(SPARK_MASTER):
        master = SPARK_MASTER
        print(f"Connecting to Spark Standalone Master at {SPARK_MASTER}...")
    else:
        master = "local[*]"
        print(f"Spark Master at {SPARK_MASTER} not reachable. Running on local mode: master({master})")

    spark = (
        SparkSession.builder
        .appName("MidtermELTMillionPipeline")
        .master(master)
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0",
                "org.mongodb:mongodb-driver-sync:5.1.1",
            ])
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark




# ============================================================
# MONGODB CONNECTION
# ============================================================

def get_mongo_client():

    return MongoClient(MONGO_URI)


# ============================================================
# READ MILLION CSV
# ============================================================

def read_million_csv(spark):

    if not MILLION_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {MILLION_FILE}"
        )

    df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(str(MILLION_FILE))
)

    # --------------------------------------------------------
    # Prepare the CSV so it matches the structure expected
    # by quality_rules.py
    # --------------------------------------------------------

    df = (
        df
        .withColumn("_id", F.col("order_id"))
        .withColumn("at_ingested", F.lit(time.time()))
        .withColumn("engine_used", F.lit("pyspark"))
        .withColumn("file_source", F.lit(str(MILLION_FILE)))
        .withColumn("id_run", F.lit("million-sample"))
        .withColumn(
            "number_row_source",
            F.monotonically_increasing_id()
        )
    )

    # Put the original CSV columns inside record_raw
    csv_columns = df.columns

    csv_columns = [
        c for c in csv_columns
        if c not in [
            "_id",
            "at_ingested",
            "engine_used",
            "file_source",
            "id_run",
            "number_row_source",
        ]
    ]

    df = df.select(
        "_id",
        "at_ingested",
        "engine_used",
        "file_source",
        "id_run",
        "number_row_source",
        F.struct(
            *[
                F.col(c).alias(c)
                for c in csv_columns
            ]
        ).alias("record_raw")
    )

    return df


# ============================================================
# UPSERT ONE PARTITION
# ============================================================

def upsert_partition(rows, collection_name):

    client = get_mongo_client()

    try:

        db = client[MONGO_DATABASE]

        collection = db[collection_name]

        operations = []

        for row in rows:

            document = row.asDict(
                recursive=True
            )

            # ------------------------------------------------
            # Stable key
            # ------------------------------------------------

            # order_id is normally produced by the quality rules.
            # Keep a fallback for the original CSV structure.
            order_id = document.get("order_id")

            if order_id is None:
                record_raw = document.get("record_raw")
                if isinstance(record_raw, dict):
                    order_id = record_raw.get("order_id")

            if order_id is None:
                continue

            # MongoDB _id is immutable.
            # It must NOT be included inside $set when doing an upsert.
            document.pop("_id", None)

            operations.append(
                UpdateOne(
                    {
                        "order_id": order_id
                    },
                    {
                        "$set": document
                    },
                    upsert=True
                )
            )

            # ------------------------------------------------
            # Bulk write
            # ------------------------------------------------

            if len(operations) >= 1000:

                collection.bulk_write(
                    operations,
                    ordered=False
                )

                operations = []

        # ----------------------------------------------------
        # Remaining operations
        # ----------------------------------------------------

        if operations:

            collection.bulk_write(
                operations,
                ordered=False
            )

    finally:

        client.close()


# ============================================================
# UPSERT DATAFRAME
# ============================================================

def upsert_to_mongodb(
    df,
    collection_name
):

    (
        df
        .rdd
        .foreachPartition(
            lambda rows:
            upsert_partition(
                rows,
                collection_name
            )
        )
    )


# ============================================================
# QUALITY SUMMARY
# ============================================================

def show_quality_summary(df):

    print("=" * 70)
    print("QUALITY SUMMARY")
    print("=" * 70)

    (
        df
        .groupBy(
            "quality_status"
        )
        .count()
        .orderBy(
            "quality_status"
        )
        .show(
            truncate=False
        )
    )

    print("=" * 70)
    print("QUARANTINE REASONS")
    print("=" * 70)

    (
        df
        .select(
            F.explode_outer(
                F.col("quarantine_reasons")
            ).alias("reason")
        )
        .filter(
            F.col("reason").isNotNull()
        )
        .groupBy(
            "reason"
        )
        .count()
        .orderBy(
            F.desc("count")
        )
        .show(
            truncate=False
        )
    )


# ============================================================
# CORRECTION EXAMPLES
# ============================================================

def show_correction_examples(df):

    print("=" * 70)
    print("CORRECTION EXAMPLES")
    print("=" * 70)

    try:
        (
            df
            .filter(
                F.size(
                    F.col("corrections")
                ) > 0
            )
            .select(
                "order_id",
                "quality_status",
                "corrections"
            )
            .show(
                10,
                truncate=False
            )
        )
    except Exception as e:
        print(f"(Correction samples displayed in Mongo / JSON due to console encoding: {e})")


# ============================================================
# QUARANTINE EXAMPLES
# ============================================================

def show_quarantine_examples(df):

    print("=" * 70)
    print("QUARANTINE EXAMPLES")
    print("=" * 70)

    try:
        (
            df
            .filter(
                F.col(
                    "quality_status"
                ) == "quarantine"
            )
            .select(
                "order_id",
                "customer_id",
                "quality_status",
                "quarantine_reasons"
            )
            .show(
                10,
                truncate=False
            )
        )
    except Exception as e:
        print(f"(Quarantine samples displayed in Mongo / JSON due to console encoding: {e})")



# ============================================================
# VERIFY MONGODB COUNTS
# ============================================================

def verify_mongodb_counts(
    validated_df,
    quarantine_df
):

    client = get_mongo_client()

    try:
        db = client[MONGO_DATABASE]

        validated_collection = db[VALIDATED_COLLECTION]
        quarantine_collection = db[QUARANTINE_COLLECTION]

        # Check IDs in small batches to avoid MongoDB command-size limits
        batch_size = 1000

        def verify_collection(df, collection, name):
            ids = [
                row["order_id"]
                for row in df.select("order_id").distinct().collect()
                if row["order_id"] is not None
            ]

            found = 0

            for i in range(0, len(ids), batch_size):
                batch = ids[i:i + batch_size]

                found += collection.count_documents(
                    {"order_id": {"$in": batch}}
                )

            print(
                f"{name}: {found:,} / {len(ids):,} IDs found"
            )

            if found != len(ids):
                raise RuntimeError(
                    f"{name} verification failed!"
                )

        print()
        print("=" * 70)
        print("MONGODB VERIFICATION")
        print("=" * 70)

        verify_collection(
            validated_df,
            validated_collection,
            "orders_validated"
        )

        verify_collection(
            quarantine_df,
            quarantine_collection,
            "orders_quarantine"
        )

        print("MONGODB UPSERT CHECK: PASSED")
        print("=" * 70)

    finally:
        client.close()

# ============================================================
# MAIN MILLION ELT PIPELINE
# ============================================================

def run_pipeline():

    spark = create_spark_session()

    start_time = time.perf_counter()

    try:

        print("=" * 70)
        print(
            "MILLION-ROW ELT "
            "IDEMPOTENT UPSERT PIPELINE"
        )
        print("=" * 70)

        print(
            f"Spark Master : "
            f"{spark.sparkContext.master}"
        )

        print(
            f"Input File   : "
            f"{MILLION_FILE}"
        )

        # ----------------------------------------------------
        # 1. READ MILLION CSV
        # ----------------------------------------------------

        print()
        print(
            "Reading million_sample.csv..."
        )

        raw_df = read_million_csv(
            spark
        )

        raw_count = raw_df.count()

        print(
            f"RAW RECORDS: {raw_count:,}"
        )

        if raw_count != 1_000_000:

            raise RuntimeError(
                f"Expected 1,000,000 records, "
                f"but found {raw_count:,}"
            )

        # ----------------------------------------------------
        # 2. APPLY 9 QUALITY RULES
        # ----------------------------------------------------

        print()
        print("=" * 70)
        print(
            "APPLYING 9 QUALITY RULES"
        )
        print("=" * 70)

        processed_df = (
            apply_all_quality_rules(
                raw_df
            )
            .cache()
        )

        processed_count = (
            processed_df.count()
        )

        print()
        print(
            f"PROCESSED RECORDS: "
            f"{processed_count:,}"
        )

        # ----------------------------------------------------
        # 3. COUNT CHECK
        # ----------------------------------------------------

        if raw_count != processed_count:

            raise RuntimeError(
                "Record count changed during "
                "quality processing!"
            )

        print(
            "COUNT CHECK: PASSED"
        )

        # ----------------------------------------------------
        # 4. QUALITY INFORMATION
        # ----------------------------------------------------

        show_quality_summary(
            processed_df
        )

        show_correction_examples(
            processed_df
        )

        show_quarantine_examples(
            processed_df
        )

        # ----------------------------------------------------
        # 5. SPLIT VALIDATED
        # ----------------------------------------------------

        validated_df = (
            processed_df
            .filter(
                F.col(
                    "quality_status"
                ).isin(
                    "valid",
                    "corrected"
                )
            )
        )

        # ----------------------------------------------------
        # 6. SPLIT QUARANTINE
        # ----------------------------------------------------

        quarantine_df = (
            processed_df
            .filter(
                F.col(
                    "quality_status"
                ) == "quarantine"
            )
        )

        validated_count = (
            validated_df.count()
        )

        quarantine_count = (
            quarantine_df.count()
        )

        print()
        print("=" * 70)
        print("OUTPUT COUNTS")
        print("=" * 70)

        print(
            f"Validated : "
            f"{validated_count:,}"
        )

        print(
            f"Quarantine: "
            f"{quarantine_count:,}"
        )

        # ----------------------------------------------------
        # 7. SPLIT CHECK
        # ----------------------------------------------------

        if (
            validated_count
            + quarantine_count
            != processed_count
        ):

            raise RuntimeError(
                "Validated + quarantine does "
                "not equal processed!"
            )

        print(
            "SPLIT COUNT CHECK: PASSED"
        )

        # ----------------------------------------------------
        # 8. UPSERT VALIDATED
        # ----------------------------------------------------

        print()
        print("=" * 70)
        print(
            "UPSERTING orders_validated"
        )
        print("=" * 70)

        print(
            f"Records: "
            f"{validated_count:,}"
        )

        if validated_count > 0:

            upsert_to_mongodb(
                validated_df,
                VALIDATED_COLLECTION
            )

        print(
            "VALIDATED UPSERT COMPLETED"
        )

        # ----------------------------------------------------
        # 9. UPSERT QUARANTINE
        # ----------------------------------------------------

        print()
        print("=" * 70)
        print(
            "UPSERTING orders_quarantine"
        )
        print("=" * 70)

        print(
            f"Records: "
            f"{quarantine_count:,}"
        )

        if quarantine_count > 0:

            upsert_to_mongodb(
                quarantine_df,
                QUARANTINE_COLLECTION
            )

        print(
            "QUARANTINE UPSERT COMPLETED"
        )

        # ----------------------------------------------------
        # 10. VERIFY MONGODB
        # ----------------------------------------------------

        verify_mongodb_counts(
        validated_df,
        quarantine_df
    )

        # ----------------------------------------------------
        # 11. FINAL RESULT
        # ----------------------------------------------------

        elapsed = (
            time.perf_counter()
            - start_time
        )

        throughput = (
            raw_count / elapsed
            if elapsed > 0
            else 0
        )

        print()
        print("=" * 70)
        print(
            "FINAL MILLION-ROW ELT RESULTS"
        )
        print("=" * 70)

        print(
            f"Raw        : "
            f"{raw_count:,}"
        )

        print(
            f"Processed  : "
            f"{processed_count:,}"
        )

        print(
            f"Validated  : "
            f"{validated_count:,}"
        )

        print(
            f"Quarantine : "
            f"{quarantine_count:,}"
        )

        print(
            f"Time       : "
            f"{elapsed:.3f} seconds"
        )

        print(
            f"Throughput : "
            f"{throughput:,.2f} records/s"
        )

        print("=" * 70)

        print(
            "MILLION-ROW IDEMPOTENT ELT "
            "PIPELINE COMPLETED SUCCESSFULLY"
        )

        print("=" * 70)

        processed_df.unpersist()

    finally:

        spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_pipeline()