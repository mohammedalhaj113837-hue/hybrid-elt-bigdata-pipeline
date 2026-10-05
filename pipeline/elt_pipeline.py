from pathlib import Path
import sys


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
# PYTHON / MONGODB
# ============================================================

from pymongo import MongoClient, UpdateOne


# ============================================================
# PROJECT SETTINGS
# ============================================================

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


# ============================================================
# QUALITY RULES
# ============================================================

from quality_rules import apply_all_quality_rules


# ============================================================
# CREATE SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("MidtermELTUpsertPipeline")
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

    return MongoClient(
        MONGO_URI
    )


# ============================================================
# READ RAW
# ============================================================

def read_orders_raw(spark):

    mongo_uri = (
        f"{MONGO_URI}/"
        f"{MONGO_DATABASE}."
        f"{RAW_COLLECTION}"
    )

    return (
        spark.read
        .format("mongodb")
        .option(
            "connection.uri",
            mongo_uri
        )
        .load()
    )


# ============================================================
# UPSERT ONE PARTITION
# ============================================================

def upsert_partition(
    rows,
    collection_name
):

    client = get_mongo_client()

    try:

        db = client[
            MONGO_DATABASE
        ]

        collection = db[
            collection_name
        ]

        operations = []

        for row in rows:

            document = row.asDict(
                recursive=True
            )

            # ------------------------------------------------
            # Use original MongoDB _id as stable key.
            # This preserves idempotency.
            # ------------------------------------------------

            record_id = document.get(
                "_id"
            )

            if record_id is None:
                continue

            operations.append(
                UpdateOne(
                    {
                        "_id": record_id
                    },
                    {
                        "$set": document
                    },
                    upsert=True
                )
            )

            # ------------------------------------------------
            # Execute in batches.
            # ------------------------------------------------

            if len(operations) >= 1000:

                collection.bulk_write(
                    operations,
                    ordered=False
                )

                operations = []

        # ----------------------------------------------------
        # Remaining operations.
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
                "quarantine_reasons"
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


# ============================================================
# QUARANTINE EXAMPLES
# ============================================================

def show_quarantine_examples(df):

    print("=" * 70)
    print("QUARANTINE EXAMPLES")
    print("=" * 70)

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


# ============================================================
# VERIFY MONGODB COUNTS
# ============================================================

def verify_mongodb_counts(
    expected_validated,
    expected_quarantine
):

    client = get_mongo_client()

    try:

        db = client[
            MONGO_DATABASE
        ]

        validated_count = (
            db[
                VALIDATED_COLLECTION
            ].count_documents({})
        )

        quarantine_count = (
            db[
                QUARANTINE_COLLECTION
            ].count_documents({})
        )

        print()
        print("=" * 70)
        print("MONGODB VERIFICATION")
        print("=" * 70)

        print(
            f"{VALIDATED_COLLECTION}: "
            f"{validated_count}"
        )

        print(
            f"{QUARANTINE_COLLECTION}: "
            f"{quarantine_count}"
        )

        # ----------------------------------------------------
        # Count validation.
        # ----------------------------------------------------

        if validated_count != expected_validated:

            raise RuntimeError(
                "orders_validated count does "
                "not match expected count!"
            )

        if quarantine_count != expected_quarantine:

            raise RuntimeError(
                "orders_quarantine count does "
                "not match expected count!"
            )

        print(
            "MONGODB COUNT CHECK: PASSED"
        )

        print("=" * 70)

    finally:

        client.close()


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_pipeline():

    spark = create_spark_session()

    try:

        print("=" * 70)
        print(
            "MIDTERM ELT "
            "IDEMPOTENT UPSERT PIPELINE"
        )
        print("=" * 70)

        # ----------------------------------------------------
        # SHOW ACTIVE SPARK MASTER
        # ----------------------------------------------------

        print()
        print(
            f"Spark Master: "
            f"{spark.sparkContext.master}"
        )

        # ----------------------------------------------------
        # 1. READ RAW
        # ----------------------------------------------------

        print()
        print(
            "Reading orders_raw..."
        )

        raw_df = read_orders_raw(
            spark
        )

        raw_count = raw_df.count()

        print(
            f"RAW RECORDS: {raw_count}"
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
            f"{processed_count}"
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
            f"{validated_count}"
        )

        print(
            f"Quarantine: "
            f"{quarantine_count}"
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
            f"Records: {validated_count}"
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
            f"Records: {quarantine_count}"
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
            validated_count,
            quarantine_count
        )

        # ----------------------------------------------------
        # 11. FINAL RESULT
        # ----------------------------------------------------

        print()
        print("=" * 70)
        print(
            "FINAL ELT RESULTS"
        )
        print("=" * 70)

        print(
            f"Raw        : {raw_count}"
        )

        print(
            f"Processed  : {processed_count}"
        )

        print(
            f"Validated  : {validated_count}"
        )

        print(
            f"Quarantine : {quarantine_count}"
        )

        print("=" * 70)

        print(
            "IDEMPOTENT UPSERT PIPELINE "
            "COMPLETED SUCCESSFULLY"
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