from pathlib import Path
import sys
import time


# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# SPARK
# ============================================================

from pyspark.sql import SparkSession


# ============================================================
# SPARK CONFIGURATION
# ============================================================

SPARK_MASTER = "spark://172.20.240.1:7077"


# ============================================================
# CREATE SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("MidtermMillionSample")
        .master(SPARK_MASTER)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# READ MILLION SAMPLE
# ============================================================

def read_million_sample(spark):

    input_file = (
        PROJECT_ROOT
        / "data"
        / "million_sample.csv"
    )

    if not input_file.exists():
        raise FileNotFoundError(
            f"File not found: {input_file}"
        )

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(input_file))
    )


# ============================================================
# RUN MILLION-ROW TEST
# ============================================================

def run_million_test():

    spark = create_spark_session()

    try:

        print("=" * 70)
        print("PYSPARK MILLION-ROW CLUSTER TEST")
        print("=" * 70)

        print(
            f"Spark Master : {SPARK_MASTER}"
        )

        print(
            f"Input file   : "
            f"{PROJECT_ROOT / 'data' / 'million_sample.csv'}"
        )

        start_time = time.perf_counter()

        df = read_million_sample(spark)

        record_count = df.count()

        elapsed = time.perf_counter() - start_time

        throughput = (
            record_count / elapsed
            if elapsed > 0
            else 0
        )

        print("=" * 70)
        print("PYSPARK CLUSTER TEST RESULTS")
        print("=" * 70)

        print(
            f"Records    : {record_count:,}"
        )

        print(
            f"Time       : {elapsed:.3f} seconds"
        )

        print(
            f"Throughput : {throughput:,.2f} records/s"
        )

        print(
            f"Partitions : "
            f"{df.rdd.getNumPartitions()}"
        )

        print("=" * 70)

        if record_count != 1_000_000:

            raise RuntimeError(
                f"Expected 1,000,000 records, "
                f"but found {record_count:,}"
            )

        print(
            "MILLION-ROW PYSPARK CLUSTER TEST PASSED"
        )

        print("=" * 70)

    finally:

        spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_million_test()