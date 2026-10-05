import sys
import time
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
)


DEFAULT_BATCH_SIZE = 1000


def load_csv_to_raw(
    file_path: str,
    id_run: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict:
    """
    Load CSV data into orders_raw using streaming batches.

    No cleaning or filtering is performed here.
    """

    input_path = Path(file_path)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    if batch_size <= 0:
        raise ValueError(
            "batch_size must be greater than zero"
        )

    client = MongoClient(MONGO_URI)

    total_loaded = 0
    batch_number = 0
    start_time = time.perf_counter()

    try:
        database = client[MONGO_DATABASE]
        collection = database[RAW_COLLECTION]

        import csv

        with input_path.open(
            mode="r",
            encoding="utf-8-sig",
            newline=""
        ) as file:

            reader = csv.DictReader(file)

            if reader.fieldnames is None:
                raise ValueError(
                    "CSV file does not contain a header."
                )

            batch = []

            for row_number, row in enumerate(reader, start=1):

                record = {
                    "id_run": id_run,
                    "file_source": str(input_path),
                    "number_row_source": row_number,
                    "engine_used": "python_batch",
                    "record_raw": dict(row),
                    "at_ingested": time.time(),
                }

                batch.append(record)

                if len(batch) >= batch_size:

                    batch_number += 1
                    batch_start = time.perf_counter()

                    collection.insert_many(
                        batch,
                        ordered=False
                    )

                    batch_elapsed = (
                        time.perf_counter() - batch_start
                    )

                    batch_count = len(batch)
                    total_loaded += batch_count

                    rate = (
                        batch_count / batch_elapsed
                        if batch_elapsed > 0
                        else 0
                    )

                    print(
                        f"Batch {batch_number} | "
                        f"Records: {batch_count} | "
                        f"Time: {batch_elapsed:.3f}s | "
                        f"Rate: {rate:.2f} records/s"
                    )

                    batch.clear()

            # Remaining records
            if batch:
                batch_number += 1
                batch_start = time.perf_counter()

                collection.insert_many(
                    batch,
                    ordered=False
                )

                batch_elapsed = (
                    time.perf_counter() - batch_start
                )

                batch_count = len(batch)
                total_loaded += batch_count

                rate = (
                    batch_count / batch_elapsed
                    if batch_elapsed > 0
                    else 0
                )

                print(
                    f"Batch {batch_number} | "
                    f"Records: {batch_count} | "
                    f"Time: {batch_elapsed:.3f}s | "
                    f"Rate: {rate:.2f} records/s"
                )

        total_elapsed = time.perf_counter() - start_time

        throughput = (
            total_loaded / total_elapsed
            if total_elapsed > 0
            else 0
        )

        result = {
            "id_run": id_run,
            "loaded_raw": total_loaded,
            "batches": batch_number,
            "seconds_elapsed": total_elapsed,
            "throughput": throughput,
            "batch_size": batch_size,
        }

        print("=" * 60)
        print("RAW LOAD COMPLETED")
        print("=" * 60)
        print(f"Run ID       : {id_run}")
        print(f"Loaded       : {total_loaded}")
        print(f"Batches      : {batch_number}")
        print(f"Elapsed      : {total_elapsed:.3f}s")
        return result

    finally:
        client.close()


if __name__ == "__main__":
    from uuid import uuid4
    sample_file = PROJECT_ROOT / "data" / "small_sample.csv"
    if sample_file.exists():
        load_csv_to_raw(str(sample_file), id_run=str(uuid4()))