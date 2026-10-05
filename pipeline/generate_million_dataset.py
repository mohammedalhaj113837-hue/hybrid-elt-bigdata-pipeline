import csv
from pathlib import Path


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE_FILE = (
    PROJECT_ROOT
    / "data"
    / "orders_huge_mixed_quality.csv"
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "million_sample.csv"
)


# ============================================================
# CONFIGURATION
# ============================================================

TARGET_ROWS = 1_000_000


# ============================================================
# CREATE MILLION-ROW SAMPLE
# ============================================================

def create_million_sample():

    print("=" * 70)
    print("CREATING MILLION-ROW SAMPLE")
    print("=" * 70)

    print(f"Source : {SOURCE_FILE}")
    print(f"Output : {OUTPUT_FILE}")
    print(f"Target : {TARGET_ROWS:,} records")
    print("=" * 70)

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Source file not found: {SOURCE_FILE}"
        )

    rows_written = 0

    with SOURCE_FILE.open(
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as source:

        reader = csv.reader(source)

        # ----------------------------------------------------
        # Read header
        # ----------------------------------------------------

        header = next(reader, None)

        if header is None:
            raise ValueError(
                "Source CSV is empty."
            )

        # ----------------------------------------------------
        # Write output
        # ----------------------------------------------------

        with OUTPUT_FILE.open(
            mode="w",
            encoding="utf-8",
            newline=""
        ) as output:

            writer = csv.writer(
                output,
                lineterminator="\n"
            )

            writer.writerow(header)

            # ------------------------------------------------
            # Copy exactly TARGET_ROWS records
            # ------------------------------------------------

            for row in reader:

                writer.writerow(row)

                rows_written += 1

                if rows_written % 100_000 == 0:

                    print(
                        f"Progress: "
                        f"{rows_written:,} / "
                        f"{TARGET_ROWS:,}"
                    )

                if rows_written >= TARGET_ROWS:
                    break

    # ========================================================
    # VALIDATION
    # ========================================================

    if rows_written != TARGET_ROWS:

        raise RuntimeError(
            f"Expected {TARGET_ROWS:,} records, "
            f"but only found {rows_written:,}."
        )

    print("=" * 70)
    print("MILLION-ROW SAMPLE CREATED SUCCESSFULLY")
    print("=" * 70)

    print(
        f"Records : {rows_written:,}"
    )

    print(
        f"Output  : {OUTPUT_FILE}"
    )

    print("=" * 70)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    create_million_sample()