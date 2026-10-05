import csv
import sys
from pathlib import Path


# Project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Allow imports from project root
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import SAMPLE_ROWS


def create_small_sample(
    input_file: str,
    output_file: str,
    rows: int = SAMPLE_ROWS
) -> None:

    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    if rows <= 0:
        raise ValueError("rows must be greater than zero")

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with (
        input_path.open(
            mode="r",
            encoding="utf-8-sig",
            newline=""
        ) as source,
        output_path.open(
            mode="w",
            encoding="utf-8",
            newline=""
        ) as destination
    ):
        reader = csv.reader(source)
        writer = csv.writer(destination)

        header = next(reader)
        writer.writerow(header)

        rows_written = 0

        for row in reader:
            writer.writerow(row)
            rows_written += 1

            if rows_written >= rows:
                break

    print("=" * 60)
    print("SMALL SAMPLE CREATED")
    print("=" * 60)
    print(f"Input file  : {input_path}")
    print(f"Output file : {output_path}")
    print(f"Rows        : {rows_written}")


def main():
    input_file = PROJECT_ROOT / "data" / "orders_huge_mixed_quality.csv"
    output_file = PROJECT_ROOT / "data" / "small_sample.csv"

    create_small_sample(
        input_file=str(input_file),
        output_file=str(output_file),
        rows=SAMPLE_ROWS
    )


if __name__ == "__main__":
    main()