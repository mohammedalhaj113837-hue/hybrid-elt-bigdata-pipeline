import csv
import json
import random
import sys
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Allow imports from project root
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import SAMPLE_ROWS

CSV_HEADERS = [
    "order_id", "customer_id", "customer_name", "customer_phone",
    "customer_email", "city", "district", "delivery_type",
    "payment_method", "payment_status", "currency", "status",
    "order_date", "delivery_cost", "payment_amount", "total_amount",
    "items_json"
]


def generate_synthetic_sample(output_file: str, rows: int = 100) -> None:
    """
    Generates a realistic synthetic sample CSV with mixed quality data,
    covering all 9 quality rules and quarantine reasons.
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    products = [
        ("SKU-01", "عسل سدر دوعني", 12500),
        ("SKU-02", "بن يمني مطحون", 20000),
        ("SKU-03", "تمر خلاص فاخر", 5000),
        ("SKU-04", "لوز خولاني", 12000),
        ("SKU-05", "زيت سمسم بلدي", 5000),
        ("SKU-06", "زعفران أصلي", 30000),
    ]

    cities = ["Sana'a", "Aden", "Taiz", "Hodeidah", "Ibb", "Mukalla"]
    statuses = ["DELIVERED", "confirmed", "pending", "shipped", "مؤكد", "قيد الانتظار"]
    currencies = ["YER", "ريال يمني", "ر.ي", "YER "]
    delivery_costs = ["1500", "١٠٠٠ ريال", "خمسة", "2,000", "1000", "0", "صفر"]

    written = 0
    with output_path.open(mode="w", encoding="utf-8-sig", newline="") as destination:
        writer = csv.writer(destination)
        writer.writerow(CSV_HEADERS)

        for i in range(1, rows + 1):
            order_id = f"ORD-{1000 + i}"
            cust_id = f"CUST-{1000 + (i % 25) + 1}"
            prod = products[i % len(products)]
            qty = (i % 4) + 1
            unit_price = prod[2]
            items_total = qty * unit_price
            items = [{"sku": prod[0], "name": prod[1], "qty": str(qty), "unit_price": str(unit_price), "total": str(items_total)}]
            items_json = json.dumps(items, ensure_ascii=False)

            city = cities[i % len(cities)]
            status = statuses[i % len(statuses)]
            curr = currencies[i % len(currencies)]
            deliv = delivery_costs[i % len(delivery_costs)]
            phone = f"96777{100000 + i}" if i % 3 == 0 else f"+96777{100000 + i}" if i % 3 == 1 else f"٠٧٧{100000 + i}"
            email = f"user{i}..test@example.com" if i % 4 == 0 else f"user{i}@example.com"
            order_date = f"2024-0{((i % 8) + 1):02d}-15" if i % 2 == 0 else f"15/0{((i % 8) + 1):02d}/2024"

            # Inject a few quarantine rows intentionally
            if i % 15 == 0:
                cust_id = ""  # MISSING_CUSTOMER_ID
            elif i % 15 == 1:
                items_json = "{invalid_json_corrupted"  # CORRUPTED_ITEMS_JSON
            elif i % 15 == 2:
                deliv = "-500"  # AMBIGUOUS_NEGATIVE_VALUE

            writer.writerow([
                order_id, cust_id, f"عميل {i}", phone,
                email, city, "المركز", "Standard",
                "Cash", "PAID", curr, status,
                order_date, deliv, str(items_total), str(items_total + 1500),
                items_json
            ])
            written += 1

    print("=" * 60)
    print("SYNTHETIC SAMPLE CREATED")
    print("=" * 60)
    print(f"Output file : {output_path}")
    print(f"Rows        : {written}")


def create_small_sample(
    input_file: str,
    output_file: str,
    rows: int = SAMPLE_ROWS
) -> None:
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        print(f"Source file {input_path} not found. Generating synthetic sample dataset...")
        generate_synthetic_sample(output_file, rows=min(rows, 500))
        return

    if rows <= 0:
        raise ValueError("rows must be greater than zero")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with (
        input_path.open(mode="r", encoding="utf-8-sig", newline="") as source,
        output_path.open(mode="w", encoding="utf-8", newline="") as destination
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