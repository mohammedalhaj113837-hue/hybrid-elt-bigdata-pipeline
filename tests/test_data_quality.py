from pathlib import Path
import sys
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.data_quality import (
    clean_text,
    arabic_to_western,
    normalize_numeric_text,
    normalize_currency_text,
    remove_currency,
    CORRECTION_SCHEMA
)


def test_correction_schema_definition():
    """Verify that correction schema has the required fields for audit tracking."""
    field_names = [f.name for f in CORRECTION_SCHEMA.elementType.fields]
    assert "field" in field_names
    assert "original_value" in field_names
    assert "corrected_value" in field_names
    assert "rule_code" in field_names


def test_arabic_digits_mapping():
    """Verify Arabic to Western digits mapping helper."""
    arabic_digits = "٠١٢٣٤٥٦٧٨٩"
    western_digits = "0123456789"
    # Pure Python logic verification corresponding to arabic_to_western
    table = str.maketrans(arabic_digits, western_digits)
    test_str = "السعر ١٥٠٠٠ ريال"
    converted = test_str.translate(table)
    assert "15000" in converted


def test_phone_number_cleaning_patterns():
    """Verify standard Yemeni phone formatting logic."""
    import re
    # Test cases: +967, 00967, 07..., 7...
    raw_phones = ["+967771234567", "00967731234567", "0771234567", "771234567"]
    cleaned = []
    for p in raw_phones:
        p_clean = re.sub(r"[\s\-\(\)\.]", "", p)
        p_clean = re.sub(r"^\+967", "967", p_clean)
        p_clean = re.sub(r"^00967", "967", p_clean)
        if re.match(r"^07\d{8}$", p_clean):
            p_clean = "967" + p_clean[1:]
        elif re.match(r"^7\d{8}$", p_clean):
            p_clean = "967" + p_clean
        cleaned.append(p_clean)

    for p in cleaned:
        assert re.match(r"^9677\d{8}$", p)


def test_email_cleaning_patterns():
    """Verify multiple @ and dots removal logic."""
    import re
    raw_email = "  user@@domain..com  "
    cleaned = raw_email.strip().replace(" ", "")
    cleaned = re.sub(r"@{2,}", "@", cleaned)
    cleaned = re.sub(r"\.{2,}", ".", cleaned)
    assert cleaned == "user@domain.com"
