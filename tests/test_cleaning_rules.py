"""
Unit Tests for Quality and Cleaning Rules (src/quality_rules.py).
Tests all 9 deterministic cleaning rules, audit trail tracking, and quarantine error codes.
"""
import pytest
from src.quality_rules import (
    normalize_arabic_digits,
    normalize_currency_and_text,
    normalize_phone,
    normalize_email,
    normalize_date,
    normalize_text_and_synonyms,
    validate_and_recalculate_order,
    process_and_classify_record,
    RULE_01, RULE_02, RULE_03, RULE_04, RULE_05, RULE_06, RULE_07, RULE_08, RULE_09,
    ERR_MISSING_ORDER_ID, ERR_MISSING_CUSTOMER_ID, ERR_INVALID_IMPOSSIBLE_DATE,
    ERR_CORRUPTED_ITEMS_JSON, ERR_UNKNOWN_PRICE, ERR_UNSAFE_EMAIL
)

def test_rule_01_arabic_digits():
    """RULE_01: Arabic/Eastern digits converted to Latin digits."""
    val, changed = normalize_arabic_digits("٥٠٠٠٫٥")
    assert changed is True
    assert val == "5000.5"

    val_pure, changed_pure = normalize_arabic_digits("12345")
    assert changed_pure is False
    assert val_pure == "12345"

def test_rule_02_currency_text():
    """RULE_02: Currency words stripped and standard currency extracted."""
    amt, curr, corr, err = normalize_currency_and_text("5000 ريال يمني")
    assert err is None
    assert amt == 5000.0
    assert curr == "YER"
    assert any(c["rule_code"] == RULE_02 for c in corr)

def test_rule_03_thousands_separators():
    """RULE_03: Thousands comma separators removed."""
    amt, curr, corr, err = normalize_currency_and_text("125,000.50")
    assert err is None
    assert amt == 125000.50
    assert any(c["rule_code"] == RULE_03 for c in corr)

def test_rule_04_known_number_words():
    """RULE_04: Known Arabic number words converted to exact numeric values."""
    amt, curr, corr, err = normalize_currency_and_text("ألفان")
    assert err is None
    assert amt == 2000.0
    assert any(c["rule_code"] == RULE_04 for c in corr)

def test_rule_05_phone_normalization():
    """RULE_05: Yemeni phone formats cleaned to standard 9-digit format."""
    phone, corr, err = normalize_phone("+967 77 123 4567")
    assert err is None
    assert phone == "771234567"
    assert len(corr) > 0
    assert corr[0]["rule_code"] == RULE_05

def test_rule_06_email_correction_and_quarantine():
    """RULE_06: Safe email fix (double-@) vs unsafe email quarantine."""
    # Safe fix
    email, corr, err = normalize_email("user@@example..com")
    assert err is None
    assert email == "user@example.com"
    assert any(c["rule_code"] == RULE_06 for c in corr)

    # Unsafe email -> Quarantine
    bad_email, _, bad_err = normalize_email("@@")
    assert bad_err == ERR_UNSAFE_EMAIL

def test_rule_07_date_normalization_and_quarantine():
    """RULE_07: Date formats standardized to ISO or quarantined if impossible."""
    # Format conversion
    d_clean, corr, err = normalize_date("17-01-2025 04:50:00")
    assert err is None
    assert d_clean == "2025-01-17T04:50:00"
    assert any(c["rule_code"] == RULE_07 for c in corr)

    # Impossible date -> Quarantine
    _, _, err_bad = normalize_date("2025-02-31")
    assert err_bad == ERR_INVALID_IMPOSSIBLE_DATE

def test_rule_08_trim_and_synonyms():
    """RULE_08: Trim whitespace and map known synonyms."""
    city, corr = normalize_text_and_synonyms("city", " صنعا ")
    assert city == "صنعاء"
    assert any(c["rule_code"] == RULE_08 for c in corr)

def test_rule_09_order_total_recalculation():
    """RULE_09: Recalculate order total from valid items when mismatch occurs."""
    items_json = '[{"sku":"SKU-1","qty":2,"unit_price":10000.0,"total":20000.0}]'
    # Delivery cost = 2000.0, Items total = 20000.0 -> Expected total = 22000.0
    tot, items, corr, err = validate_and_recalculate_order("15000.0", 2000.0, items_json)
    assert err is None
    assert tot == 22000.0
    assert any(c["rule_code"] == RULE_09 for c in corr)

def test_corrupted_items_quarantine():
    """Quarantine when items_json is corrupt."""
    tot, items, corr, err = validate_and_recalculate_order("22000.0", 2000.0, "not-json")
    assert err == ERR_CORRUPTED_ITEMS_JSON

def test_classification_end_to_end():
    """Test full document classification into Valid, Corrected, and Quarantined."""
    # 1. Valid record (clean)
    valid_doc = {
        "id_run": "run_test",
        "source_file": "test.csv",
        "source_row_number": 1,
        "raw_record": {
            "order_id": "ORD-100",
            "order_date": "2025-01-15T10:00:00",
            "status": "مؤكد",
            "customer_id": "CUST-1",
            "customer_name": "أحمد علي",
            "customer_phone": "771234567",
            "customer_email": "ahmed@example.com",
            "city": "صنعاء",
            "district": "السبعين",
            "delivery_type": "عادي",
            "delivery_cost": "2000.0",
            "payment_method": "بطاقة",
            "payment_status": "تم الدفع",
            "payment_amount": "22000.0",
            "currency": "YER",
            "total_amount": "22000.0",
            "items_json": '[{"sku":"SKU-1","qty":2,"unit_price":10000.0,"total":20000.0}]'
        }
    }
    res_valid = process_and_classify_record(valid_doc)
    assert res_valid["classification"] == "valid"
    assert len(res_valid["corrections"]) == 0

    # 2. Corrected record (has Arabic digits and double-@)
    corrected_doc = dict(valid_doc)
    corrected_doc["raw_record"] = dict(valid_doc["raw_record"])
    corrected_doc["raw_record"]["total_amount"] = "٢٢٠٠٠٫٠"
    corrected_doc["raw_record"]["customer_email"] = "ahmed@@example.com"
    res_corr = process_and_classify_record(corrected_doc)
    assert res_corr["classification"] == "corrected"
    assert len(res_corr["corrections"]) >= 2

    # 3. Quarantined record (missing customer_id and corrupted items_json)
    quar_doc = dict(valid_doc)
    quar_doc["raw_record"] = dict(valid_doc["raw_record"])
    quar_doc["raw_record"]["customer_id"] = ""
    quar_doc["raw_record"]["items_json"] = "corrupted-json"
    res_quar = process_and_classify_record(quar_doc)
    assert res_quar["classification"] == "quarantined"
    assert ERR_MISSING_CUSTOMER_ID in res_quar["error_codes"]
    assert ERR_CORRUPTED_ITEMS_JSON in res_quar["error_codes"]
