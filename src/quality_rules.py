"""
Quality and Cleaning Rules Module for Midterm Data Pipeline.
Implements 9 deterministic, auditable, and testable cleaning rules.
Enforces strict ELT validation:
- Safe deterministic corrections produce an Audit Trail entry.
- Unsafe / ambiguous malformed values trigger specific Quarantine Error Codes.
"""
import re
import json
import logging
from typing import Dict, Any, Tuple, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Rule Codes
# --------------------------------------------------------------------------
RULE_01 = "RULE_01"  # Arabic/Eastern digits -> Latin
RULE_02 = "RULE_02"  # Currency text normalization
RULE_03 = "RULE_03"  # Thousands separators normalization
RULE_04 = "RULE_04"  # Known number words normalization
RULE_05 = "RULE_05"  # Phone number normalization
RULE_06 = "RULE_06"  # Email double-@ and repeated dot fix
RULE_07 = "RULE_07"  # Date normalization
RULE_08 = "RULE_08"  # Trim & synonym normalization
RULE_09 = "RULE_09"  # Order total recalculation

# --------------------------------------------------------------------------
# Quarantine Error Codes
# --------------------------------------------------------------------------
ERR_MISSING_ORDER_ID = "MISSING_ORDER_ID"
ERR_MISSING_CUSTOMER_ID = "MISSING_CUSTOMER_ID"
ERR_INVALID_IMPOSSIBLE_DATE = "INVALID_IMPOSSIBLE_DATE"
ERR_CORRUPTED_ITEMS_JSON = "CORRUPTED_ITEMS_JSON"
ERR_EMPTY_ITEMS = "EMPTY_ITEMS"
ERR_UNKNOWN_PRICE = "UNKNOWN_PRICE"
ERR_AMBIGUOUS_NEGATIVE_VALUE = "AMBIGUOUS_NEGATIVE_VALUE"
ERR_DUPLICATE_ORDER_ID = "DUPLICATE_ORDER_ID"
ERR_MULTIPLE_CONFLICTING_ERRORS = "MULTIPLE_CONFLICTING_ERRORS"
ERR_UNSAFE_EMAIL = "UNSAFE_EMAIL"

# --------------------------------------------------------------------------
# Lookup Tables & Dictionaries
# --------------------------------------------------------------------------
ARABIC_EASTERN_DIGITS_MAP = str.maketrans({
    "٠": "0", "١": "1", "٢": "2", "٣": "3", "٤": "4",
    "٥": "5", "٦": "6", "٧": "7", "٨": "8", "٩": "9",
    "٫": ".", "٬": ","
})

KNOWN_NUMBER_WORDS = {
    "ألف": 1000.0, "الف": 1000.0,
    "ألفان": 2000.0, "الفين": 2000.0, "ألفين": 2000.0,
    "ثلاثة آلاف": 3000.0, "ثلاثة الاف": 3000.0, "ثلاث الاف": 3000.0,
    "أربعة آلاف": 4000.0, "اربعة الاف": 4000.0, "اربع الاف": 4000.0,
    "خمسة آلاف": 5000.0, "خمسة الاف": 5000.0, "خمس الاف": 5000.0,
    "ستة آلاف": 6000.0, "ستة الاف": 6000.0,
    "سبعة آلاف": 7000.0, "سبعة الاف": 7000.0,
    "ثمانية آلاف": 8000.0, "ثمانية الاف": 8000.0,
    "تسعة آلاف": 9000.0, "تسعة الاف": 9000.0,
    "عشرة آلاف": 10000.0, "عشرة الاف": 10000.0, "عشر الاف": 10000.0,
    "عشرون ألف": 20000.0, "عشرين ألف": 20000.0, "عشرين الف": 20000.0,
    "خمسون ألف": 50000.0, "خمسين ألف": 50000.0, "خمسين الف": 50000.0,
    "مائة ألف": 100000.0, "مية ألف": 100000.0, "مئة الف": 100000.0
}

KNOWN_CITIES = {
    "صنعاء": "صنعاء", "صنعا": "صنعاء",
    "عدن": "عدن",
    "تعز": "تعز",
    "الحديدة": "الحديدة", "حديدة": "الحديدة",
    "إب": "إب", "اب": "إب",
    "المكلا": "المكلا", "مكلا": "المكلا",
    "ذمار": "ذمار",
    "حجة": "حجة",
    "عمران": "عمران",
    "لحج": "لحج"
}

KNOWN_STATUSES = {
    "مؤكد": "مؤكد", "موكد": "مؤكد",
    "قيد الانتظار": "قيد الانتظار", "انتظار": "قيد الانتظار",
    "قيد الشحن": "قيد الشحن", "شحن": "قيد الشحن",
    "تم التسليم": "تم التسليم", "مسلم": "تم التسليم", "تم تسليمه": "تم التسليم",
    "ملغي": "ملغي", "ملغى": "ملغي",
    "مرتجع": "مرتجع", "مسترجع": "مرتجع"
}

# --------------------------------------------------------------------------
# Rule Implementations
# --------------------------------------------------------------------------

def normalize_arabic_digits(val_str: str) -> Tuple[str, bool]:
    """RULE_01: Convert Arabic/Eastern digits and decimal separators to standard Latin digits."""
    if not val_str:
        return val_str, False
    has_arabic = any(c in val_str for c in "٠١٢٣٤٥٦٧٨٩٫٬")
    if has_arabic:
        converted = val_str.translate(ARABIC_EASTERN_DIGITS_MAP)
        return converted, True
    return val_str, False

def normalize_currency_and_text(val_str: str, default_currency: str = "YER") -> Tuple[Optional[float], str, List[Dict[str, Any]], Optional[str]]:
    """
    RULE_02, RULE_03, RULE_04:
    Parses amount string, handles currency words, thousands commas, known number words.
    Returns: (numeric_amount, currency, corrections, error_code)
    """
    corrections = []
    if val_str is None:
        return None, default_currency, corrections, ERR_UNKNOWN_PRICE

    raw_original = str(val_str).strip()
    if not raw_original or raw_original in ["???", "????????", "null", "none", "nan", "N/A"]:
        return None, default_currency, corrections, ERR_UNKNOWN_PRICE

    current_val = raw_original

    # 1. Arabic digits normalization (RULE_01)
    norm_digits, was_arabic = normalize_arabic_digits(current_val)
    if was_arabic:
        corrections.append({
            "rule_code": RULE_01,
            "field": "amount",
            "original_value": current_val,
            "corrected_value": norm_digits,
            "reason": "Eastern/Arabic digits converted to Latin"
        })
        current_val = norm_digits

    # 2. Known number words (RULE_04)
    cleaned_word = current_val.strip()
    if cleaned_word in KNOWN_NUMBER_WORDS:
        numeric_val = KNOWN_NUMBER_WORDS[cleaned_word]
        corrections.append({
            "rule_code": RULE_04,
            "field": "amount",
            "original_value": raw_original,
            "corrected_value": str(numeric_val),
            "reason": f"Known Arabic number word '{cleaned_word}' mapped to {numeric_val}"
        })
        return numeric_val, default_currency, corrections, None

    # 3. Currency text extraction (RULE_02)
    currency = default_currency
    if "ريال يمني" in current_val or "ريال" in current_val or "YER" in current_val:
        cleaned_no_curr = re.sub(r"(ريال\s*يمني|ريال|YER|yer)", "", current_val).strip()
        if cleaned_no_curr != current_val:
            corrections.append({
                "rule_code": RULE_02,
                "field": "amount",
                "original_value": current_val,
                "corrected_value": cleaned_no_curr,
                "reason": "Extracted currency text and normalized to YER"
            })
            current_val = cleaned_no_curr
            currency = "YER"
    elif "USD" in current_val or "دولار" in current_val:
        currency = "USD"
        cleaned_no_curr = re.sub(r"(USD|دولار)", "", current_val).strip()
        current_val = cleaned_no_curr
    elif "SAR" in current_val or "سعودي" in current_val:
        currency = "SAR"
        cleaned_no_curr = re.sub(r"(SAR|سعودي)", "", current_val).strip()
        current_val = cleaned_no_curr

    # 4. Thousands separators (RULE_03)
    if "," in current_val:
        without_commas = current_val.replace(",", "")
        corrections.append({
            "rule_code": RULE_03,
            "field": "amount",
            "original_value": current_val,
            "corrected_value": without_commas,
            "reason": "Thousands separator commas removed"
        })
        current_val = without_commas

    # 5. Parse to float
    try:
        cleaned_str = current_val.strip()
        numeric_val = float(cleaned_str)
        if numeric_val < 0:
            return None, currency, corrections, ERR_AMBIGUOUS_NEGATIVE_VALUE
        return numeric_val, currency, corrections, None
    except ValueError:
        return None, currency, corrections, ERR_UNKNOWN_PRICE

def normalize_phone(phone_str: str) -> Tuple[Optional[str], List[Dict[str, Any]], Optional[str]]:
    """
    RULE_05: Phone Number Normalization
    Cleans Yemeni mobile numbers (70, 71, 73, 77, 78) to standard 9-digit format.
    """
    corrections = []
    if not phone_str:
        return None, corrections, None

    orig_phone = str(phone_str).strip()
    norm_phone, was_arabic = normalize_arabic_digits(orig_phone)
    if was_arabic:
        corrections.append({
            "rule_code": RULE_01,
            "field": "customer_phone",
            "original_value": orig_phone,
            "corrected_value": norm_phone,
            "reason": "Arabic digits in phone converted to Latin"
        })

    # Strip symbols: +, -, spaces, parentheses
    cleaned = re.sub(r"[\s\+\-\(\)]", "", norm_phone)

    # Strip country code prefix (967, 00967) or leading 0
    if cleaned.startswith("00967"):
        cleaned = cleaned[5:]
    elif cleaned.startswith("967"):
        cleaned = cleaned[3:]
    elif cleaned.startswith("0") and len(cleaned) == 10:
        cleaned = cleaned[1:]

    # Yemeni mobile numbers are 9 digits starting with 7
    if re.match(r"^7[01378]\d{7}$", cleaned):
        if cleaned != orig_phone:
            corrections.append({
                "rule_code": RULE_05,
                "field": "customer_phone",
                "original_value": orig_phone,
                "corrected_value": cleaned,
                "reason": "Normalized phone number format to standard 9-digit Yemeni number"
            })
        return cleaned, corrections, None

    # If valid digits but slightly different or clean
    if cleaned.isdigit() and len(cleaned) == 9:
        if cleaned != orig_phone:
            corrections.append({
                "rule_code": RULE_05,
                "field": "customer_phone",
                "original_value": orig_phone,
                "corrected_value": cleaned,
                "reason": "Stripped formatting from phone number"
            })
        return cleaned, corrections, None

    return cleaned, corrections, None

def normalize_email(email_str: str) -> Tuple[Optional[str], List[Dict[str, Any]], Optional[str]]:
    """
    RULE_06: Email Repeated Symbols Fix
    Safely corrects user@@domain..com -> user@domain.com.
    Quarantines if fundamentally broken (missing @, missing domain, @@ only).
    """
    corrections = []
    if not email_str:
        return "", corrections, None

    orig_email = str(email_str).strip()

    # Broken/Unsafe email check
    if orig_email in ["@@", "@", "null", "none", "???"] or not ("@" in orig_email):
        return None, corrections, ERR_UNSAFE_EMAIL

    current = orig_email

    # Fix consecutive @@
    if "@@" in current:
        fixed_at = re.sub(r"@+", "@", current)
        corrections.append({
            "rule_code": RULE_06,
            "field": "customer_email",
            "original_value": current,
            "corrected_value": fixed_at,
            "reason": "Replaced repeated '@' symbols with a single '@'"
        })
        current = fixed_at

    # Fix consecutive ..
    if ".." in current:
        fixed_dot = re.sub(r"\.+", ".", current)
        corrections.append({
            "rule_code": RULE_06,
            "field": "customer_email",
            "original_value": current,
            "corrected_value": fixed_dot,
            "reason": "Replaced repeated dots with a single dot"
        })
        current = fixed_dot

    # Validate resulting email structure
    email_pattern = r"^[^@\s]+@[^@\s]+\.[a-zA-Z0-9]+$"
    if not re.match(email_pattern, current):
        return None, corrections, ERR_UNSAFE_EMAIL

    return current, corrections, None

def normalize_date(date_str: str) -> Tuple[Optional[str], List[Dict[str, Any]], Optional[str]]:
    """
    RULE_07: Date Normalization
    Converts valid dates in varied formats (ISO, DD-MM-YYYY, DD/MM/YYYY) to standard ISO format.
    Quarantines invalid / impossible dates (e.g. 2025-02-31, year > 2030 or < 2000).
    """
    corrections = []
    if not date_str:
        return None, corrections, ERR_INVALID_IMPOSSIBLE_DATE

    orig_date = str(date_str).strip()
    norm_date_str, was_arabic = normalize_arabic_digits(orig_date)
    if was_arabic:
        corrections.append({
            "rule_code": RULE_01,
            "field": "order_date",
            "original_value": orig_date,
            "corrected_value": norm_date_str,
            "reason": "Converted Arabic digits in date"
        })

    candidate_formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d"
    ]

    parsed_dt = None
    for fmt in candidate_formats:
        try:
            parsed_dt = datetime.strptime(norm_date_str, fmt)
            break
        except ValueError:
            continue

    if parsed_dt is None:
        return None, corrections, ERR_INVALID_IMPOSSIBLE_DATE

    if parsed_dt.year < 2000 or parsed_dt.year > 2030:
        return None, corrections, ERR_INVALID_IMPOSSIBLE_DATE

    standard_iso = parsed_dt.strftime("%Y-%m-%dT%H:%M:%S")

    if standard_iso != orig_date:
        corrections.append({
            "rule_code": RULE_07,
            "field": "order_date",
            "original_value": orig_date,
            "corrected_value": standard_iso,
            "reason": "Normalized date format to standard ISO 8601"
        })

    return standard_iso, corrections, None

def normalize_text_and_synonyms(field_name: str, val_str: str) -> Tuple[str, List[Dict[str, Any]]]:
    """
    RULE_08: Trim and Synonym Normalization for categorical fields.
    """
    corrections = []
    if not val_str:
        return "", corrections

    orig_val = str(val_str).strip()
    cleaned = re.sub(r"\s+", " ", orig_val)

    if field_name == "city":
        if cleaned in KNOWN_CITIES and KNOWN_CITIES[cleaned] != orig_val:
            mapped_city = KNOWN_CITIES[cleaned]
            corrections.append({
                "rule_code": RULE_08,
                "field": "city",
                "original_value": orig_val,
                "corrected_value": mapped_city,
                "reason": f"Standardized city synonym '{orig_val}' -> '{mapped_city}'"
            })
            return mapped_city, corrections

    elif field_name == "status":
        if cleaned in KNOWN_STATUSES and KNOWN_STATUSES[cleaned] != orig_val:
            mapped_status = KNOWN_STATUSES[cleaned]
            corrections.append({
                "rule_code": RULE_08,
                "field": "status",
                "original_value": orig_val,
                "corrected_value": mapped_status,
                "reason": f"Standardized status synonym '{orig_val}' -> '{mapped_status}'"
            })
            return mapped_status, corrections

    if cleaned != orig_val:
        corrections.append({
            "rule_code": RULE_08,
            "field": field_name,
            "original_value": orig_val,
            "corrected_value": cleaned,
            "reason": "Trimmed extra whitespace"
        })

    return cleaned, corrections

def safe_float(val: Any) -> float:
    """Helper to convert int/float/string to float safely, handling Arabic digits."""
    if isinstance(val, (int, float)):
        return float(val)
    if not val:
        return 0.0
    s_val = str(val).strip()
    norm_s, _ = normalize_arabic_digits(s_val)
    norm_s = norm_s.replace(",", "")
    try:
        return float(norm_s)
    except ValueError:
        return 0.0

def safe_int(val: Any) -> int:
    """Helper to convert int/float/string to int safely."""
    f = safe_float(val)
    return int(f)

def validate_and_recalculate_order(
    raw_total_str: str,
    delivery_cost_val: float,
    items_json_str: str
) -> Tuple[Optional[float], Optional[List[Dict[str, Any]]], List[Dict[str, Any]], Optional[str]]:
    """
    RULE_09: Order items parsing, validation, and total recalculation.
    Returns: (final_total, parsed_items, corrections, error_code)
    """
    corrections = []

    # 1. Validate items_json
    if not items_json_str or items_json_str.strip() in ["", "null", "none", "not-json"]:
        return None, None, corrections, ERR_CORRUPTED_ITEMS_JSON

    try:
        items = json.loads(items_json_str)
    except Exception:
        return None, None, corrections, ERR_CORRUPTED_ITEMS_JSON

    if not isinstance(items, list):
        return None, None, corrections, ERR_CORRUPTED_ITEMS_JSON

    if len(items) == 0:
        return None, None, corrections, ERR_EMPTY_ITEMS

    # 2. Inspect item elements and compute sum safely
    calculated_items_total = 0.0
    has_ambiguous_negative = False
    cleaned_items_list = []

    for item in items:
        if not isinstance(item, dict):
            return None, None, corrections, ERR_CORRUPTED_ITEMS_JSON
        
        qty = safe_int(item.get("qty", 0))
        unit_price = safe_float(item.get("unit_price", 0.0))
        item_total = safe_float(item.get("total", 0.0))

        if qty < 0:
            has_ambiguous_negative = True

        if qty > 0 and unit_price > 0:
            calculated_items_total += (qty * unit_price)
        else:
            calculated_items_total += item_total

        cleaned_items_list.append({
            "sku": str(item.get("sku", "")),
            "name": str(item.get("name", "")),
            "qty": qty,
            "unit_price": unit_price,
            "total": item_total
        })

    if has_ambiguous_negative and calculated_items_total <= 0:
        return None, cleaned_items_list, corrections, ERR_AMBIGUOUS_NEGATIVE_VALUE

    expected_order_total = calculated_items_total + (delivery_cost_val if delivery_cost_val and delivery_cost_val > 0 else 0.0)

    # 3. Check parsed raw total
    raw_total_num, curr, curr_corr, err = normalize_currency_and_text(raw_total_str)
    corrections.extend(curr_corr)

    if err is not None:
        if expected_order_total > 0:
            corrections.append({
                "rule_code": RULE_09,
                "field": "total_amount",
                "original_value": str(raw_total_str),
                "corrected_value": str(expected_order_total),
                "reason": f"Recalculated missing/unparseable order total from valid items ({calculated_items_total}) + delivery ({delivery_cost_val})"
            })
            return expected_order_total, cleaned_items_list, corrections, None
        else:
            return None, cleaned_items_list, corrections, err

    if abs(raw_total_num - expected_order_total) > 0.01:
        corrections.append({
            "rule_code": RULE_09,
            "field": "total_amount",
            "original_value": str(raw_total_num),
            "corrected_value": str(expected_order_total),
            "reason": f"Recalculated mismatched total from items ({calculated_items_total}) + delivery ({delivery_cost_val})"
        })
        return expected_order_total, cleaned_items_list, corrections, None

    return raw_total_num, cleaned_items_list, corrections, None

# --------------------------------------------------------------------------
# Main Record Processing & Classification Function
# --------------------------------------------------------------------------

def process_and_classify_record(raw_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes a raw ELT document from orders_raw, applies all 9 deterministic quality rules,
    tracks the Audit Trail, and assigns one and only one classification:
    - 'valid'
    - 'corrected'
    - 'quarantined'
    """
    raw_record = raw_doc.get("raw_record", {})
    id_run = raw_doc.get("id_run", "")
    source_file = raw_doc.get("source_file", "")
    source_row_number = raw_doc.get("source_row_number", 0)

    corrections: List[Dict[str, Any]] = []
    error_codes: List[str] = []

    # 1. Order ID (Stable Business Key) Validation
    order_id_raw = raw_record.get("order_id", "")
    if not order_id_raw or str(order_id_raw).strip() in ["", "null", "none", "nan"]:
        error_codes.append(ERR_MISSING_ORDER_ID)
        clean_order_id = None
    else:
        norm_order_id, was_arabic = normalize_arabic_digits(str(order_id_raw).strip())
        if was_arabic:
            corrections.append({
                "rule_code": RULE_01,
                "field": "order_id",
                "original_value": order_id_raw,
                "corrected_value": norm_order_id,
                "reason": "Normalized Arabic digits in order_id"
            })
        clean_order_id = norm_order_id

    # 2. Customer ID Validation
    cust_id_raw = raw_record.get("customer_id", "")
    if not cust_id_raw or str(cust_id_raw).strip() in ["", "null", "none", "nan"]:
        error_codes.append(ERR_MISSING_CUSTOMER_ID)
        clean_cust_id = None
    else:
        norm_cust_id, was_arabic = normalize_arabic_digits(str(cust_id_raw).strip())
        if was_arabic:
            corrections.append({
                "rule_code": RULE_01,
                "field": "customer_id",
                "original_value": cust_id_raw,
                "corrected_value": norm_cust_id,
                "reason": "Normalized Arabic digits in customer_id"
            })
        clean_cust_id = norm_cust_id

    # 3. Date Validation (RULE_07)
    order_date_raw = raw_record.get("order_date", "")
    clean_date, date_corr, date_err = normalize_date(order_date_raw)
    corrections.extend(date_corr)
    if date_err:
        error_codes.append(date_err)

    # 4. Text and Categorical Fields (RULE_08)
    clean_status, status_corr = normalize_text_and_synonyms("status", raw_record.get("status", ""))
    corrections.extend(status_corr)

    clean_cust_name, name_corr = normalize_text_and_synonyms("customer_name", raw_record.get("customer_name", ""))
    corrections.extend(name_corr)

    clean_city, city_corr = normalize_text_and_synonyms("city", raw_record.get("city", ""))
    corrections.extend(city_corr)

    clean_district, dist_corr = normalize_text_and_synonyms("district", raw_record.get("district", ""))
    corrections.extend(dist_corr)

    clean_deliv_type, deliv_corr = normalize_text_and_synonyms("delivery_type", raw_record.get("delivery_type", ""))
    corrections.extend(deliv_corr)

    clean_pay_method, paym_corr = normalize_text_and_synonyms("payment_method", raw_record.get("payment_method", ""))
    corrections.extend(paym_corr)

    clean_pay_status, pays_corr = normalize_text_and_synonyms("payment_status", raw_record.get("payment_status", ""))
    corrections.extend(pays_corr)

    # 5. Customer Phone (RULE_05)
    clean_phone, phone_corr, phone_err = normalize_phone(raw_record.get("customer_phone", ""))
    corrections.extend(phone_corr)

    # 6. Customer Email (RULE_06)
    clean_email, email_corr, email_err = normalize_email(raw_record.get("customer_email", ""))
    corrections.extend(email_corr)
    if email_err:
        error_codes.append(email_err)

    # 7. Delivery Cost Normalization
    deliv_cost_num, _, deliv_cost_corr, _ = normalize_currency_and_text(raw_record.get("delivery_cost", "0.0"))
    corrections.extend(deliv_cost_corr)
    clean_delivery_cost = deliv_cost_num if deliv_cost_num is not None else 0.0

    # 8. Items JSON & Total Recalculation (RULE_09)
    clean_total, clean_items, total_corr, total_err = validate_and_recalculate_order(
        raw_record.get("total_amount", ""),
        clean_delivery_cost,
        raw_record.get("items_json", "")
    )
    corrections.extend(total_corr)
    if total_err:
        error_codes.append(total_err)

    # 9. Payment Amount Normalization
    clean_pay_amt, currency_code, pay_corr, _ = normalize_currency_and_text(raw_record.get("payment_amount", ""))
    corrections.extend(pay_corr)

    # ----------------------------------------------------------------------
    # Classification Decision
    # ----------------------------------------------------------------------
    if error_codes:
        classification = "quarantined"
        if len(error_codes) > 1:
            error_codes.append(ERR_MULTIPLE_CONFLICTING_ERRORS)

        return {
            "classification": classification,
            "id_order": clean_order_id or order_id_raw or "UNKNOWN_ORDER",
            "id_run": id_run,
            "source_file": source_file,
            "source_row_number": source_row_number,
            "error_codes": list(set(error_codes)),
            "quarantine_reason": f"Record failed validation with errors: {', '.join(set(error_codes))}",
            "raw_record": raw_record,
            "detected_at": datetime.now().isoformat()
        }

    # No errors: Record is either Corrected or Valid
    if corrections:
        classification = "corrected"
    else:
        classification = "valid"

    validated_doc = {
        "classification": classification,
        "id_order": clean_order_id,
        "order_date": clean_date,
        "status": clean_status,
        "customer_id": clean_cust_id,
        "customer_name": clean_cust_name,
        "customer_phone": clean_phone,
        "customer_email": clean_email,
        "city": clean_city,
        "district": clean_district,
        "delivery_type": clean_deliv_type,
        "delivery_cost": clean_delivery_cost,
        "payment_method": clean_pay_method,
        "payment_status": clean_pay_status,
        "payment_amount": clean_pay_amt,
        "currency": currency_code or "YER",
        "total_amount": clean_total,
        "items": clean_items,
        "quality_status": classification,
        "corrections": corrections,
        "id_run": id_run,
        "source_file": source_file,
        "source_row_number": source_row_number,
        "processed_at": datetime.now().isoformat(),
        "raw_record": raw_record
    }

    return validated_doc
