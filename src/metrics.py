"""
Metrics and Reporting Module for Midterm Data Pipeline.
Calculates, formats, and saves comprehensive execution metrics to:
- reports/results.json (Machine-readable JSON)
- reports/results.md (Structured Markdown report for Viva / discussion)
"""
import os
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any

from config.settings import REPORTS_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def save_pipeline_metrics(metrics: Dict[str, Any], output_filename: str = "results.json") -> Path:
    """
    Saves metrics dictionary to reports/results.json and creates human-readable reports/results.md.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORTS_DIR / output_filename
    md_path = REPORTS_DIR / output_filename.replace(".json", ".md")

    # Add timestamp
    metrics["generated_at"] = datetime.now(timezone.utc).isoformat()

    # 1. Write JSON report
    with open(json_path, mode="w", encoding="utf-8") as f_json:
        json.dump(metrics, f_json, ensure_ascii=False, indent=2)

    # 2. Write Markdown summary report
    md_content = f"""# Midterm Data Pipeline — Execution Report
**Run ID:** `{metrics.get('run_id')}`  
**Generated At:** `{metrics.get('generated_at')}`  
**Status:** **{metrics.get('status', 'SUCCESS')}**

---

## 1. Input File & Engine Selection
| Property | Value |
|---|---|
| **Input File** | `{metrics.get('file_name')}` |
| **File Size** | `{metrics.get('file_size_mb')} MB` |
| **Threshold** | `{metrics.get('threshold_mb', 200.0)} MB` |
| **Engine Used** | `{metrics.get('used_engine')}` |
| **Partitions / Batch Size** | `{metrics.get('partitions', metrics.get('batch_size'))}` |

---

## 2. Ingestion & ELT Processing Metrics
| Metric | Count / Value |
|---|---|
| **Total Read Rows** | `{metrics.get('read_rows', 0):,}` |
| **Raw Loaded (orders_raw)** | `{metrics.get('loaded_raw', 0):,}` |
| **Valid Records** | `{metrics.get('count_valid', 0):,}` |
| **Corrected Records** | `{metrics.get('count_corrected', 0):,}` |
| **Quarantined Records** | `{metrics.get('count_quarantine', 0):,}` |
| **Consistency Equation** | **`{metrics.get('consistency_equation', 'PASS')}`** (`Raw == Valid + Corrected + Quarantined`) |

---

## 3. Idempotent Upsert Breakdown (orders_validated)
| Upsert Result | Count |
|---|---|
| **Inserted (New Business Records)** | `{metrics.get('count_inserted', 0):,}` |
| **Updated (Modified Existing)** | `{metrics.get('count_updated', 0):,}` |
| **Unchanged (Exact Match)** | `{metrics.get('count_unchanged', 0):,}` |

---

## 4. Performance & Throughput
- **Total Pipeline Time:** `{metrics.get('seconds_elapsed', 0.0):.3f} seconds`
- **Overall Throughput:** `{metrics.get('throughput', 0.0):,.2f} records/second`

---

## 5. Rules & Quarantine Error Distribution
### Cleaning Rules Triggered:
```json
{json.dumps(metrics.get('rule_case_counts', {}), ensure_ascii=False, indent=2)}
```

### Quarantine Reasons:
```json
{json.dumps(metrics.get('error_case_counts', {}), ensure_ascii=False, indent=2)}
```
"""
    with open(md_path, mode="w", encoding="utf-8") as f_md:
        f_md.write(md_content)

    logger.info(f"Metrics reports successfully written to:")
    logger.info(f"  JSON: {json_path}")
    logger.info(f"  MD:   {md_path}")
    return json_path
