# validate.py
"""
Validation script for Telco dataset after load.

Checks:
- No missing values in: tenure, MonthlyCharges, TotalCharges
- Unique count of rows in staged CSV equals original dataset row count (by unique key if available)
- Row count matches Supabase table
- All segments (tenure_group, monthly_charge_segment) exist (no unexpected nulls/values)
- Contract codes are only {0,1,2}

Prints a validation summary.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from supabase import create_client

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
STAGED_PATH = BASE_DIR / "data" / "staged" / "Customer_transformed.csv"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "telco_data"


def _get_supabase_client():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise SystemExit("Please set SUPABASE_URL and SUPABASE_KEY in your .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _extract_data_from_response(res):
    """
    Defensive extractor for different Supabase client response shapes.
    Returns list of dicts.
    """
    data = getattr(res, "data", None)
    if isinstance(data, list):
        return data

    try:
        if isinstance(res, dict) and isinstance(res.get("data"), list):
            return res["data"]
    except Exception:
        pass

    if isinstance(res, (list, tuple)):
        # Find first element that is a list of dicts
        for item in res:
            if isinstance(item, list) and all(isinstance(x, dict) for x in item):
                return item
        if len(res) > 0 and isinstance(res[0], dict):
            return list(res)

    json_like = getattr(res, "json", None)
    if callable(json_like):
        try:
            j = res.json()
            if isinstance(j, dict) and isinstance(j.get("data"), list):
                return j["data"]
        except Exception:
            pass

    return []


def validate():
    print("🔎 Starting validation...")

    # 1) Check staged CSV exists
    if not STAGED_PATH.exists():
        print(f"❌ Staged file not found at: {STAGED_PATH}")
        return

    df = pd.read_csv(STAGED_PATH)
    total_rows_file = len(df)
    print(f"📄 Rows in staged CSV: {total_rows_file}")

    # 2) No missing values in tenure, MonthlyCharges, TotalCharges
    missing_checks = {}
    for col in ["tenure", "MonthlyCharges", "TotalCharges"]:
        if col in df.columns:
            missing_count = df[col].isna().sum()
            missing_checks[col] = int(missing_count)
        else:
            missing_checks[col] = None  # column missing entirely

    print("\n✅ Missing-value checks (expected 0):")
    for col, miss in missing_checks.items():
        print(f" - {col}: {miss}")

    # 3) Unique count of rows = original dataset
    # We will compare unique on all columns if no explicit key.
    unique_rows = len(df.drop_duplicates())
    print(f"\n🔁 Unique rows in staged CSV: {unique_rows}")
    if unique_rows == total_rows_file:
        print(" - No duplicate rows detected.")
    else:
        print(" - Duplicate rows detected!")

    # 4) Row count matches Supabase table
    try:
        supabase = _get_supabase_client()
        res = supabase.table(TABLE_NAME).select("id").execute()
        data = _extract_data_from_response(res)
        total_rows_db = len(data)
        print(f"\n🗄️ Rows in Supabase table '{TABLE_NAME}': {total_rows_db}")
        if total_rows_db == total_rows_file:
            print(" - Row count matches between staged CSV and Supabase.")
        else:
            print(" - Row count mismatch! Investigate missing/extra rows.")
    except Exception as e:
        print(f"⚠️ Could not fetch table rows from Supabase: {e}")
        total_rows_db = None

    # 5) All segments exist: tenure_group, monthly_charge_segment (no unexpected nulls)
    seg_checks = {}
    for seg in ["tenure_group", "monthly_charge_segment"]:
        if seg in df.columns:
            nulls = int(df[seg].isna().sum())
            unique_vals = df[seg].dropna().unique().tolist()
            seg_checks[seg] = {"nulls": nulls, "unique_values": unique_vals}
        else:
            seg_checks[seg] = None

    print("\n📊 Segment checks:")
    for seg, info in seg_checks.items():
        if info is None:
            print(f" - {seg}: column missing")
        else:
            print(f" - {seg}: nulls={info['nulls']}, unique_values={info['unique_values']}")

    # 6) Contract codes only {0,1,2}
    contract_check = {}
    if "contract_type_code" in df.columns:
        unique_codes = sorted(df["contract_type_code"].dropna().unique().tolist())
        allowed = {0, 1, 2}
        invalid_codes = [c for c in unique_codes if int(c) not in allowed]
        contract_check["unique_codes"] = unique_codes
        contract_check["invalid_codes"] = invalid_codes
    else:
        contract_check["error"] = "contract_type_code column missing"

    print("\n🔢 Contract codes check:")
    if "error" in contract_check:
        print(f" - {contract_check['error']}")
    else:
        print(f" - unique_codes: {contract_check['unique_codes']}")
        if contract_check["invalid_codes"]:
            print(f" - INVALID codes found: {contract_check['invalid_codes']}")
        else:
            print(" - All contract codes are valid (0,1,2).")

    # Final summary
    print("\n📋 Validation Summary:")
    print(f" - staged_rows: {total_rows_file}")
    print(f" - unique_rows: {unique_rows}")
    print(f" - missing_values: {missing_checks}")
    if total_rows_db is not None:
        print(f" - supabase_rows: {total_rows_db}")
    print(f" - segments: {seg_checks}")
    print(f" - contract_check: {contract_check}")

    print("\n✅ Validation completed.")


if __name__ == "__main__":
    validate()
