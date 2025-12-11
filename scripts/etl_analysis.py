# etl_analysis.py
"""
Analysis script for Telco dataset.

Reads the 'telco_data' table from Supabase and produces:
- Metrics (churn percentage, avg monthly charges per contract, counts per tenure group, internet service distribution)
- Pivot table: Churn vs Tenure Group
- Optional visualizations:
    - Churn rate by Monthly Charge Segment
    - Histogram of TotalCharges
    - Bar plot of Contract types

Writes CSV(s) into data/processed/, plus PNG visualizations.
Primary output CSV: data/processed/analysis_summary.csv
"""

from dotenv import load_dotenv
import os
from pathlib import Path
import pandas as pd
from supabase import create_client
import matplotlib.pyplot as plt

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "telco_data"

BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Please set SUPABASE_URL and SUPABASE_KEY in your .env")


# -----------------------
# Helpers
# -----------------------
def _get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _extract_data_from_response(res):
    """
    Defensive extractor for Supabase responses — returns a list of dicts.
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


def _find_col(df: pd.DataFrame, candidates):
    """
    Return first matching column name in df for given candidate names (case-insensitive).
    candidates: list of strings
    Returns None if not found.
    """
    lc = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lc:
            return lc[cand.lower()]
    return None


# -----------------------
# Fetch table
# -----------------------
def fetch_table(limit: int | None = None) -> pd.DataFrame:
    print(f"🔍 Fetching data from Supabase table '{TABLE_NAME}' ...")
    supabase = _get_supabase_client()
    query = supabase.table(TABLE_NAME).select("*")
    if limit:
        query = query.limit(limit)
    res = query.execute()
    rows = _extract_data_from_response(res)
    df = pd.DataFrame(rows)
    if df.empty:
        print("⚠️  No rows extracted from Supabase.")
        return df

    # Normalize common numeric columns
    # Identify column names flexibly
    monthly_col = _find_col(df, ["monthlycharges", "monthly_charges", "monthlycharges", "monthlycharge", "monthly_charge"])
    total_col = _find_col(df, ["totalcharges", "total_charges", "totalcharges", "totalcharge"])
    hour_col = _find_col(df, ["hour"])
    # coerce numeric if present
    for col in [monthly_col, total_col]:
        if col:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ensure churn column normalization (could be 'churn')
    churn_col = _find_col(df, ["churn"])
    if churn_col:
        # normalize to canonical 'churn' boolean-like (Yes/No)
        df[churn_col] = df[churn_col].astype(str).str.strip().str.lower()

    return df


# -----------------------
# Analysis
# -----------------------
def analyze_and_save(df: pd.DataFrame):
    if df.empty:
        print("No data to analyze.")
        return

    # Resolve column names we will use (flexible)
    churn_col = _find_col(df, ["churn"])
    monthly_col = _find_col(df, ["monthlycharges", "monthly_charges", "monthlycharge", "monthly_charge"])
    contract_col = _find_col(df, ["contract", "contract_type", "contract_type_code"])
    tenure_group_col = _find_col(df, ["tenure_group", "tenuregroup"])
    charge_segment_col = _find_col(df, ["monthly_charge_segment", "monthlycharges_segment", "monthly_charge_segment"])
    total_col = _find_col(df, ["totalcharges", "total_charges", "totalcharge"])
    internet_col = _find_col(df, ["internetservice", "internet_service"])
    contract_type_col = contract_col  # reuse

    # ---------- KPI: churn percentage ----------
    churn_pct = None
    if churn_col and churn_col in df.columns:
        # Interpret churn as 'yes' values
        churn_yes = df[churn_col].astype(str).str.strip().str.lower().isin({"yes", "true", "1", "y"})
        churn_pct = float(churn_yes.sum() / len(df) * 100)
    else:
        print("⚠️ churn column not found.")

    # ---------- KPI: Average monthly charges per contract ----------
    avg_monthly_by_contract = pd.DataFrame()
    if monthly_col and contract_col and monthly_col in df.columns and contract_col in df.columns:
        avg_monthly_by_contract = (
            df.groupby(contract_col, dropna=False)[monthly_col]
            .mean()
            .reset_index()
            .rename(columns={monthly_col: "avg_monthly_charges", contract_col: "contract"})
        )

    # ---------- KPI: Count of tenure groups ----------
    tenure_counts = pd.Series(dtype=int)
    if tenure_group_col and tenure_group_col in df.columns:
        tenure_counts = df[tenure_group_col].fillna("MISSING").value_counts().rename_axis("tenure_group").reset_index(name="count")

    # ---------- KPI: Internet service distribution ----------
    internet_dist = pd.Series(dtype=int)
    if internet_col and internet_col in df.columns:
        internet_dist = df[internet_col].fillna("MISSING").value_counts().rename_axis("internet_service").reset_index(name="count")

    # ---------- Pivot: Churn vs Tenure Group ----------
    churn_vs_tenure = pd.DataFrame()
    if churn_col and tenure_group_col and churn_col in df.columns and tenure_group_col in df.columns:
        # create binary churn flag
        churn_flag = df[churn_col].astype(str).str.strip().str.lower().isin({"yes", "true", "1", "y"})
        pivot_df = pd.crosstab(df[tenure_group_col].fillna("MISSING"), churn_flag, normalize='index')  # proportions per tenure_group
        # rename columns
        pivot_df = pivot_df.rename(columns={False: "no_churn_pct", True: "churn_pct"}).reset_index()
        # convert to percentage
        pivot_df["no_churn_pct"] = pivot_df["no_churn_pct"] * 100
        pivot_df["churn_pct"] = pivot_df["churn_pct"] * 100
        churn_vs_tenure = pivot_df

    # ---------- Build summary metrics row ----------
    summary = {
        "churn_percentage": churn_pct,
        "rows_analyzed": len(df)
    }

    # Save primary summary CSV
    summary_df = pd.DataFrame([summary])
    summary_path = PROCESSED_DIR / "analysis_summary.csv"
    summary_df.to_csv(summary_path, index=False)
    print(f"✅ Saved summary metrics to {summary_path}")

    # Save auxiliary CSVs for detail
    if not avg_monthly_by_contract.empty:
        avg_monthly_by_contract.to_csv(PROCESSED_DIR / "avg_monthly_by_contract.csv", index=False)
        print(f"✅ Saved avg monthly charges by contract to {PROCESSED_DIR / 'avg_monthly_by_contract.csv'}")

    if not tenure_counts.empty:
        tenure_counts.to_csv(PROCESSED_DIR / "tenure_group_counts.csv", index=False)
        print(f"✅ Saved tenure group counts to {PROCESSED_DIR / 'tenure_group_counts.csv'}")

    if not internet_dist.empty:
        internet_dist.to_csv(PROCESSED_DIR / "internet_service_distribution.csv", index=False)
        print(f"✅ Saved internet service distribution to {PROCESSED_DIR / 'internet_service_distribution.csv'}")

    if not churn_vs_tenure.empty:
        churn_vs_tenure.to_csv(PROCESSED_DIR / "churn_vs_tenure_pivot.csv", index=False)
        print(f"✅ Saved churn vs tenure pivot to {PROCESSED_DIR / 'churn_vs_tenure_pivot.csv'}")

    # -------------------
    # Visualizations
    # -------------------
    try:
        # 1) Churn rate by Monthly Charge Segment (requires monthly charge segment column)
        if charge_segment_col and charge_segment_col in df.columns and churn_col and churn_col in df.columns:
            seg = df[[charge_segment_col, churn_col]].copy()
            seg["churn_flag"] = seg[churn_col].astype(str).str.strip().str.lower().isin({"yes", "true", "1", "y"})
            seg_agg = seg.groupby(charge_segment_col)["churn_flag"].mean().reset_index()
            seg_agg["churn_pct"] = seg_agg["churn_flag"] * 100

            plt.figure(figsize=(8, 4))
            plt.bar(seg_agg[charge_segment_col].astype(str), seg_agg["churn_pct"])
            plt.title("Churn Rate by Monthly Charge Segment")
            plt.ylabel("Churn Rate (%)")
            plt.xlabel("Monthly Charge Segment")
            plt.tight_layout()
            plt.savefig(PROCESSED_DIR / "churn_by_charge_segment.png")
            plt.close()
            print(f"✅ Saved churn by charge segment plot to {PROCESSED_DIR / 'churn_by_charge_segment.png'}")

        # 2) Histogram of TotalCharges
        if total_col and total_col in df.columns:
            plt.figure(figsize=(8, 4))
            df[total_col].dropna().plot(kind="hist", bins=40)
            plt.title("Distribution of TotalCharges")
            plt.xlabel("TotalCharges")
            plt.tight_layout()
            plt.savefig(PROCESSED_DIR / "totalcharges_histogram.png")
            plt.close()
            print(f"✅ Saved TotalCharges histogram to {PROCESSED_DIR / 'totalcharges_histogram.png'}")

        # 3) Bar plot of Contract types
        if contract_type_col and contract_type_col in df.columns:
            plt.figure(figsize=(8, 4))
            ct = df[contract_type_col].fillna("MISSING").astype(str).value_counts()
            ct.plot(kind="bar")
            plt.title("Contract Type Distribution")
            plt.ylabel("Count")
            plt.tight_layout()
            plt.savefig(PROCESSED_DIR / "contract_type_distribution.png")
            plt.close()
            print(f"✅ Saved contract type distribution to {PROCESSED_DIR / 'contract_type_distribution.png'}")

    except Exception as e:
        print(f"⚠️ Plotting failed: {e}")

    print("\n🎯 Analysis finished. Files written to data/processed/.")


def run_analysis(limit: int | None = None):
    df = fetch_table(limit=limit)
    analyze_and_save(df)


if __name__ == "__main__":
    run_analysis()
