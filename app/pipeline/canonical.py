import os
import hashlib
from datetime import datetime, timezone
import pandas as pd
from .config import BUSINESS_ID, SOURCE, FALLBACK_INSTITUTION_NAME

def deterministic_txn_id(row: pd.Series) -> str:
    raw = "|".join([
        str(row.get("account_id", "")),
        str(row.get("posted_at", "")),
        str(row.get("amount", "")),
        str(row.get("raw_description", "")),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def build_canonical(df: pd.DataFrame, import_batch_id: str) -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()
    out = pd.DataFrame()

    out["transaction_id"] = df.apply(deterministic_txn_id, axis=1)
    out["business_id"] = BUSINESS_ID
    out["account_id"] = df["account_id"].astype(str)
    out["provider_txn_id"] = out["transaction_id"]

    out["posted_at"] = df["posted_at"].astype(str)
    out["amount"] = df["amount"]
    out["currency"] = df["currency"].astype(str).str.upper()

    out["cashflow_type"] = df["cashflow_type"]
    out["category"] = df["category"]

    out["vendor_name"] = df.get("vendor_name")
    out["raw_description"] = df["raw_description"]

    out["source"] = SOURCE
    out["meta"] = df.apply(
        lambda r: {
            "import_batch_id": import_batch_id,
            "institution_name": r.get("institution_name", FALLBACK_INSTITUTION_NAME),
        },
        axis=1,
    )

    out["created_at"] = now
    out["updated_at"] = now
    return out

def write_outputs(canonical_df: pd.DataFrame, out_dir: str = "output"):
    os.makedirs(out_dir, exist_ok=True)
    jsonl_path = os.path.join(out_dir, "canonical_transactions.jsonl")
    csv_path = os.path.join(out_dir, "canonical_transactions.csv")
    canonical_df.to_json(jsonl_path, orient="records", lines=True, force_ascii=False)
    canonical_df.to_csv(csv_path, index=False)
    return jsonl_path, csv_path
