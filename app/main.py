from __future__ import annotations
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from uuid import uuid4
import uuid
from app.db import get_collection
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from app.db import collection, ensure_indexes
from app.mongo import upsert_transactions

from app.pipeline.io_utils import load_csv
from app.pipeline.vendor import extract_vendor_name
from app.pipeline.canonical import build_canonical
from app.pipeline.categorize import apply_cashflow_and_category_rules

# IMPORTANT:
# Make sure your gemini_client.py exposes a function named `ask_gemini(prompt: str) -> str`
from app.gemini_client import ask_gemini


app = FastAPI(title="Finz Assignment API")


@app.on_event("startup")
def startup() -> None:
    # Creates unique index for (business_id, provider_txn_id) and ignores "already exists" conflicts
    ensure_indexes()


# ----------------------------
# Models
# ----------------------------
class IngestResponse(BaseModel):
    created: int
    updated: int
    skipped: int


class CashflowSummaryResponse(BaseModel):
    totals: Dict[str, float]
    by_category: Dict[str, float]
    top_vendors: List[Dict[str, Any]]


class QARequest(BaseModel):
    question: str = Field(..., min_length=3)
    start_date: str = Field(..., description="YYYY-MM-DD")
    end_date: str = Field(..., description="YYYY-MM-DD")


class QAResponse(BaseModel):
    answer: str
    context: Dict[str, Any]


# ----------------------------
# Helpers
# ----------------------------
def _iso_range(start_date: str, end_date: str) -> tuple[str, str]:
    # Keep it simple: inclusive day range
    start_iso = f"{start_date}T00:00:00Z"
    end_iso = f"{end_date}T23:59:59Z"
    return start_iso, end_iso


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")



def _build_summary_from_mongo(business_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
    start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end_date).replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )

    match = {
        "business_id": business_id,
        "posted_at": {"$gte": start_dt, "$lte": end_dt},
    }

    # totals inflow/outflow
    totals_pipeline = [
        {"$match": match},
        {"$group": {"_id": "$cashflow_type", "total": {"$sum": "$amount"}}},
    ]


    totals_rows = list(collection.aggregate(totals_pipeline))

    inflows = sum(r["total"] for r in totals_rows if r["_id"] == "INFLOW")
    # outflows are negative amounts; present as positive number
    outflows = abs(sum(r["total"] for r in totals_rows if r["_id"] == "OUTFLOW"))
    net_cash = inflows - outflows

    # by_category (only outflows)
    by_cat_pipeline = [
        {"$match": {**match, "cashflow_type": "OUTFLOW"}},
        {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": 1}},  # most negative first
    ]
    cat_rows = list(collection.aggregate(by_cat_pipeline))
    by_category = {r["_id"]: abs(r["total"]) for r in cat_rows if r["_id"]}

    # top vendors (only outflows)
    top_vendor_pipeline = [
        {"$match": {**match, "cashflow_type": "OUTFLOW"}},
        {"$group": {"_id": "$vendor_name", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": 1}},  # most negative first
        {"$limit": 10},
    ]
    vendor_rows = list(collection.aggregate(top_vendor_pipeline))
    top_vendors = [{"vendor": r["_id"], "amount": abs(r["total"])} for r in vendor_rows if r["_id"]]

    return {
        "totals": {"inflows": float(inflows), "outflows": float(outflows), "net_cash": float(net_cash)},
        "by_category": {k: float(v) for k, v in by_category.items()},
        "top_vendors": top_vendors,
        "start_date": start_date,
        "end_date": end_date,
    }


def _make_gemini_prompt(question: str, summary: Dict[str, Any]) -> str:
    # Hard-grounding instruction (very important for recruiter)
    return f"""
You are an AI finance operator assistant.
Use ONLY the numbers provided in the CONTEXT. If something is not present, say you don't have enough data.

CONTEXT (30-day window):
- Period: {summary["start_date"]} to {summary["end_date"]}
- Totals: {summary["totals"]}
- By Category: {summary["by_category"]}
- Top Vendors: {summary["top_vendors"]}

QUESTION:
{question}

Answer clearly and concisely. Include key numbers. Do not invent values.
""".strip()


# ----------------------------
# Endpoints
# ----------------------------
@app.post("/ingest/bank-transactions", response_model=IngestResponse)
def ingest_bank_transactions(
    csv_path: str = Query(..., description="Path to raw CSV file on disk, e.g. data/The Winslow_Checking.csv"),
    import_batch_id: Optional[str] = Query(None),
    business_id: str = Query("demo-business-1"),
    account_id: str = Query("bank-1"),
    institution_name: str = Query("demo-bank"),
) -> IngestResponse:
    """
    Reads raw CSV -> canonical normalize -> deterministic categorize -> Mongo upsert (idempotent).
    """
    batch_id = import_batch_id or str(uuid4())

    # 1) load raw
    
    raw_df = load_csv(csv_path)
    print("raw_df type:", type(raw_df))
    raw_df["vendor_name"] = extract_vendor_name(raw_df["description"])
    # categorize
    raw_df = apply_cashflow_and_category_rules(raw_df)

    # quick check: what is still missing?
    missing_cat = raw_df["category"].isna().sum()
    print("Raw rows:", raw_df.shape)
    print("Missing category:", missing_cat)

# build canonical
    batch_id = f"import_{uuid.uuid4().hex[:12]}"
    canonical_df = build_canonical(raw_df, import_batch_id=batch_id)
    print("Canonical rows:", canonical_df.shape)


    # 5) add created_at/updated_at timestamps (upsert will respect created_at on insert)
    now = _now_iso()
    canonical_df["updated_at"] = now
    if "created_at" not in canonical_df.columns:
        canonical_df["created_at"] = now
    canonical_df["posted_at"] = pd.to_datetime(
    canonical_df["posted_at"],
    errors="coerce",
    utc=True
)

# Convert pandas Timestamp → python datetime (Mongo-friendly)
    canonical_df["posted_at"] = pd.to_datetime(
    canonical_df["posted_at"],
    utc=True
)

    # convert DataFrame -> list[dict] (THIS is where your .to_dict() should be)
    collection = get_collection()
    docs = canonical_df.where(canonical_df.notna(), None).to_dict(orient="records")

    # 6) upsert
    created, updated, skipped = upsert_transactions(docs)

    return IngestResponse(created=created, updated=updated, skipped=skipped)


@app.get("/cashflow/summary", response_model=CashflowSummaryResponse)
def cashflow_summary(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    business_id: str = Query("demo-business-1"),
) -> CashflowSummaryResponse:
    summary = _build_summary_from_mongo(business_id, start_date, end_date)
    return CashflowSummaryResponse(
        totals=summary["totals"],
        by_category=summary["by_category"],
        top_vendors=summary["top_vendors"],
    )


@app.post("/qa", response_model=QAResponse)
def qa(req: QARequest, business_id: str = Query("demo-business-1")) -> QAResponse:
    summary = _build_summary_from_mongo(business_id, req.start_date, req.end_date)

    prompt = _make_gemini_prompt(req.question, summary)

    try:
        answer = ask_gemini(prompt)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return QAResponse(answer=answer, context=summary)
