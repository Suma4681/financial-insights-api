# financial-insights-api — Bank Transaction Normalization + MongoDB Upserts + Gemini Q&A (Checking Only)

This project implements a production-style backend workflow to:

1. Ingest **raw bank Checking CSV transactions**
2. Normalize them into a **canonical transaction schema**
3. Categorize transactions using a **deterministic rules engine** aligned to a P&L structure
4. Store results in **MongoDB** with **idempotent upserts** using `(business_id, provider_txn_id)`
5. Provide:
   - a cashflow summary endpoint
   - a Gemini-backed `/qa` endpoint that answers questions using **only the computed numbers** (grounded context)

✅ **Important scope choice:** This implementation intentionally uses **only the Checking Account raw CSV**.  
The Credit Card raw file is not ingested (by design) to keep the pipeline focused and avoid mixing statement-style credit card data with bank cashflow transactions.

---

## Architecture (High Level)

**Raw CSV → DataFrame → Canonical Schema → Rules Categorization → MongoDB Upsert → Summary → Gemini Q&A**

- **Normalization:** builds canonical fields, deterministic IDs, standard types
- **Categorization:** keyword-driven deterministic mapping (no LLM used for categories)
- **MongoDB storage:** `bank_transactions` collection, unique compound index for idempotency
- **Q&A:** `/cashflow/summary` generates numeric context; `/qa` asks Gemini using only those numbers

---

## Canonical Transaction Schema

Each transaction is normalized into:

```json
{
  "transaction_id": "deterministic-hash",
  "business_id": "demo-business-1",
  "account_id": "bank-1",
  "provider_txn_id": "deterministic-hash",
  "posted_at": "2025-11-10T10:30:00Z",
  "amount": -523.40,
  "currency": "USD",
  "cashflow_type": "OUTFLOW",
  "category": "OUTFLOW_COGS_FOOD",
  "vendor_name": "Sysco Foods",
  "raw_description": "SYSCO FOODS 1234",
  "source": "bank",
  "meta": {
    "import_batch_id": "string",
    "institution_name": "demo-bank"
  },
  "created_at": "ISO timestamp",
  "updated_at": "ISO timestamp"
}
