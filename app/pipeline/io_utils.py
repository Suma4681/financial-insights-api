import pandas as pd
from .config import CURRENCY_DEFAULT, FALLBACK_ACCOUNT_ID, FALLBACK_INSTITUTION_NAME

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    if "description" not in df.columns:
        raise ValueError("CSV must contain a 'description' column")
    if "amount" not in df.columns:
        raise ValueError("CSV must contain an 'amount' column")

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    if "posted_at" not in df.columns:
        if "transacted_at" in df.columns:
            df["posted_at"] = df["transacted_at"]
        else:
            raise ValueError("CSV must contain 'posted_at' or 'transacted_at'")

    if "currency" not in df.columns:
        df["currency"] = CURRENCY_DEFAULT
    else:
        df["currency"] = df["currency"].fillna(CURRENCY_DEFAULT).astype(str).str.upper()

    if "account" in df.columns and "account_id" not in df.columns:
        df["account_id"] = df["account"]
    elif "account_id" not in df.columns:
        df["account_id"] = FALLBACK_ACCOUNT_ID

    if "institution_name" not in df.columns:
        df["institution_name"] = FALLBACK_INSTITUTION_NAME
    else:
        df["institution_name"] = df["institution_name"].fillna(FALLBACK_INSTITUTION_NAME)

    df["raw_description"] = df["description"].fillna("").astype(str)
    return df
