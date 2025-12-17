import pandas as pd

def apply_cashflow_and_category_rules(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # initialize
    df["cashflow_type"] = None
    df["category"] = None

    # normalize description once
    desc = df["raw_description"].fillna("").astype(str).str.upper()

    # -------------------------
    # 1) TRANSFER (must be first)
    # -------------------------
    mask_internal = desc.str.contains(
        r"CREDIT CARD PAYMENT|AUTOPAY|ONLINE TRANSFER",
        regex=True, na=False
    )
    df.loc[mask_internal, ["cashflow_type", "category"]] = ["TRANSFER", "TRANSFER_INTERNAL"]

    mask_external = desc.str.contains(
        r"ZELLE|VENMO|WIRE TRANSFER",
        regex=True, na=False
    )
    df.loc[mask_external, ["cashflow_type", "category"]] = ["TRANSFER", "TRANSFER_EXTERNAL"]

    # -------------------------
    # 2) WITHDRAWAL (after transfer, before inflow/outflow)
    # -------------------------
    mask_unclassified = df["cashflow_type"].isna()

    mask_atm = (
        mask_unclassified
        & (df["amount"] < 0)
        & desc.str.contains(r"\bATM\b|CASH WITHDRAWAL|ATM WITHDRAWAL", regex=True, na=False)
    )
    df.loc[mask_atm, ["cashflow_type", "category"]] = ["WITHDRAWAL", "WITHDRAWAL_ATM"]

    mask_owner = (
        mask_unclassified
        & (df["amount"] < 0)
        & desc.str.contains(r"OWNER DRAW|OWNERS DRAW|OWNER WITHDRAWAL", regex=True, na=False)
    )
    df.loc[mask_owner, ["cashflow_type", "category"]] = ["WITHDRAWAL", "WITHDRAWAL_OWNER_DRAW"]

    # -------------------------
    # 3) INFLOW (amount > 0)
    # -------------------------
    mask_inflow_shift4 = (
        df["cashflow_type"].isna()
        & (df["amount"] > 0)
        & desc.str.contains(r"\bSHIFT4\b|CARD\s*SALES|BATCH", regex=True, na=False)
    )
    df.loc[mask_inflow_shift4, ["cashflow_type", "category"]] = ["INFLOW", "INFLOW_CARD_SALES"]

    mask_inflow_other = df["cashflow_type"].isna() & (df["amount"] > 0)
    df.loc[mask_inflow_other, ["cashflow_type", "category"]] = ["INFLOW", "INFLOW_PROCESSOR_OTHER"]

    # -------------------------
    # 4) OUTFLOW (P&L mapping)
    # -------------------------

    # LABOR
    mask_payroll = (
        df["cashflow_type"].isna()
        & (df["amount"] < 0)
        & desc.str.contains(r"ADP WAGE|ADP PAYROLL|PAYROLL\b", regex=True, na=False)
    )
    df.loc[mask_payroll, ["cashflow_type", "category"]] = ["OUTFLOW", "OUTFLOW_LABOR_PAYROLL"]

    mask_taxes = (
        df["cashflow_type"].isna()
        & (df["amount"] < 0)
        & desc.str.contains(r"ADP TAX|PAYROLL TAX", regex=True, na=False)
    )
    df.loc[mask_taxes, ["cashflow_type", "category"]] = ["OUTFLOW", "OUTFLOW_LABOR_TAXES"]

    mask_benefits = (
        df["cashflow_type"].isna()
        & (df["amount"] < 0)
        & desc.str.contains(r"PAYROLL FEES|ADP BENEFITS|ADP PAY-BY-PAY", regex=True, na=False)
    )
    df.loc[mask_benefits, ["cashflow_type", "category"]] = ["OUTFLOW", "OUTFLOW_LABOR_BENEFITS"]

    # COGS FOOD (your vendor list)
    mask_cogs_food = (
        df["cashflow_type"].isna()
        & (df["amount"] < 0)
        & desc.str.contains(
            r"SYSCO|BALDOR|UNION BEER|MANHATTAN BEER|EMPIRE|SGWS|ANHEUSER|WOOLCO|MS WALKER",
            regex=True, na=False
        )
    )
    df.loc[mask_cogs_food, ["cashflow_type", "category"]] = ["OUTFLOW", "OUTFLOW_COGS_FOOD"]

    # INSURANCE
    mask_insurance = (
        df["cashflow_type"].isna()
        & (df["amount"] < 0)
        & desc.str.contains(r"INSUR|FAIRMONT|IPFS", regex=True, na=False)
    )
    df.loc[mask_insurance, ["cashflow_type", "category"]] = ["OUTFLOW", "OUTFLOW_INSURANCE"]

    # MAINTENANCE / WASTE
    mask_maintenance = (
        df["cashflow_type"].isna()
        & (df["amount"] < 0)
        & desc.str.contains(r"CARTING|WASTE|TRASH", regex=True, na=False)
    )
    df.loc[mask_maintenance, ["cashflow_type", "category"]] = ["OUTFLOW", "OUTFLOW_MAINTENANCE"]

    # LOANS / DEBT SERVICE
    mask_loan = (
        df["cashflow_type"].isna()
        & (df["amount"] < 0)
        & desc.str.contains(r"\bSBA\b|\bLOAN\b|TERM LOAN|PROMISSORY", regex=True, na=False)
    )
    df.loc[mask_loan, ["cashflow_type", "category"]] = ["OUTFLOW", "OUTFLOW_LOAN_PAYMENT"]

    # CHECKS -> misc opex
    mask_check = (
        df["cashflow_type"].isna()
        & (df["amount"] < 0)
        & desc.str.contains(r"\bCHECK\s*#", regex=True, na=False)
    )
    df.loc[mask_check, ["cashflow_type", "category"]] = ["OUTFLOW", "OUTFLOW_MISC_OPEX"]

    # SOFTWARE (do NOT include SHIFT4 here)
    mask_software = (
        df["cashflow_type"].isna()
        & (df["amount"] < 0)
        & desc.str.contains(r"SEATED|SOFTWARE|SAAS", regex=True, na=False)
    )
    df.loc[mask_software, ["cashflow_type", "category"]] = ["OUTFLOW", "OUTFLOW_SOFTWARE"]

    # -------------------------
    # 5) FINAL FALLBACKS
    # -------------------------
    mask_fallback_outflow = df["cashflow_type"].isna() & (df["amount"] < 0)
    df.loc[mask_fallback_outflow, ["cashflow_type", "category"]] = ["OUTFLOW", "OUTFLOW_VENDOR_NONCOGS"]

    # optional: if anything still unlabeled (rare) leave None or mark as UNKNOWN
    return df
