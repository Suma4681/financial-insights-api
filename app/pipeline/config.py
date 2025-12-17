import os

BUSINESS_ID = os.getenv("BUSINESS_ID", "demo-business-1")
SOURCE = os.getenv("SOURCE", "bank")
CURRENCY_DEFAULT = os.getenv("CURRENCY_DEFAULT", "USD")

FALLBACK_ACCOUNT_ID = os.getenv("FALLBACK_ACCOUNT_ID", "bank-1")
FALLBACK_INSTITUTION_NAME = os.getenv("FALLBACK_INSTITUTION_NAME", "demo-bank")
