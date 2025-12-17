import uuid
from app.pipeline.io_utils import load_csv
from app.pipeline.vendor import extract_vendor_name
from app.pipeline.categorize import apply_cashflow_and_category_rules
from app.pipeline.canonical import build_canonical, write_outputs

df = load_csv("data/The Winslow_Checking.csv")
df["vendor_name"] = extract_vendor_name(df["description"])
df = apply_cashflow_and_category_rules(df)

batch_id = f"import_{uuid.uuid4().hex[:12]}"
canonical_df = build_canonical(df, import_batch_id=batch_id)

print("Missing category:", df["category"].isna().sum())
print("Unique transaction_id:", canonical_df["transaction_id"].nunique(), "/", len(canonical_df))

write_outputs(canonical_df, out_dir="output")
canonical_df.head()
