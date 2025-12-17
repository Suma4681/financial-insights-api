from app.db import get_collection

def upsert_transactions(docs: list[dict]):
    collection = get_collection()
    created = updated = skipped = 0

    for record in docs:
        record = dict(record)              # copy
        created_at = record.pop("created_at", None)  # remove from $set

        result = collection.update_one(
            {"business_id": record["business_id"], "provider_txn_id": record["provider_txn_id"]},
            {
                "$set": record,
                "$setOnInsert": {"created_at": created_at},
            },
            upsert=True,
        )

        if result.upserted_id is not None:
            created += 1
        elif result.modified_count > 0:
            updated += 1
        else:
            skipped += 1

    return created, updated, skipped

    return created, updated, skipped
