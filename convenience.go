package kvlite

// TODO(component:convenience): thin one-shot string API over buckets —
// db.Get/Put/Delete(bucket, key, value string), each wrapping a transaction and
// returning copied strings (safe outside the txn). Strict wrapper over the core.
