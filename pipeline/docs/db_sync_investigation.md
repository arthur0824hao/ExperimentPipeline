# DB-JSON Sync Investigation

**Date:** 2026-03-22
**Ticket:** B-007 / TKT-003
**Status:** COMPLETE

## What is the sync direction?

**DB → JSON (one-way, best-effort).**

The only sync mechanism is `sync_snapshot_to_json()` (db_registry.py line ~379), which:
1. Calls `exp_registry.snapshot_as_json()` (DB function) to get full DB state as JSON
2. Writes it atomically to `experiments.json`

There is NO code path where `experiments.json` is read and written back to the DB.

## Is there any code path where JSON overwrites DB?

**No.**

The `INSERT INTO` / `UPSERT` operations in db_registry.py (lines 1349, 2028) are:
- `disabled_workers` table inserts (user-triggered)
- `archived_experiments` table inserts (user-triggered archiving)

These are all write operations initiated by user actions, NOT by reading from JSON.

## Does the experiment DB exist?

**Yes.** `ExperimentPipeline-memory` DB exists and is accessible:
- Host: localhost:5432
- Owner: arthur0824hao
- Contains `exp_registry` schema

## What happens when get_conn fails?

When the DB is unreachable:
1. `DBExperimentsDB.load()` throws an exception in the `try` block (line ~485-516)
2. The `except` block (line ~528+) catches it
3. **Before TKT-004 fix:** returned `{"experiments": [], "completed": [], "archived": []}` directly, potentially wiping JSON on next `_sync_snapshot()` call
4. **After TKT-004 fix:** tries to read from `experiments.json` first. If JSON has data, uses it. Only falls back to empty dict if JSON is also unavailable.

## Root Cause Hypothesis

The reported "DB sync issue" likely occurs when:
1. DB is temporarily unreachable during startup/operation
2. `load()` returns empty data
3. `_sync_snapshot()` is called (e.g., periodically or on write)
4. **Before TKT-004 fix:** `sync_snapshot_to_json()` would write the empty data to `experiments.json` via atomic replace
5. This would wipe the JSON file's previous contents
6. Next time DB is back, the JSON has stale/empty data while DB has correct data
7. But since the primary source is DB, this doesn't cause DB corruption

**TKT-004 fixes this** by:
- Guarding `load()` to fall back to JSON on DB failure
- Guarding `_sync_snapshot()` to NOT sync empty DB data if JSON already has content

## Key Code Locations

| File | Function | Purpose |
|------|----------|---------|
| db_registry.py:379 | `sync_snapshot_to_json()` | DB → JSON sync (one way) |
| db_registry.py:462 | `_sync_snapshot()` | Wrapper (now guarded by TKT-004) |
| db_registry.py:483 | `load()` | Read from DB (now guarded by TKT-004) |
| db_registry.py:544 | `load_all_for_panel()` | NEW - read ALL experiments including queue |
| experiments.py:2512 | startup code | Calls `sync_snapshot_to_json` on startup |

## Conclusion

The sync is unidirectional (DB → JSON). There is no JSON → DB sync path. The "DB sync issue" was likely caused by the `_sync_snapshot()` guard being too permissive, allowing empty DB data to overwrite non-empty JSON. TKT-004 fixes this.
