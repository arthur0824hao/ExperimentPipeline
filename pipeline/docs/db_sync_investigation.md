# DB-JSON Sync Investigation (Canonical Path)

**Date:** 2026-03-26  
**Bundle:** B-010 / TKT-002  
**Status:** Updated to current master

## Canonical Document Path

The canonical investigation path is:

- `pipeline/docs/db_sync_investigation.md`

No duplicate canonical copy is maintained under `docs/`.

## Sync Direction

Current sync is **DB -> JSON (one-way, best-effort)**.

In `pipeline/db_registry.py`:
- `sync_snapshot_to_json(json_path, dsn)` reads `exp_registry.snapshot_as_json()`
- writes atomically to the JSON snapshot path

`experiments.json` is treated as a compatibility artifact for tooling and fallback visibility, not as the truth source.

## Truth Source

- Truth source: PostgreSQL `exp_registry` schema
- Default experiment DB name in code: `FraudDetect-experiment`

This investigation intentionally distinguishes experiment registry DB from memory DB. They are not the same database.

## Guard Against Empty-DB Snapshot Clobber

`DBExperimentsDB._sync_snapshot()` includes a guard:
- if DB appears empty while local snapshot already has content, snapshot sync is skipped
- this prevents accidental overwrite of non-empty JSON with empty DB output during transient DB failures

## Current Findings

1. No JSON->DB writeback path was found in current master.
2. DB naming drift in old docs is corrected here.
3. Already-landed protections are documented as current behavior (not pending work).

## Related Files

- `pipeline/db_registry.py`
- `docs/architecture.md`
