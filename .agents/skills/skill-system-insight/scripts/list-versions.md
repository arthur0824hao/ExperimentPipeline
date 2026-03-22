# Procedure: List Evolution Versions
Goal: list stored evolution snapshots for a user, optionally filtered by target, in most-recent-first order.

## Step 0: Query snapshots
Run (fill `{user}`):
```sql
SELECT id, version_tag, target, trigger_reason, created_at FROM get_evolution_history('{user}', 50);
```
(typed table)

Optional target filter:
- After fetching rows, filter by tags containing:
  - `target:soul`
  - `target:recipe`
  - `target:both`

## Step 1: Extract fields
When using typed table, `version_tag`, `target`, and `trigger_reason` are direct columns.
For each row:
- `version_tag`: from the row
- `target`: from the row
- `created_at`: from the row
- `summary`: from `trigger_reason` (typed) or `title` (agent_memories)

## Step 2: Format output table
Render as:
```text
version_tag | target | created_at | summary
```

Sorting:
- Most recent first (already sorted by SQL).

## Notes
- If more than 50 versions exist, paginate by adjusting LIMIT/OFFSET.
