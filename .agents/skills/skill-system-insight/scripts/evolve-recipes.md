# Procedure: Evolve Recipes
Goal: evolve workflow recipes conservatively based on workflow facets, storing versioned snapshots in Postgres.

Artifacts involved:
- Prompt: `prompts/recipe-evolution.md`
- Recipes: `../skill-system-workflow/recipes/*.yaml`
- Storage: Postgres `agent_memories` via `store_memory()`

Versioning constraints:
- Max 1 evolution per day per user (shared limit with soul evolution).
- Version tag format: `v{N}_{target}_{timestamp}` (example: `v4_recipe_20260211`).

## Step 0: Rate limit check (24h)
Run (fill `{user}`):
```sql
SELECT id, created_at, version_tag FROM evolution_snapshots
WHERE user_id='{user}' AND created_at >= (NOW() - INTERVAL '24 hours')
ORDER BY created_at DESC LIMIT 1;
```

Decision:
- If a row is returned: STOP. Report the most recent snapshot id/title/time and do not evolve.
- If no row: continue.

## Step 1: Load workflow-related facets
Query recent facets and filter for workflow/recipe signals (either in SQL via text match or client-side after fetching):
```sql
SELECT * FROM get_recent_facets('{user}', 200);
```
(typed table; filter client-side for workflow signals)

Filter criteria (examples):
- Mentions of recipe ids/names
- Repeated retries, step confusion, or missing prerequisites
- Long-running sessions that stall at the same step

## Step 2: Load current recipes
Read all YAML files in:
- `../skill-system-workflow/recipes/`

## Step 3: Analyze with the recipe evolution prompt
Use:
- `prompts/recipe-evolution.md`

Provide:
- Filtered workflow facets (from Step 1)
- Full recipe contents (from Step 2)

Expected output:
- Markdown plan listing candidates and unified diffs.

## Step 4: Validate proposed changes
For each candidate recipe diff:
- Confirm 3+ supporting observations are cited.
- Confirm the change is minimal and addresses the failure mode.
- Safety rule: never delete recipes.

Deprecation guidance (instead of delete):
- Keep the recipe file.
- Add a clear deprecation note/flag.
- Point to a replacement recipe if applicable.

## Step 5: Apply changes to recipe files
1. Apply diffs to the corresponding recipe YAML files.
2. Preserve formatting conventions already present in the recipe.
3. Avoid broad renames unless strictly required.

## Step 6: Create evolution snapshot
Create a snapshot YAML matching `schema/evolution-snapshot.yaml`.

Guidelines:
- `target`: `recipe`.
- `snapshot_data`: store the full updated content for each modified recipe.
  - If multiple recipes changed, concatenate them with clear separators and filenames.
- `changes`: list per-field changes with evidence references.

## Step 7: Store snapshot to Postgres
Use `store_memory()` with tags and metadata:
```sql
SELECT insert_evolution_snapshot(
  '{user}', '{version_tag}', '{target}', '{trigger}',
  '{changes JSONB}', '{snapshot_data}', '{full snapshot YAML}',
  NULL, 'evolution-agent'
);
```
Writes to both `evolution_snapshots` (typed) and `agent_memories` (general log).

## Step 8: Report
Report:
- Recipes changed (file paths)
- What changed (brief)
- Evidence (3+ references per recipe)
- Version tag stored
