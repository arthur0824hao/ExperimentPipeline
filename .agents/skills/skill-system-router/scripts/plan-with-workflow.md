# Plan With Workflow

Use this procedure to integrate `skill-system-workflow` into router execution for complex goals.

## Inputs

- `goal` (required)
- `context` (optional)

## Procedure

1. Ensure `skills-index.json` is available and includes capability `workflow-plan`.
2. Resolve workflow skill from capability index (`workflow-plan` -> `skill-system-workflow`).
3. Read workflow manifest and select operation `plan`.
4. Execute workflow `plan` with:
   - `goal`
   - `context` (if provided)
5. Parse workflow result and capture:
   - DAG content
   - Mermaid diagram
   - wave/task dependency structure
6. Prepare router execution plan:
   - execute tasks by wave order
   - run parallel tasks inside the same wave when dependencies permit
   - map each task to the best-matching capability/skill operation
7. Return the plan package for Step 5 chain execution.

## Output

- `dag`: YAML
- `mermaid`: string
- `wave_count`: integer
- `execution_notes`: concise summary of routing strategy

## Notes

- Use this path for multi-step or high-uncertainty goals.
- For simple one-operation goals, route directly without workflow planning.
