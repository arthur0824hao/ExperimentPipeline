---
name: skill-system-workflow
description: "Pure planning engine. Converts goals into DAGs with Mermaid visualization. Ticket lifecycle has moved to skill-system-tkt."
license: MIT
metadata:
  os: windows, linux, macos
---

# Skill System Workflow

`skill-system-workflow` is the planning layer for the skill system. It turns a goal
into a DAG with Mermaid output, prefers reusable recipes when possible, and stays
strictly read-only with respect to execution.

Ticket lifecycle operations are owned by `skill-system-tkt`. This skill now owns
only planning, visualization, and recipe discovery.

## Overview

- Input: a goal plus optional context
- Output: a DAG document and Mermaid `flowchart TD`
- Planning strategy: recipe match first, dynamic planning second
- Execution: out of scope; downstream skills consume the DAG

## Core Operations

### `plan`

Analyze a goal and produce a workflow DAG plus Mermaid visualization.

1. Read the available `recipes/`
2. Match `goal` against `trigger_patterns`
3. If a recipe matches, adapt it to the goal
4. Otherwise, use `prompts/plan-workflow.md` to generate a custom DAG
5. Render Mermaid from the DAG using the conventions below

Procedure: `scripts/plan-and-visualize.md`

### `visualize`

Convert an existing DAG YAML into a Mermaid flowchart.

- Parse `waves[*].tasks[*]`
- Use one Mermaid `subgraph` per wave
- Add `depends_on` edges
- Apply status styling (`pending`, `running`, `done`, `failed`)

### `list-recipes`

List available workflow recipes by reading `recipes/` and returning each recipe's
name and description.

## File Layout

- `prompts/plan-workflow.md`: one-pass dynamic DAG planning prompt
- `schema/workflow-dag.yaml`: workflow DAG shape specification
- `schema/recipe.yaml`: recipe shape specification
- `recipes/*.yaml`: reusable workflow templates
- `scripts/plan-and-visualize.md`: human procedure for plan -> DAG -> Mermaid

## Recipe Format Reference

Recipes are small YAML documents that describe reusable waves and tasks.

- `name`: recipe identifier (must match the filename without extension)
- `trigger_patterns`: goal keywords/phrases that indicate the recipe is applicable
- `waves`: ordered execution waves
- `waves[*].parallel`: whether tasks in the wave can be performed simultaneously
- `waves[*].tasks[*].depends_on`: task ids from earlier waves that must complete first

See: `schema/recipe.yaml`

## Mermaid Conventions

### Diagram structure

- Graph direction: `flowchart TD`
- One subgraph per wave: `subgraph waveN [Wave N: <description>]`
- Each task is a node with id `task_id`
- Node label format: `<agent_type>\n<task name>`

### Node shapes

- Task nodes: rounded rectangles: `task_id(["<agent_type>\\n<name>"])`
- Optional start/end anchors (if used): `start((Start))`, `end((End))`

### Status styling

Use Mermaid classes based on each task's `status`:

```text
pending: not started
running: in progress
done: completed successfully
failed: needs intervention
```

## Configuration

Runtime settings are in `config/workflow.yaml`. Config is the single source of truth.

See: `../../config/workflow.yaml`

## Migration Note

Ticket lifecycle operations are owned by `skill-system-tkt`. `skill-system-workflow`
remains a pure planning engine with 3 operations: `plan`, `visualize`, and
`list-recipes`.

## Operational Notes

- Keep waves small (2-6 tasks) so the diagram remains readable.
- Prefer parallelism inside a wave; use `depends_on` for cross-wave ordering.
- Every task should have a clear verification outcome.

```skill-manifest
{
  "schema_version": "2.0",
  "id": "skill-system-workflow",
  "version": "1.2.0",
  "capabilities": ["workflow-plan", "workflow-visualize", "workflow-list-recipes"],
  "effects": ["fs.read", "db.read"],
  "operations": {
    "plan": {
      "description": "Analyze a goal and produce an execution plan as a DAG with Mermaid visualization.",
      "input": {
        "goal": {"type": "string", "required": true, "description": "User's goal or task description"},
        "context": {"type": "string", "required": false, "description": "Additional context (files, constraints)"}
      },
      "output": {
        "description": "Workflow DAG YAML plus Mermaid diagram",
        "fields": {"dag": "YAML", "mermaid": "string"}
      },
      "entrypoints": {
        "agent": "Follow scripts/plan-and-visualize.md procedure"
      }
    },
    "visualize": {
      "description": "Convert an existing DAG YAML to a Mermaid flowchart.",
      "input": {
        "dag_yaml": {"type": "string", "required": true, "description": "DAG YAML content"}
      },
      "output": {
        "description": "Mermaid flowchart string",
        "fields": {"mermaid": "string"}
      },
      "entrypoints": {
        "agent": "Apply Mermaid conventions from SKILL.md to the DAG"
      }
    },
    "list-recipes": {
      "description": "List available workflow recipes.",
      "input": {},
      "output": {
        "description": "Array of recipe names and descriptions",
        "fields": {"recipes": "array"}
      },
      "entrypoints": {
        "agent": "List files in recipes/ directory"
      }
    }
  },
  "stdout_contract": {
    "last_line_json": false,
    "note": "Agent-executed procedures; output is DAG YAML and Mermaid text."
  }
}
```
