# AGENTS.md

## Project
- Name: ExperimentPipeline
- Purpose: General-purpose experiment pipeline project (runner, preprocess, registry, watcher, tooling)

## Skill Startup Order
1. `skill-system-router`
2. `skill-system-memory`

## Shell Policy
- All shell commands must run through tmux sessions.
- Protected tmux sessions (never kill): `unified`, `unified-oc`, `mem-handoff`, `exp_runner`

## Skills
- Skills are available under `.agents/skills/`
- Main entry index: `.agents/skills/skills-index.json`
