#!/usr/bin/env bash
# UserPromptSubmit hook — injects skill system reminder before every prompt.
# Install: add to your agent config's UserPromptSubmit hooks array.
#
# Claude Code:  .claude/settings.json → hooks.UserPromptSubmit
# OpenCode:     equivalent hooks config
#
# Stdout is injected as additionalContext that the agent sees.

SKILLS_DIR="${SKILLS_DIR:-./skills}"
INDEX="$SKILLS_DIR/skills-index.json"

if [ -f "$INDEX" ]; then
  echo "SKILL SYSTEM ACTIVE. Load skill-system-router to discover capabilities. Index: $INDEX"
else
  echo "SKILL SYSTEM ACTIVE. Load skill-system-router — run rebuild-index if skills-index.json is missing."
fi
