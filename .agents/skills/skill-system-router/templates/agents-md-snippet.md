## Skill System

This project uses a skill system for agent orchestration, behavioral profiles, and persistent memory.

### Skills Directory
`{SKILLS_DIR}`

### How to Use
1. **Router**: Load `skill-system-router` to orchestrate skills. It reads skill manifests, executes operations, and uses `skill-system-workflow` planning for complex multi-step goals.
2. **Soul**: Load `skill-system-soul` to adopt a behavioral profile. Available: `balanced` (default), `creative`, `strict`, `talkative`.
3. **Memory**: Use `agent_memories` (PostgreSQL) for persistent cross-session memory. See `skill-system-memory`.
4. **Insight**: Unified observe-and-evolve engine. Extracts facets, maintains soul state, evolves profiles and recipes with versioning and rollback. See `skill-system-insight`.
5. **Workflow**: Load `skill-system-workflow` to plan multi-step work as DAGs with Mermaid visualization. Includes reusable recipes for common patterns (debug, feature, session start/end).
6. **TKT**: Ticket-based multi-agent work management: Roadmap → Bundle → Tickets → Review. See `skill-system-tkt`.
7. **Feedback**: If you spot repeated friction or quality gaps, use router operation `report-improvement-issue` to file a GitHub issue.

### Quick Reference
- Discover skills: read `{SKILLS_DIR}/skills-index.json` or run `build-index.sh`
- Load a soul: read `{SKILLS_DIR}/skill-system-soul/profiles/<name>.md`
- Store memory: `SELECT store_memory(type, category, tags, title, content, metadata, agent_id, session_id, importance);`
- Search memory: `SELECT * FROM search_memories('query');`
- Extract insight: follow `{SKILLS_DIR}/skill-system-insight/scripts/extract-facets.md`
- Plan workflow: follow `{SKILLS_DIR}/skill-system-workflow/scripts/plan-and-visualize.md`
- Evolve soul: follow `{SKILLS_DIR}/skill-system-insight/scripts/evolve-soul.md`
- List versions: follow `{SKILLS_DIR}/skill-system-insight/scripts/list-versions.md`
- Create TKT bundle: `bash {SKILLS_DIR}/skill-system-tkt/scripts/tkt.sh create-bundle --goal "<goal>"`

### User Soul State
If a personalized profile exists at `{SKILLS_DIR}/skill-system-soul/profiles/<user>.md`, prefer it over `balanced.md`.
Check `agent_memories` for `category='soul-state'` to see the user's dual matrix.

### Insight Suggestion
After completing a non-trivial session, consider asking:
> "Want me to run an insight pass on this session? It helps me learn your preferences for better collaboration."
