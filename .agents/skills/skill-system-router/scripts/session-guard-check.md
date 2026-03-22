# Session Guard Check (Router Lightweight Guard)

Use this procedure at the start of a session to ensure the Router discovery step has happened before any other skill is loaded.

## Guard Concept

- Maintain a per-session flag named `router_checked` (or equivalent).
- Meaning of the flag: "Router discovery was performed in this session (skills were discovered / index was consulted)."

## Procedure

1. Before loading or executing any skill other than `skill-system-router`, check whether the session has a truthy `router_checked` flag.
2. If `router_checked` is not set:
   - Emit this warning (verbatim):
     - `Router check not completed. Run router discover-skills first.`
   - Perform router discovery:
     - Read `skills-index.json` and confirm it exists and is reasonably current for this repo.
     - If `skills-index.json` is missing or known-stale, run the router index rebuild operation.
3. Mark the flag as set for the remainder of the session.
4. Proceed with normal routing (capability match -> manifest read -> policy check -> execute).

## How To Mark The Flag

Pick one mechanism that is scoped to the current session:

- Working memory (preferred when available): store a small working-memory record like `router_checked=true` with the current `session_id`.
- Session variable: if your runner supports session-scoped variables, set `session.router_checked = true`.
- In-memory note: if neither is available, keep an internal note in the session context and treat it as authoritative for the session lifetime.

## Notes

- This guard is intentionally lightweight: it does not attempt to validate every skill, only that router discovery happened.
- If the user explicitly requests skipping discovery, still set expectations and proceed cautiously.
