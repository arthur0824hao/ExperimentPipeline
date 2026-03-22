# Procedure: Create TKT Bundle

PM Agent uses this procedure to interpret user intent and create a structured bundle.

## Steps

### 1. Interpret User Intent

Read the user's request and extract:
- **Goal**: What they want to achieve (one clear sentence)
- **Scope**: What files/systems/areas are involved
- **Constraints**: Any deadlines, quality requirements, or limitations

### 2. Check Roadmap

Read `.tkt/roadmap.yaml`:
- Does this relate to an existing goal? → Attach bundle to that goal
- Is this a new direction? → Create a new goal entry first

### 3. Decompose into Tickets

Break the goal into worker tickets. Each ticket should be:
- **Atomic**: One clear task, completable by one agent
- **Verifiable**: Has acceptance criteria that can be checked
- **Independent**: Minimal dependencies on other tickets (list any in `depends_on`)

Rule of thumb: 2-6 worker tickets per bundle. If more than 6, consider splitting into multiple bundles.

### 4. Create Bundle

```bash
bash "<this-skill-dir>/scripts/tkt.sh" create-bundle --goal "<goal>"
```

This creates the bundle directory with TKT-000 (integrate) and TKT-A00 (audit).

### 5. Add Worker Tickets

For each decomposed task:

```bash
bash "<this-skill-dir>/scripts/tkt.sh" add --bundle <B-NNN> --type worker --title "<title>" --description "<description>"
```

### 6. Update Roadmap

Add the new bundle reference to the relevant goal in `roadmap.yaml`.

### 7. Present to User

Show the bundle structure:
- Bundle ID and goal
- List of tickets with titles
- Mermaid diagram of dependencies (if any)
- Ask for confirmation before agents begin work

## Audit Ticket Guidelines

The TKT-A00 audit ticket should specify:
- Random selection of 2-3 items from completed work
- Check against: code standards, test coverage, documentation accuracy, security
- Output a quality score (1-5) with specific findings
